mod common;

use common::*;
use mitosis::ctx::Config;
use mitosis::daemon::run;
use mitosis::error::{ErrorCode, Result};
use mitosis::paths::Home;
use mitosis::protocol::{CloneCreateArgs, Request, RequestEnvelope, Response, ResponseEnvelope, PROTOCOL_VERSION};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;

const SEED: &str = "create table t(id int); insert into t values (1)";

/// A short home under /tmp with no Postgres needed.
fn short_home() -> tempfile::TempDir {
    tempfile::Builder::new().prefix("md").tempdir_in("/tmp").unwrap()
}

/// Starts the daemon on `root` and waits until its socket accepts connections.
async fn start(root: &Path, config: Config) -> JoinHandle<Result<()>> {
    let handle = tokio::spawn(run(Home::at(root.to_path_buf()), config));
    let sock = root.join("daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(30);
    while UnixStream::connect(&sock).await.is_err() {
        assert!(!handle.is_finished(), "daemon exited before binding its socket");
        assert!(Instant::now() < deadline, "daemon socket never appeared");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    handle
}

struct Conn {
    rd: BufReader<OwnedReadHalf>,
    wr: OwnedWriteHalf,
}

impl Conn {
    async fn open(root: &Path) -> Conn {
        let (rd, wr) = UnixStream::connect(root.join("daemon.sock")).await.unwrap().into_split();
        Conn { rd: BufReader::new(rd), wr }
    }

    async fn line(&mut self, line: &str) -> ResponseEnvelope {
        self.wr.write_all(format!("{line}\n").as_bytes()).await.unwrap();
        let mut reply = String::new();
        self.rd.read_line(&mut reply).await.unwrap();
        assert!(reply.ends_with('\n'), "reply is not one line: {reply:?}");
        serde_json::from_str(&reply).unwrap()
    }

    async fn send(&mut self, request: Request) -> ResponseEnvelope {
        self.line(&serde_json::to_string(&RequestEnvelope { v: PROTOCOL_VERSION, request }).unwrap()).await
    }

    async fn ok(&mut self, request: Request) -> Response {
        self.send(request).await.result.expect("request failed")
    }
}

/// Sends Shutdown and waits for `run` to return.
async fn shutdown(root: &Path, handle: JoinHandle<Result<()>>) {
    assert!(matches!(Conn::open(root).await.ok(Request::Shutdown).await, Response::Ok));
    tokio::time::timeout(Duration::from_secs(30), handle).await.unwrap().unwrap().unwrap();
    assert!(!root.join("daemon.sock").exists());
}

async fn status_clones(root: &Path) -> usize {
    match Conn::open(root).await.ok(Request::Status).await {
        Response::Status(s) => s.clones,
        other => panic!("wrong response: {other:?}"),
    }
}

#[tokio::test]
async fn status_over_socket() {
    let tmp = short_home();
    let handle = start(tmp.path(), Config::default()).await;
    match Conn::open(tmp.path()).await.ok(Request::Status).await {
        Response::Status(s) => {
            assert_eq!(s.clones, 0);
            assert_eq!(s.templates, 0);
            assert_eq!(s.pid, std::process::id());
            assert_eq!(s.home, tmp.path());
        }
        other => panic!("wrong response: {other:?}"),
    }
    shutdown(tmp.path(), handle).await;
}

#[tokio::test]
async fn protocol_mismatch() {
    let tmp = short_home();
    let handle = start(tmp.path(), Config::default()).await;
    let reply = Conn::open(tmp.path()).await.line(r#"{"v":99,"request":{"type":"status"}}"#).await;
    let e = reply.result.unwrap_err();
    assert_eq!(e.code, ErrorCode::ProtocolMismatch);
    assert!(e.message.contains("99") && e.message.contains('1'), "{}", e.message);
    shutdown(tmp.path(), handle).await;
}

#[tokio::test]
async fn garbage_line() {
    let tmp = short_home();
    let handle = start(tmp.path(), Config::default()).await;
    let mut conn = Conn::open(tmp.path()).await;
    assert_eq!(conn.line("not json").await.result.unwrap_err().code, ErrorCode::InvalidRequest);
    // The same connection still serves the next line.
    assert!(matches!(conn.ok(Request::Status).await, Response::Status(_)));
    shutdown(tmp.path(), handle).await;
}

#[tokio::test]
async fn second_daemon_refused() {
    let tmp = short_home();
    let handle = start(tmp.path(), Config::default()).await;
    let second = run(Home::at(tmp.path().to_path_buf()), Config::default()).await;
    assert_eq!(second.unwrap_err().code, ErrorCode::AlreadyRunning);
    // The refused daemon did not touch the first one's socket.
    assert_eq!(status_clones(tmp.path()).await, 0);
    shutdown(tmp.path(), handle).await;
}

fn clone_args() -> CloneCreateArgs {
    CloneCreateArgs { template: "main".into(), name: None, ttl_secs: 7200, durable: false, parent_pid: None }
}

async fn clones(conn: &mut Conn) -> usize {
    match conn.ok(Request::CloneList).await {
        Response::Clones { clones } => clones.len(),
        other => panic!("wrong response: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn clone_roundtrip_over_socket() {
    let Some(env) = TestEnv::new() else { return };
    let root = env.tmp.path();
    let handle = start(root, env.ctx.config.clone()).await;
    let mut conn = Conn::open(root).await;

    assert!(matches!(
        conn.ok(Request::TemplateInit(template_args("main", seed(SEED)))).await,
        Response::TemplateBuilt(_)
    ));
    let info = match conn.ok(Request::CloneCreate(clone_args())).await {
        Response::CloneCreated(info) => info,
        other => panic!("wrong response: {other:?}"),
    };
    assert_eq!(psql_url(&env, &info.url, "select count(*) from t"), "1");
    assert_eq!(clones(&mut conn).await, 1);
    match conn.ok(Request::CloneDestroy { id_or_name: info.id.clone() }).await {
        Response::Destroyed { id } => assert_eq!(id, info.id),
        other => panic!("wrong response: {other:?}"),
    }
    assert_eq!(clones(&mut conn).await, 0);
    shutdown(root, handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_keeps_clones() {
    let Some(env) = TestEnv::new() else { return };
    let root = env.tmp.path();
    let handle = start(root, env.ctx.config.clone()).await;
    let mut conn = Conn::open(root).await;
    conn.ok(Request::TemplateInit(template_args("main", seed(SEED)))).await;
    let info = match conn.ok(Request::CloneCreate(clone_args())).await {
        Response::CloneCreated(info) => info,
        other => panic!("wrong response: {other:?}"),
    };
    drop(conn);

    shutdown(root, handle).await;
    assert_eq!(psql_url(&env, &info.url, "select count(*) from t"), "1");

    let handle = start(root, env.ctx.config.clone()).await;
    let mut conn = Conn::open(root).await;
    match conn.ok(Request::Gc).await {
        Response::Gc(report) => {
            assert!(report.adopted.contains(&info.id), "{report:?}");
            assert!(report.destroyed.is_empty(), "{report:?}");
        }
        other => panic!("wrong response: {other:?}"),
    }
    assert_eq!(clones(&mut conn).await, 1);
    assert_eq!(psql_url(&env, &info.url, "select count(*) from t"), "1");
    shutdown(root, handle).await;
}
