// The serial guard only orders whole tests, each on its own runtime; no task waits on it.
#![allow(clippy::await_holding_lock)]

mod common;
#[path = "common/pgwire.rs"]
mod pgwire;

use common::*;
use mitosis::client;
use mitosis::ctx::Config;
use mitosis::daemon::run;
use mitosis::envfile::{create_clone, env_url, get_var};
use mitosis::error::Result;
use mitosis::paths::Home;
use mitosis::pg::newest;
use mitosis::protocol::{CloneCreateArgs, CloneInfo, EventFilter, Request, Response, RouterStatus};
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;

const SEED: &str = "create table t(id int); insert into t values (1)";

/// Each test runs a daemon with a live router; one at a time.
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

struct Fixture {
    env: TestEnv,
    home: Home,
    port: u16,
    daemon: JoinHandle<Result<()>>,
    _serial: MutexGuard<'static, ()>,
}

impl Fixture {
    /// A built template and a daemon with the router on this env's router port. With `listening`,
    /// waits until the router accepts connections.
    async fn new(listening: bool) -> Option<Fixture> {
        let serial = serial();
        let env = TestEnv::new()?;
        Some(Fixture::start(serial, env, listening).await)
    }

    async fn start(serial: MutexGuard<'static, ()>, env: TestEnv, listening: bool) -> Fixture {
        template(&env, "main", SEED).await;
        Fixture::launch(serial, env, true, listening).await
    }

    /// A daemon on `env`'s home (its template already built), with the router on or off.
    async fn launch(
        serial: MutexGuard<'static, ()>,
        env: TestEnv,
        router: bool,
        listening: bool,
    ) -> Fixture {
        let config = Config {
            router,
            ..env.ctx.config()
        };
        Fixture::launch_with(serial, env, config, listening).await
    }

    /// A daemon on `env`'s home with `config`.
    async fn launch_with(
        serial: MutexGuard<'static, ()>,
        env: TestEnv,
        config: Config,
        listening: bool,
    ) -> Fixture {
        let root = env.tmp.path().to_path_buf();
        let port = config.router_port;
        let daemon = tokio::spawn(run(Home::at(root.clone()), config));
        let deadline = Instant::now() + Duration::from_secs(30);
        while UnixStream::connect(root.join("daemon.sock")).await.is_err() {
            assert!(!daemon.is_finished(), "daemon exited before binding");
            assert!(Instant::now() < deadline, "daemon socket never appeared");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let fx = Fixture {
            env,
            home: Home::at(root),
            port,
            daemon,
            _serial: serial,
        };
        if listening {
            fx.wait_router(Duration::from_secs(10), |r| r.listening)
                .await;
        }
        fx
    }

    async fn router(&self) -> RouterStatus {
        match client::request(&self.home, Request::Status).await.unwrap() {
            Response::Status(s) => s.router.expect("router status with the router on"),
            other => panic!("not a status: {other:?}"),
        }
    }

    async fn wait_router(
        &self,
        within: Duration,
        ok: impl Fn(&RouterStatus) -> bool,
    ) -> RouterStatus {
        let deadline = Instant::now() + within;
        loop {
            let r = self.router().await;
            if ok(&r) {
                return r;
            }
            assert!(
                Instant::now() < deadline,
                "router never reached the state: {r:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn clone(&self, name: Option<&str>) -> CloneInfo {
        match client::request(&self.home, Request::CloneCreate(args(name)))
            .await
            .unwrap()
        {
            Response::CloneCreated(info) => info,
            other => panic!("not created: {other:?}"),
        }
    }

    async fn status(&self, name: &str) -> CloneInfo {
        match client::request(&self.home, Request::CloneList)
            .await
            .unwrap()
        {
            Response::Clones { clones } => clones
                .into_iter()
                .find(|c| c.name.as_deref() == Some(name))
                .expect("clone listed"),
            other => panic!("not clones: {other:?}"),
        }
    }

    async fn stop(self) {
        let _ = client::request(&self.home, Request::Shutdown).await;
        let _ = tokio::time::timeout(Duration::from_secs(30), self.daemon).await;
    }
}

fn args(name: Option<&str>) -> CloneCreateArgs {
    CloneCreateArgs {
        template: "main".into(),
        name: name.map(String::from),
        ttl_secs: 3600,
        durable: false,
        parent_pid: None,
        fresh: false,
        protect: false,
        env: None,
        wait_secs: None,
        branch: None,
    }
}

/// The password in a clone URL.
fn password(url: &str) -> &str {
    let rest = url.strip_prefix("postgres://postgres:").unwrap();
    &rest[..rest.find('@').unwrap()]
}

/// `psql` against `url` without asserting success.
fn psql_try(env: &TestEnv, url: &str, sql: &str) -> Output {
    let installs = env.ctx.installs();
    Command::new(newest(&installs).unwrap().bin("psql"))
        .arg(url)
        .args(["-v", "ON_ERROR_STOP=1", "-X", "-Atc", sql])
        .env("PGCONNECT_TIMEOUT", "5")
        .output()
        .expect("run psql")
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn router_connect_by_name_and_id() {
    let Some(fx) = Fixture::new(true).await else {
        return;
    };
    let named = fx.clone(Some("feat")).await;
    let unnamed = fx.clone(None).await;
    let pw = password(&named.url);
    assert_eq!(
        named.url,
        format!("postgres://postgres:{pw}@127.0.0.1:{}/feat", fx.port)
    );
    assert!(
        unnamed
            .url
            .ends_with(&format!(":{}/{}", fx.port, unnamed.id)),
        "{}",
        unnamed.url
    );
    assert!(named
        .direct_url
        .contains(&format!("@127.0.0.1:{}/", named.port)));
    assert_eq!(named.status, "ready");
    for c in [&named, &unnamed] {
        assert_eq!(psql_url(&fx.env, &c.url, "select count(*) from t"), "1");
        assert_eq!(psql_url(&fx.env, &c.direct_url, "select 1"), "1");
    }
    // The router names the clone, Postgres sees the template's database.
    assert_eq!(
        psql_url(&fx.env, &named.url, "select current_database()"),
        "app"
    );
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn router_unknown_database_is_3d000() {
    let Some(fx) = Fixture::new(true).await else {
        return;
    };
    let c = fx.clone(Some("feat")).await;
    let pw = password(&c.url);
    let bad = format!("postgres://postgres:{pw}@127.0.0.1:{}/postgres", fx.port);
    let out = psql_try(&fx.env, &bad, "select 1");
    assert!(!out.status.success());
    let err = stderr(&out);
    assert!(err.contains("no clone named \"postgres\""), "{err}");
    assert!(err.contains("<clone name or id>"), "{err}");
    let raw = pgwire::connect(fx.port, "postgres", pw, 0)
        .await
        .err()
        .unwrap();
    assert!(raw.contains("3D000") && raw.contains("postgres"), "{raw}");
    // The router is still fine afterwards.
    assert_eq!(psql_url(&fx.env, &c.url, "select 1"), "1");
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn router_wrong_password_rejected() {
    let Some(fx) = Fixture::new(true).await else {
        return;
    };
    let c = fx.clone(Some("feat")).await;
    let wrong = c.url.replace(password(&c.url), "wrongpassword");
    let out = psql_try(&fx.env, &wrong, "select 1");
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("password authentication failed"),
        "{}",
        stderr(&out)
    );
    assert_eq!(psql_url(&fx.env, &c.url, "select 1"), "1");
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn router_ssl_request_gets_n() {
    let Some(fx) = Fixture::new(true).await else {
        return;
    };
    let c = fx.clone(Some("feat")).await;
    assert_eq!(
        psql_url(&fx.env, &format!("{}?sslmode=prefer", c.url), "select 1"),
        "1"
    );
    let out = psql_try(&fx.env, &format!("{}?sslmode=require", c.url), "select 1");
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("does not support SSL"),
        "{}",
        stderr(&out)
    );
    // A direct TLS hello (sslnegotiation=direct, libpq 17+) is closed, not answered.
    let out = psql_try(
        &fx.env,
        &format!("{}?sslmode=require&sslnegotiation=direct", c.url),
        "select 1",
    );
    assert!(!out.status.success());
    assert_eq!(psql_url(&fx.env, &c.url, "select 1"), "1");
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn router_cancel_routes() {
    let Some(fx) = Fixture::new(true).await else {
        return;
    };
    let c = fx.clone(Some("feat")).await;
    let pw = password(&c.url).to_string();
    let major = newest(&fx.env.ctx.installs()).unwrap().major;
    for minor in [0u16, 2] {
        let mut conn = pgwire::connect(fx.port, "feat", &pw, minor).await.unwrap();
        if minor == 0 || major < 18 {
            assert_eq!(conn.key.len(), 4, "3.{minor}");
        } else {
            assert!(
                conn.key.len() > 4,
                "3.2 on Postgres {major}: {:?}",
                conn.key
            );
        }
        let (pid, key) = (conn.pid, conn.key.clone());
        let started = Instant::now();
        let running =
            tokio::spawn(async move { pgwire::query(&mut conn, "select pg_sleep(30)").await });
        tokio::time::sleep(Duration::from_millis(500)).await;
        // An unknown key is dropped silently and cancels nothing.
        pgwire::cancel(fx.port, pid, &[0u8; 4]).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!running.is_finished(), "a wrong key cancelled the query");
        pgwire::cancel(fx.port, pid, &key).await;
        let res = tokio::time::timeout(Duration::from_secs(10), running)
            .await
            .expect("query still running after the cancel")
            .unwrap();
        let (code, msg) = res.expect_err("the query was not cancelled");
        assert_eq!(code, "57014", "3.{minor}: {msg}");
        assert!(started.elapsed() < Duration::from_secs(5), "3.{minor}");
    }
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn router_port_taken_keeps_router_url() {
    // The test seam: retry the bind every half second instead of every minute.
    std::env::set_var("MITOSIS_ROUTER_RETRY_MS", "500");
    let serial = serial();
    let Some(env) = TestEnv::new() else {
        return;
    };
    let port = env.ctx.config().router_port;
    let blocker = std::net::TcpListener::bind(("127.0.0.1", port)).expect("occupy the router port");
    let fx = Fixture::start(serial, env, false).await;
    let r = fx
        .wait_router(Duration::from_secs(10), |r| r.error.is_some())
        .await;
    assert!(!r.listening);
    assert!(
        r.error.as_deref().unwrap().contains(&port.to_string()),
        "{r:?}"
    );

    let c = fx.clone(Some("feat")).await;
    let pw = password(&c.url);
    assert_eq!(
        c.url,
        format!("postgres://postgres:{pw}@127.0.0.1:{port}/feat")
    );
    assert!(
        c.warnings
            .iter()
            .any(|w| w.contains("router is not listening")),
        "{:?}",
        c.warnings
    );
    assert_eq!(psql_url(&fx.env, &c.direct_url, "select 1"), "1");

    drop(blocker);
    let r = fx
        .wait_router(Duration::from_secs(70), |r| r.listening)
        .await;
    assert_eq!(r.error, None);
    assert_eq!(psql_url(&fx.env, &c.url, "select 1"), "1");

    let filter = EventFilter {
        kind: Some("router".into()),
        ..Default::default()
    };
    let Response::Events { events } = client::request(
        &fx.home,
        Request::Events {
            filter,
            follow: false,
        },
    )
    .await
    .unwrap() else {
        panic!("not events")
    };
    let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
    assert_eq!(kinds, ["router.failed", "router.started"]);
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn env_file_gets_router_url() {
    let Some(fx) = Fixture::new(true).await else {
        return;
    };
    let project = tempfile::tempdir().unwrap();
    let file = std::fs::canonicalize(project.path()).unwrap().join(".env");
    std::fs::write(&file, "DATABASE_URL=old\n").unwrap();
    let info = create_clone(&fx.home, args(Some("feat")), Some(file.clone()), None)
        .await
        .unwrap();
    let pw = password(&info.url);
    assert_eq!(
        info.url,
        format!("postgres://postgres:{pw}@127.0.0.1:{}/feat", fx.port)
    );
    let text = std::fs::read_to_string(&file).unwrap();
    let written = get_var(&text, "DATABASE_URL").unwrap();
    assert_eq!(written, info.url);
    assert_eq!(psql_url(&fx.env, &written, "select count(*) from t"), "1");
    fx.stop().await;
}

/// A named clone put to sleep by an in-process ctx, before any daemon runs on the home.
async fn sleeping_clone(env: &TestEnv, name: &str) -> CloneInfo {
    let c = mitosis::clones::create(&env.ctx, args(Some(name)))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match mitosis::clones::sleep(&env.ctx, name, "idle").await {
            Ok(()) => return c,
            Err(e) if Instant::now() < deadline && e.message.contains("open connections") => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Err(e) => panic!("sleep {name}: {e}"),
        }
    }
}

/// A sleeping clone's direct URL refuses connections, so `mitosis env --direct` wakes it first.
#[tokio::test(flavor = "multi_thread")]
async fn env_direct_wakes_sleeping_clone() {
    let serial = serial();
    let Some(env) = TestEnv::new() else {
        return;
    };
    template(&env, "main", SEED).await;
    sleeping_clone(&env, "a").await;
    let fx = Fixture::launch(serial, env, true, true).await;
    // The router listens, so the startup gc leaves it asleep.
    assert_eq!(fx.status("a").await.status, "sleeping");
    let (url, _) = env_url(&fx.home, "a", true).await.unwrap();
    let info = fx.status("a").await;
    assert_eq!(info.status, "ready");
    assert_eq!(url, info.direct_url);
    assert_eq!(psql_url(&fx.env, &url, "select count(*) from t"), "1");
    fx.stop().await;
}

/// With the router off nothing wakes a clone on connect: the daemon starts sleeping clones again.
#[tokio::test(flavor = "multi_thread")]
async fn router_off_at_daemon_start_wakes_sleeping_clones() {
    let serial = serial();
    let Some(env) = TestEnv::new() else {
        return;
    };
    template(&env, "main", SEED).await;
    sleeping_clone(&env, "a").await;
    let fx = Fixture::launch(serial, env, false, false).await;
    // Woken by a task once the daemon accepts.
    let deadline = Instant::now() + Duration::from_secs(30);
    let info = loop {
        let info = fx.status("a").await;
        if info.status == "ready" {
            break info;
        }
        assert!(Instant::now() < deadline, "never woke: {}", info.status);
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(info.url, info.direct_url);
    assert_eq!(psql_url(&fx.env, &info.url, "select count(*) from t"), "1");
    fx.stop().await;
}

/// The startup wakes stay within the budget: with room for one running clone, one of two
/// sleeping clones is started and the other stays asleep with a warning in the daemon log.
#[tokio::test(flavor = "multi_thread")]
async fn startup_wakes_stop_at_the_budget() {
    let serial = serial();
    let Some(env) = TestEnv::new() else {
        return;
    };
    template(&env, "main", SEED).await;
    sleeping_clone(&env, "a").await;
    sleeping_clone(&env, "b").await;
    let config = Config {
        router: false,
        max_clones: 1,
        ..env.ctx.config()
    };
    let fx = Fixture::launch_with(serial, env, config, false).await;
    let log = fx.home.logs_dir().join("daemon.log");
    let deadline = Instant::now() + Duration::from_secs(60);
    while !std::fs::read_to_string(&log)
        .unwrap_or_default()
        .contains("sleeping clones left asleep")
    {
        assert!(Instant::now() < deadline, "the wake task never finished");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut statuses = [fx.status("a").await.status, fx.status("b").await.status];
    statuses.sort();
    assert_eq!(statuses, ["ready", "sleeping"]);
    let text = std::fs::read_to_string(&log).unwrap();
    assert!(text.contains("mitosis wake <clone>"), "{text}");
    fx.stop().await;
}
