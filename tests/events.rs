// The daemon here runs in this process; the follow test holds nothing across an await that the
// daemon needs.
mod common;

use common::*;
use mitosis::clones::{create, destroy};
use mitosis::daemon;
use mitosis::lifecycle::{reap_expired, reconcile};
use mitosis::paths::Home;
use mitosis::protocol::{
    CloneCreateArgs, EnvTarget, EventFilter, EventRow, Request, RequestEnvelope, Response,
    ResponseEnvelope, TemplateSource, PROTOCOL_VERSION,
};
use mitosis::templates;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;

const SEED: &str = "create table t(id int); insert into t values (1)";

fn args(template: &str) -> CloneCreateArgs {
    CloneCreateArgs {
        template: template.into(),
        name: None,
        ttl_secs: 7200,
        durable: false,
        parent_pid: None,
        fresh: false,
        protect: false,
        env: None,
        wait_secs: None,
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn all(env: &TestEnv) -> Vec<EventRow> {
    env.ctx
        .store()
        .list_events(&EventFilter::default())
        .unwrap()
}

fn of_kind<'a>(events: &'a [EventRow], kind: &str) -> Vec<&'a EventRow> {
    events.iter().filter(|e| e.kind == kind).collect()
}

/// The `reason` of every `clone.destroyed` event for clone `id`.
fn reasons(events: &[EventRow], id: &str) -> Vec<String> {
    of_kind(events, "clone.destroyed")
        .into_iter()
        .filter(|e| e.detail["id"] == id)
        .map(|e| e.detail["reason"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn each_action_writes_its_event() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;

    // A first build is generation 1, current.
    let evs = all(&env);
    let built = of_kind(&evs, "template.built");
    assert_eq!(built.len(), 1);
    assert_eq!(built[0].template.as_deref(), Some("main"));
    assert_eq!(built[0].detail["gen"], 1);
    assert_eq!(built[0].detail["kind"], "full");
    assert_eq!(built[0].detail["current"], true);

    // create, then a destroy the user asked for
    let a = create(&env.ctx, args("main")).await.unwrap();
    let evs = all(&env);
    let created = of_kind(&evs, "clone.created");
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].clone.as_deref(), Some(a.id.as_str()));
    assert_eq!(created[0].template.as_deref(), Some("main"));
    assert_eq!(created[0].detail["id"], a.id.as_str());
    destroy(&env.ctx, &a.id).await.unwrap();
    assert_eq!(reasons(&all(&env), &a.id), vec!["user"]);

    // a TTL reap
    let b = create(&env.ctx, args("main")).await.unwrap();
    let reaped = reap_expired(&env.ctx, now() + 7300).await.unwrap();
    assert_eq!(reaped, vec![b.id.clone()]);
    assert_eq!(reasons(&all(&env), &b.id), vec!["ttl"]);

    // --fresh destroys the old clone of the name, shown under the name
    let named = || CloneCreateArgs {
        name: Some("keep".into()),
        ..args("main")
    };
    let c = create(&env.ctx, named()).await.unwrap();
    let d = create(
        &env.ctx,
        CloneCreateArgs {
            fresh: true,
            ..named()
        },
    )
    .await
    .unwrap();
    assert_ne!(c.id, d.id);
    let evs = all(&env);
    assert_eq!(reasons(&evs, &c.id), vec!["fresh"]);
    let by_name: Vec<_> = evs
        .iter()
        .filter(|e| e.clone.as_deref() == Some("keep"))
        .map(|e| e.kind.as_str())
        .collect();
    assert_eq!(
        by_name,
        vec!["clone.created", "clone.destroyed", "clone.created"]
    );

    // a refresh builds generation 2
    templates::refresh(&env.ctx, "main", None).await.unwrap();
    let evs = all(&env);
    let built = of_kind(&evs, "template.built");
    assert_eq!(built.len(), 2);
    assert_eq!(built[1].detail["gen"], 2);

    // a seed that fails
    let e = templates::init(&env.ctx, template_args("bad", seed("select 1/0")))
        .await
        .unwrap_err();
    assert_eq!(e.code, mitosis::error::ErrorCode::TemplateBuildFailed);
    let evs = all(&env);
    let failed = of_kind(&evs, "template.failed");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].template.as_deref(), Some("bad"));
    assert_eq!(failed[0].detail["step"], "seed");
    assert_eq!(failed[0].detail["op"], "build");

    // a rollback to generation 1
    templates::rollback(&env.ctx, "main", None).await.unwrap();
    let evs = all(&env);
    let rolled = of_kind(&evs, "template.rolled_back");
    assert_eq!(rolled.len(), 1);
    assert_eq!(rolled[0].detail["from"], 2);
    assert_eq!(rolled[0].detail["to"], 1);

    // gc
    reconcile(env.ctx.clone(), now()).await.unwrap();
    let evs = all(&env);
    let gc = of_kind(&evs, "gc");
    assert_eq!(gc.len(), 1);
    assert_eq!(gc[0].detail["failed"], 0);

    // newest last, ids rising, and the clone filter finds a destroyed named clone by its id
    assert!(evs.windows(2).all(|w| w[0].id < w[1].id));
    let by_id = env
        .ctx
        .store()
        .list_events(&EventFilter {
            clone: Some(c.id.clone()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(by_id.len(), 2);

    destroy(&env.ctx, "keep").await.unwrap();
}

#[tokio::test]
async fn events_never_contain_secrets() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "a", SEED).await;
    let src = create(&env.ctx, args("a")).await.unwrap();
    let password = |url: &str| {
        url.strip_prefix("postgres://postgres:")
            .and_then(|r| r.split_once('@'))
            .map(|(pw, _)| pw.to_string())
            .unwrap()
    };
    let src_password = password(&src.url);
    assert_eq!(src_password.len(), 32, "{src_password}");

    // A URL source with a password, built and cloned with a `.env` entry.
    templates::init(
        &env.ctx,
        template_args(
            "b",
            TemplateSource::Url {
                url: src.url.clone(),
            },
        ),
    )
    .await
    .unwrap();
    let dotenv = env.tmp.path().join(".env");
    std::fs::write(&dotenv, "DATABASE_URL=old\n").unwrap();
    let copy = create(
        &env.ctx,
        CloneCreateArgs {
            env: Some(EnvTarget {
                file: dotenv,
                var: "DATABASE_URL".into(),
                previous: Some("old".into()),
            }),
            ..args("b")
        },
    )
    .await
    .unwrap();
    let copy_password = password(&copy.url);

    // A URL source whose build fails on a wrong password.
    let wrong = "wrongpw0123456789";
    let bad = format!("postgres://postgres:{wrong}@127.0.0.1:{}/app", src.port);
    templates::init(
        &env.ctx,
        template_args("c", TemplateSource::Url { url: bad }),
    )
    .await
    .unwrap_err();

    destroy(&env.ctx, &copy.id).await.unwrap();
    destroy(&env.ctx, &src.id).await.unwrap();
    let evs = all(&env);
    assert!(of_kind(&evs, "template.failed").len() == 1, "{evs:?}");
    let json = serde_json::to_string(&evs).unwrap();
    for secret in [&src_password, &copy_password, &wrong.to_string()] {
        assert!(!json.contains(secret.as_str()), "{json}");
    }
    assert!(!json.contains("postgres://"), "{json}");
}

struct Conn {
    rd: BufReader<OwnedReadHalf>,
    wr: OwnedWriteHalf,
}

impl Conn {
    async fn open(root: &Path) -> Conn {
        let (rd, wr) = UnixStream::connect(root.join("daemon.sock"))
            .await
            .unwrap()
            .into_split();
        Conn {
            rd: BufReader::new(rd),
            wr,
        }
    }

    async fn send(&mut self, request: Request) {
        let mut line = serde_json::to_vec(&RequestEnvelope {
            v: PROTOCOL_VERSION,
            request,
        })
        .unwrap();
        line.push(b'\n');
        self.wr.write_all(&line).await.unwrap();
    }

    async fn recv(&mut self) -> Response {
        let mut reply = String::new();
        self.rd.read_line(&mut reply).await.unwrap();
        let env: ResponseEnvelope = serde_json::from_str(&reply).unwrap();
        env.result.expect("request failed")
    }

    async fn call(&mut self, request: Request) -> Response {
        self.send(request).await;
        self.recv().await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn follow_receives_new_event() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let root = env.tmp.path();
    let handle = tokio::spawn(daemon::run(Home::at(root.to_path_buf()), env.ctx.config()));
    let deadline = Instant::now() + Duration::from_secs(30);
    while UnixStream::connect(root.join("daemon.sock")).await.is_err() {
        assert!(Instant::now() < deadline, "daemon socket never appeared");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Following: the past events come first (one `template.built` is in the log), then new ones.
    let mut follower = Conn::open(root).await;
    follower
        .send(Request::Events {
            filter: EventFilter {
                kind: Some("clone".into()),
                ..Default::default()
            },
            follow: true,
        })
        .await;
    let Response::Events { events } = follower.recv().await else {
        panic!("the first reply is the past events")
    };
    assert!(events.is_empty(), "{events:?}");

    let created = match Conn::open(root)
        .await
        .call(Request::CloneCreate(args("main")))
        .await
    {
        Response::CloneCreated(info) => info,
        other => panic!("wrong response: {other:?}"),
    };
    let Response::Event(ev) = tokio::time::timeout(Duration::from_secs(5), follower.recv())
        .await
        .expect("no event within 5 s")
    else {
        panic!("the follower got something else")
    };
    assert_eq!(ev.kind, "clone.created");
    assert_eq!(ev.detail["id"], created.id.as_str());

    // A plain request lists the same event, template build included.
    let Response::Events { events } = Conn::open(root)
        .await
        .call(Request::Events {
            filter: EventFilter::default(),
            follow: false,
        })
        .await
    else {
        panic!("wrong response")
    };
    let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
    assert!(
        kinds.contains(&"template.built") && kinds.contains(&"clone.created"),
        "{kinds:?}"
    );

    let mut conn = Conn::open(root).await;
    let destroyed = conn
        .call(Request::CloneDestroy {
            id_or_name: created.id.clone(),
            force: false,
            forget: false,
        })
        .await;
    assert!(matches!(destroyed, Response::Destroyed { .. }));
    // The follower sees the destroy and its reason.
    let Response::Event(ev) = follower.recv().await else {
        panic!("the follower got something else")
    };
    assert_eq!(
        (ev.kind.as_str(), ev.detail["reason"].as_str()),
        ("clone.destroyed", Some("user"))
    );

    // A shutdown is not held up by the open follower, and it ends the stream.
    assert!(matches!(
        Conn::open(root).await.call(Request::Shutdown).await,
        Response::Ok
    ));
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut rest = String::new();
    assert_eq!(follower.rd.read_line(&mut rest).await.unwrap(), 0);
}
