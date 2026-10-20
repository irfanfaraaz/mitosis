//! Sleeping clones: idle sleep, router wake, eviction under the RAM budget, idle reap.
//! Each test runs its own in-process router on its env's slice port.

mod common;
#[allow(dead_code)] // the cancel helpers are for the router tests
#[path = "common/pgwire.rs"]
mod pgwire;

use common::*;
use mitosis::budget::usage;
use mitosis::clones::{create, destroy, list, sleep, wake};
use mitosis::ctx::Config;
use mitosis::lifecycle::idle_pass;
use mitosis::pg::newest;
use mitosis::protocol::{CloneCreateArgs, CloneInfo, EventFilter};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const SEED: &str = "create table t(id int); insert into t values (1)";

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn args(name: Option<&str>) -> CloneCreateArgs {
    CloneCreateArgs {
        template: "main".into(),
        name: name.map(String::from),
        ttl_secs: 7200,
        durable: false,
        parent_pid: None,
        fresh: false,
        protect: false,
        env: None,
        wait_secs: Some(0),
    }
}

/// A template, the router on, and `idle_sleep` of one second.
async fn setup() -> Option<TestEnv> {
    let env = TestEnv::new()?;
    template(&env, "main", SEED).await;
    start_router(&env).await;
    env.ctx.set_config(Config {
        idle_sleep: Some(Duration::from_secs(1)),
        ..env.ctx.config()
    });
    Some(env)
}

fn status(env: &TestEnv, id: &str) -> String {
    list(&env.ctx)
        .unwrap()
        .into_iter()
        .find(|c| c.id == id)
        .expect("clone listed")
        .status
}

fn password(url: &str) -> String {
    let rest = url.strip_prefix("postgres://postgres:").unwrap();
    rest[..rest.find('@').unwrap()].to_string()
}

fn events(env: &TestEnv, kind: &str, id: &str) -> usize {
    let filter = EventFilter {
        kind: Some(kind.into()),
        clone: Some(id.into()),
        ..Default::default()
    };
    env.ctx.store().list_events(&filter).unwrap().len()
}

/// Runs the idle pass (looking 10 s ahead) until `id` is slept; a just-closed backend can take a
/// moment to leave `pg_stat_activity`.
async fn sleep_idle(env: &TestEnv, id: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (slept, _) = idle_pass(&env.ctx, now() + 10).await.unwrap();
        if slept.iter().any(|s| s == id) {
            return;
        }
        assert!(Instant::now() < deadline, "clone {id} never slept");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// `clones::sleep`, retried while a just-closed router connection is still counted.
async fn put_to_sleep(env: &TestEnv, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match sleep(&env.ctx, name, "idle").await {
            Ok(()) => return,
            Err(e) if Instant::now() < deadline && e.message.contains("open connections") => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Err(e) => panic!("sleep {name}: {e}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_clone_sleeps_and_router_wakes_it() {
    let Some(env) = setup().await else { return };
    let a = create(&env.ctx, args(Some("a"))).await.unwrap();
    assert!(a
        .url
        .ends_with(&format!(":{}/a", env.ctx.config().router_port)));
    psql_url(&env, &a.url, "insert into t values (2)");
    sleep_idle(&env, &a.id).await;
    let listed = list(&env.ctx).unwrap();
    let c: &CloneInfo = listed.iter().find(|c| c.id == a.id).unwrap();
    assert_eq!(c.status, "sleeping");
    assert_eq!(c.rss_bytes, None);
    assert_eq!(c.url, a.url);
    assert_eq!(events(&env, "clone.slept", &a.id), 1);
    // The first router connection wakes it, with the data written before the sleep.
    assert_eq!(psql_url(&env, &a.url, "select count(*) from t"), "2");
    assert_eq!(status(&env, &a.id), "ready");
    assert_eq!(events(&env, "clone.woke", &a.id), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_backend_keeps_clone_awake() {
    let Some(env) = setup().await else { return };
    let a = create(&env.ctx, args(None)).await.unwrap();
    let installs = env.ctx.installs();
    let mut busy = Command::new(newest(&installs).unwrap().bin("psql"))
        .arg(&a.direct_url)
        .args(["-X", "-Atc", "select pg_sleep(30)"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (slept, _) = idle_pass(&env.ctx, now() + 100).await.unwrap();
    let _ = busy.kill();
    let _ = busy.wait();
    assert!(slept.is_empty(), "{slept:?}");
    assert_eq!(status(&env, &a.id), "ready");
}

/// Sets the RAM budget halfway between what the running clones use plus one more estimate and
/// that minus half an estimate: a new clone does not fit until one clone sleeps.
async fn budget_for_two(env: &TestEnv) {
    let u = usage(&env.ctx).await.unwrap();
    let rss: Vec<u64> = u.clones.iter().filter_map(|c| c.rss_bytes).collect();
    assert_eq!(rss.len(), 2, "{u:?}");
    let est = rss.iter().sum::<u64>() / 2;
    env.ctx.set_config(Config {
        ram_budget: Some(u.ram_bytes + est / 2),
        ..env.ctx.config()
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn ram_budget_sleeps_lru_idle_before_queueing() {
    let Some(env) = setup().await else { return };
    let a = create(&env.ctx, args(None)).await.unwrap();
    let b = create(&env.ctx, args(None)).await.unwrap();
    // b was active more recently than a.
    env.ctx
        .router()
        .last_active
        .insert(b.id.clone(), now() + 60);
    budget_for_two(&env).await;
    let started = Instant::now();
    let c = create(&env.ctx, args(None)).await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(20));
    assert_eq!(status(&env, &a.id), "sleeping");
    assert_eq!(status(&env, &b.id), "ready");
    assert_eq!(status(&env, &c.id), "ready");
    // The sleeping clone still counts for the cap.
    assert_eq!(list(&env.ctx).unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn open_connection_is_never_evicted() {
    let Some(env) = setup().await else { return };
    let port = env.ctx.config().router_port;
    let a = create(&env.ctx, args(None)).await.unwrap();
    let b = create(&env.ctx, args(None)).await.unwrap();
    let mut held = pgwire::connect(port, &a.id, &password(&a.url), 0)
        .await
        .unwrap();
    // a is the least recently active, but it has a connection.
    env.ctx
        .router()
        .last_active
        .insert(b.id.clone(), now() + 60);
    budget_for_two(&env).await;
    let c = create(&env.ctx, args(None)).await.unwrap();
    assert_eq!(status(&env, &a.id), "ready");
    assert_eq!(status(&env, &b.id), "sleeping");
    pgwire::query(&mut held, "select 1").await.unwrap();
    destroy(&env.ctx, &c.id).await.unwrap();

    // With every running clone connected, nothing can be slept: the queue decides.
    let mut held_b = pgwire::connect(port, &b.id, &password(&b.url), 0)
        .await
        .unwrap();
    pgwire::query(&mut held_b, "select 1").await.unwrap();
    let u = usage(&env.ctx).await.unwrap();
    env.ctx.set_config(Config {
        ram_budget: Some(u.ram_bytes + 1),
        ..env.ctx.config()
    });
    let e = create(&env.ctx, args(None)).await.unwrap_err();
    assert_eq!(e.code, mitosis::error::ErrorCode::BudgetFull, "{e}");
    for c in list(&env.ctx).unwrap() {
        assert_eq!(c.status, "ready", "{}", c.id);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_reap_spares_named_and_protected() {
    let Some(env) = setup().await else { return };
    env.ctx.set_config(Config {
        idle_sleep: None,
        idle_reap: Some(Duration::from_secs(1)),
        ..env.ctx.config()
    });
    let u = create(&env.ctx, args(None)).await.unwrap();
    let n = create(&env.ctx, args(Some("n"))).await.unwrap();
    let p = create(
        &env.ctx,
        CloneCreateArgs {
            protect: true,
            ..args(Some("p"))
        },
    )
    .await
    .unwrap();
    let (slept, reaped) = idle_pass(&env.ctx, now() + 100).await.unwrap();
    assert_eq!(reaped, vec![u.id.clone()]);
    assert!(slept.is_empty(), "{slept:?}");
    let ids: Vec<String> = list(&env.ctx).unwrap().into_iter().map(|c| c.id).collect();
    assert!(ids.contains(&n.id) && ids.contains(&p.id) && !ids.contains(&u.id));
    let destroyed = env
        .ctx
        .store()
        .list_events(&EventFilter {
            kind: Some("clone.destroyed".into()),
            clone: Some(u.id.clone()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(destroyed[0].detail["reason"], "idle");
}

#[tokio::test(flavor = "multi_thread")]
async fn destroy_with_open_router_connection_closes_it() {
    let Some(env) = setup().await else { return };
    let port = env.ctx.config().router_port;
    let a = create(&env.ctx, args(Some("a"))).await.unwrap();
    let mut conn = pgwire::connect(port, "a", &password(&a.url), 0)
        .await
        .unwrap();
    pgwire::query(&mut conn, "select 1").await.unwrap();
    let started = Instant::now();
    destroy(&env.ctx, &a.id).await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(8));
    let res = tokio::time::timeout(Duration::from_secs(5), pgwire::query(&mut conn, "select 1"))
        .await
        .expect("the router connection was left open");
    assert!(res.is_err());
    assert_eq!(env.ctx.router().open_conns(&a.id), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn wake_moves_port_when_taken_router_url_unchanged() {
    let Some(env) = setup().await else { return };
    let w = create(&env.ctx, args(Some("w"))).await.unwrap();
    psql_url(&env, &w.url, "insert into t values (2)");
    put_to_sleep(&env, "w").await;
    let _held = TcpListener::bind(("127.0.0.1", w.port)).unwrap();
    // Through the router: the wake moves the port, the client never notices.
    assert_eq!(psql_url(&env, &w.url, "select count(*) from t"), "2");
    let now_info = list(&env.ctx)
        .unwrap()
        .into_iter()
        .find(|c| c.id == w.id)
        .unwrap();
    assert_ne!(now_info.port, w.port);
    assert_eq!(now_info.url, w.url);
    assert_ne!(now_info.direct_url, w.direct_url);
    let res = env.ctx.store().get_reservation("w").unwrap().unwrap();
    assert_eq!(res.port, now_info.port);
    // A wake by hand reports the move.
    put_to_sleep(&env, "w").await;
    let _held2 = TcpListener::bind(("127.0.0.1", now_info.port)).unwrap();
    let (row, warnings) = wake(&env.ctx, "w").await.unwrap();
    assert_ne!(row.port, now_info.port);
    assert!(
        warnings.iter().any(|x| x.contains("was taken")),
        "{warnings:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_wakes_start_once() {
    let Some(env) = setup().await else { return };
    let port = env.ctx.config().router_port;
    let a = create(&env.ctx, args(Some("a"))).await.unwrap();
    put_to_sleep(&env, "a").await;
    let pw = password(&a.url);
    let conns: Vec<_> = (0..5)
        .map(|_| {
            let pw = pw.clone();
            tokio::spawn(async move {
                let mut c = pgwire::connect(port, "a", &pw, 0).await?;
                pgwire::query(&mut c, "select 1")
                    .await
                    .map_err(|(code, msg)| format!("{code}: {msg}"))
            })
        })
        .collect();
    for c in conns {
        c.await.unwrap().unwrap();
    }
    assert_eq!(events(&env, "clone.woke", &a.id), 1);
    assert_eq!(status(&env, &a.id), "ready");
}
