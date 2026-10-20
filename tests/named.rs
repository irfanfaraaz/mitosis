mod common;

use common::*;
use mitosis::clones::{create, destroy, destroy_with, list};
use mitosis::error::ErrorCode;
use mitosis::lifecycle::reap_expired;
use mitosis::protocol::CloneCreateArgs;
use std::collections::HashSet;
use std::net::TcpListener;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

const SEED: &str = "create table t(id int); insert into t values (1)";

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
        wait_secs: None,
        branch: None,
    }
}

fn named(n: &str) -> CloneCreateArgs {
    args(Some(n))
}

#[tokio::test]
async fn named_clone_keeps_url_across_recreate() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, named("x")).await.unwrap();
    destroy(&env.ctx, "x").await.unwrap();
    let b = create(&env.ctx, named("x")).await.unwrap();
    assert_eq!(a.url, b.url);
    assert!(b.warnings.is_empty());
}

#[tokio::test]
async fn named_clone_moves_port_when_reserved_port_is_taken() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, named("x")).await.unwrap();
    destroy(&env.ctx, "x").await.unwrap();
    let _held = TcpListener::bind(("127.0.0.1", a.port)).unwrap();
    let b = create(&env.ctx, named("x")).await.unwrap();
    assert_ne!(a.port, b.port);
    assert!(
        b.warnings.iter().any(|w| w.contains("URL changed")),
        "{:?}",
        b.warnings
    );
    // Same password, new port, and the reservation follows.
    assert_eq!(
        a.url.replace(&a.port.to_string(), &b.port.to_string()),
        b.url
    );
    assert_eq!(
        env.ctx.store().get_reservation("x").unwrap().unwrap().port,
        b.port
    );
}

#[tokio::test]
async fn unnamed_clone_skips_reserved_port() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, named("x")).await.unwrap();
    destroy(&env.ctx, "x").await.unwrap();
    let u = create(&env.ctx, args(None)).await.unwrap();
    assert_ne!(u.port, a.port);
    assert!(env.ctx.store().reserved_ports().unwrap().contains(&a.port));
}

#[tokio::test]
async fn repeat_named_create_returns_existing() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, named("x")).await.unwrap();
    assert!(!a.existing);
    let b = create(&env.ctx, named("x")).await.unwrap();
    assert!(b.existing);
    assert_eq!((a.id, a.url), (b.id, b.url));
    assert_eq!(list(&env.ctx).unwrap().len(), 1);
}

#[tokio::test]
async fn repeat_with_other_durable_is_refused() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    create(&env.ctx, named("x")).await.unwrap();
    let e = create(
        &env.ctx,
        CloneCreateArgs {
            durable: true,
            ..named("x")
        },
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e.hint.unwrap_or_default().contains("--fresh"));
}

#[tokio::test]
async fn repeat_with_longer_ttl_extends() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, named("x")).await.unwrap();
    let shorter = create(
        &env.ctx,
        CloneCreateArgs {
            ttl_secs: 60,
            ..named("x")
        },
    )
    .await
    .unwrap();
    assert_eq!(shorter.expires_at, a.expires_at);
    let b = create(
        &env.ctx,
        CloneCreateArgs {
            ttl_secs: 20_000,
            ..named("x")
        },
    )
    .await
    .unwrap();
    assert!(b.existing && b.expires_at > a.expires_at + 10_000);
}

#[tokio::test]
async fn fresh_resets_data_same_url() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, named("x")).await.unwrap();
    psql_url(&env, &a.url, "insert into t values (2)");
    assert_eq!(psql_url(&env, &a.url, "select count(*) from t"), "2");
    let b = create(
        &env.ctx,
        CloneCreateArgs {
            fresh: true,
            ..named("x")
        },
    )
    .await
    .unwrap();
    assert!(!b.existing);
    assert_ne!(a.id, b.id);
    assert_eq!(a.url, b.url);
    assert_eq!(psql_url(&env, &b.url, "select count(*) from t"), "1");
}

#[tokio::test]
async fn protected_survives_ttl_and_needs_force() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let no_name = create(
        &env.ctx,
        CloneCreateArgs {
            protect: true,
            env: None,
            wait_secs: None,
            branch: None,
            ..args(None)
        },
    )
    .await
    .unwrap_err();
    assert_eq!(no_name.code, ErrorCode::InvalidRequest);
    let a = create(
        &env.ctx,
        CloneCreateArgs {
            protect: true,
            env: None,
            wait_secs: None,
            branch: None,
            parent_pid: Some(std::process::id() as i32),
            ttl_secs: 1,
            ..named("keep")
        },
    )
    .await
    .unwrap();
    assert!(a.protected && a.expires_at == i64::MAX && a.parent_pid.is_none());
    assert!(reap_expired(&env.ctx, now() + 1_000_000)
        .await
        .unwrap()
        .is_empty());
    let e = destroy_with(&env.ctx, "keep", false, false)
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e
        .hint
        .unwrap_or_default()
        .contains("mitosis destroy keep --force"));
    assert_eq!(list(&env.ctx).unwrap().len(), 1);
    assert_eq!(
        destroy_with(&env.ctx, "keep", true, false).await.unwrap(),
        a.id
    );
}

#[tokio::test]
async fn forget_releases_port() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, named("x")).await.unwrap();
    destroy_with(&env.ctx, "x", false, true).await.unwrap();
    assert!(env.ctx.store().get_reservation("x").unwrap().is_none());
    assert!(!env.ctx.store().reserved_ports().unwrap().contains(&a.port));
    // The freed port is the lowest again, so an unnamed clone may take it.
    let u = create(&env.ctx, args(None)).await.unwrap();
    assert_eq!(u.port, a.port);
    // `--forget` also works after the clone is gone.
    create(&env.ctx, named("y")).await.unwrap();
    destroy(&env.ctx, "y").await.unwrap();
    assert_eq!(destroy_with(&env.ctx, "y", false, true).await.unwrap(), "y");
    assert_eq!(env.ctx.store().reserved_ports().unwrap(), HashSet::new());
}

#[tokio::test]
async fn mcp_refuses_other_sessions_name() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let mut a = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .unwrap();
    let mut b = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .unwrap();
    let with = |pid: u32| CloneCreateArgs {
        parent_pid: Some(pid as i32),
        ..named("x")
    };
    let first = create(&env.ctx, with(a.id())).await.unwrap();
    let same = create(&env.ctx, with(a.id())).await.unwrap();
    assert!(same.existing && same.id == first.id);
    let e = create(&env.ctx, with(b.id())).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(
        e.message.contains("belongs to another session"),
        "{}",
        e.message
    );
    destroy(&env.ctx, "x").await.unwrap();
    let _ = a.kill();
    let _ = b.kill();
    let _ = (a.wait(), b.wait());
}

#[tokio::test]
async fn losing_same_name_create_keeps_the_reservation() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, named("x")).await.unwrap();
    // A create that reaches the reservation step while `x` runs fails on the unique name.
    let held: Vec<TcpListener> = vec![TcpListener::bind(("127.0.0.1", 0)).unwrap()];
    drop(held);
    let before = env.ctx.store().get_reservation("x").unwrap().unwrap();
    let mut row = env.ctx.store().get_clone("x").unwrap().unwrap();
    row.id = "ffffff".into();
    row.port = before.port + 1;
    assert!(env.ctx.store().insert_clone(&row).is_err());
    assert_eq!(
        env.ctx.store().get_reservation("x").unwrap().unwrap(),
        before
    );
    assert_eq!(before.port, a.port);
}

#[tokio::test]
async fn gc_keeps_reservation_of_running_clone() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, named("x")).await.unwrap();
    let mut r = env.ctx.store().get_reservation("x").unwrap().unwrap();
    r.last_used = now() - 31 * 86_400;
    env.ctx.store().upsert_reservation(&r).unwrap();
    mitosis::lifecycle::reconcile(env.ctx.clone(), now())
        .await
        .unwrap();
    assert!(env.ctx.store().get_reservation("x").unwrap().is_some());
    destroy(&env.ctx, "x").await.unwrap();
    mitosis::lifecycle::reconcile(env.ctx.clone(), now())
        .await
        .unwrap();
    assert!(env.ctx.store().get_reservation("x").unwrap().is_none());
    let _ = a;
}
