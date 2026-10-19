mod common;

use common::*;
use mitosis::clones::{create, destroy, list};
use mitosis::ctx::{Config, Ctx};
use mitosis::error::ErrorCode;
use mitosis::paths::Home;
use mitosis::protocol::CloneCreateArgs;
use mitosis::templates;
use std::net::TcpListener;

const SEED: &str = "create table t(id int); insert into t values (1)";

fn args(template: &str) -> CloneCreateArgs {
    CloneCreateArgs {
        template: template.into(),
        name: None,
        ttl_secs: 7200,
        durable: false,
        parent_pid: None,
    }
}

/// A second Ctx on the env's home (same store and templates) with a changed config.
fn ctx_with(env: &TestEnv, f: impl FnOnce(&mut Config)) -> Ctx {
    let mut config = env.ctx.config();
    f(&mut config);
    Ctx::open(Home::at(env.tmp.path().to_path_buf()), config).unwrap()
}

#[tokio::test]
async fn clone_is_isolated() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, args("main")).await.unwrap();
    let b = create(&env.ctx, args("main")).await.unwrap();
    psql_url(&env, &a.url, "insert into t values (2)");
    assert_eq!(psql_url(&env, &a.url, "select count(*) from t"), "2");
    assert_eq!(psql_url(&env, &b.url, "select count(*) from t"), "1");
    assert!(
        a.url.starts_with("postgres://postgres:")
            && a.url.contains("@127.0.0.1:")
            && a.url.ends_with("/app")
    );
    assert_ne!(a.port, b.port);
}

#[tokio::test]
async fn destroy_frees_everything() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, args("main")).await.unwrap();
    assert_eq!(list(&env.ctx).unwrap().len(), 1);
    assert_eq!(destroy(&env.ctx, &a.id).await.unwrap(), a.id);
    assert!(!env.ctx.home.clones_dir().join(&a.id).exists());
    assert!(!env.ctx.home.sockets_dir().join(&a.id).exists());
    assert!(list(&env.ctx).unwrap().is_empty());
    assert!(TcpListener::bind(("127.0.0.1", a.port)).is_ok());
    assert_eq!(
        destroy(&env.ctx, &a.id).await.unwrap_err().code,
        ErrorCode::CloneNotFound
    );
}

#[tokio::test]
async fn destroy_by_name() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(
        &env.ctx,
        CloneCreateArgs {
            name: Some("feat-x".into()),
            ..args("main")
        },
    )
    .await
    .unwrap();
    assert_eq!(a.name.as_deref(), Some("feat-x"));
    assert_eq!(destroy(&env.ctx, "feat-x").await.unwrap(), a.id);
}

#[tokio::test]
async fn duplicate_clone_name_rejected() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let named = || CloneCreateArgs {
        name: Some("dup".into()),
        ..args("main")
    };
    create(&env.ctx, named()).await.unwrap();
    let e = create(&env.ctx, named()).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert_eq!(list(&env.ctx).unwrap().len(), 1);
}

#[tokio::test]
async fn cap_reached() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let ctx = ctx_with(&env, |c| c.max_clones = 1);
    let a = create(&ctx, args("main")).await.unwrap();
    let e = create(&ctx, args("main")).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::CapReached);
    assert!(e.message.contains(&a.id), "{}", e.message);
}

#[tokio::test]
async fn low_disk() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let ctx = ctx_with(&env, |c| c.min_free_bytes = u64::MAX);
    assert_eq!(
        create(&ctx, args("main")).await.unwrap_err().code,
        ErrorCode::LowDisk
    );
    assert!(list(&ctx).unwrap().is_empty());
}

#[tokio::test]
async fn port_in_use_is_skipped() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let first = *env.ctx.config().port_range.start();
    let Ok(_busy) = TcpListener::bind(("127.0.0.1", first)) else {
        println!("skipping: port {first} already taken by something else");
        return;
    };
    let a = create(&env.ctx, args("main")).await.unwrap();
    assert_ne!(a.port, first);
    assert_eq!(psql_url(&env, &a.url, "select count(*) from t"), "1");
}

/// Rewrites the current generation row of template `main`.
fn set_current_gen(env: &TestEnv, f: impl FnOnce(&mut mitosis::state::GenerationRow)) {
    let store = env.ctx.store();
    let (_, mut g) = store.current("main").unwrap().unwrap();
    f(&mut g);
    store.delete_generation("main", g.gen).unwrap();
    store.insert_generation(&g).unwrap();
}

#[tokio::test]
async fn version_mismatch() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    set_current_gen(&env, |g| g.pg_major = 99);
    let e = create(&env.ctx, args("main")).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::NoMatchingPostgres);
    assert_eq!(e.hint.as_deref(), Some("brew install postgresql@99"));
}

#[tokio::test]
async fn missing_extension() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    set_current_gen(&env, |g| g.extensions.push("no_such_ext".into()));
    let e = create(&env.ctx, args("main")).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::MissingExtension);
    assert!(e.message.contains("no_such_ext"), "{}", e.message);
    assert_eq!(
        std::fs::read_dir(env.ctx.home.clones_dir())
            .unwrap()
            .count(),
        0
    );
    assert!(list(&env.ctx).unwrap().is_empty());
}

#[tokio::test]
async fn unknown_template() {
    // Fails before any Postgres work, so it runs without Postgres installed.
    let tmp = tmp_home();
    let ctx = Ctx::open(Home::at(tmp.path().to_path_buf()), Config::default()).unwrap();
    let e = create(&ctx, args("nope")).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::TemplateNotFound);
    assert_eq!(
        e.hint.as_deref(),
        Some("mitosis template init nope --seed \"…\"")
    );
}

#[tokio::test]
async fn refresh_while_clone_runs() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, args("main")).await.unwrap();
    templates::refresh(&env.ctx, "main", None).await.unwrap();
    assert_eq!(psql_url(&env, &a.url, "select count(*) from t"), "1");
}

#[tokio::test]
async fn durable_flag() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let durable = create(
        &env.ctx,
        CloneCreateArgs {
            durable: true,
            ..args("main")
        },
    )
    .await
    .unwrap();
    let fast = create(&env.ctx, args("main")).await.unwrap();
    assert_eq!(psql_url(&env, &durable.url, "show fsync"), "on");
    assert_eq!(psql_url(&env, &fast.url, "show fsync"), "off");
}
