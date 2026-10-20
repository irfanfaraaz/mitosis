mod common;

use common::*;
use mitosis::clones::{create, destroy};
use mitosis::ctx::Config;
use mitosis::error::ErrorCode;
use mitosis::protocol::CloneCreateArgs;
use std::time::{Duration, Instant};

const SEED: &str = "create table t(id int)";

fn args(name: Option<&str>, wait_secs: Option<u64>) -> CloneCreateArgs {
    CloneCreateArgs {
        template: "main".into(),
        name: name.map(String::from),
        ttl_secs: 7200,
        durable: false,
        parent_pid: None,
        fresh: false,
        protect: false,
        env: None,
        wait_secs,
        branch: None,
    }
}

fn cap(env: &TestEnv, max_clones: usize) {
    env.ctx.set_config(Config {
        max_clones,
        ..env.ctx.config()
    });
}

#[tokio::test]
async fn queue_admits_after_destroy() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    cap(&env, 1);
    let a = create(&env.ctx, args(None, None)).await.unwrap();
    let ctx = env.ctx.clone();
    let b = tokio::spawn(async move { create(&ctx, args(None, Some(10))).await });
    tokio::time::sleep(Duration::from_secs(1)).await;
    destroy(&env.ctx, &a.id).await.unwrap();
    let b = b.await.unwrap().unwrap();
    assert!(b.waited_ms >= 900, "waited {} ms", b.waited_ms);
}

#[tokio::test]
async fn queue_times_out_with_cap_reached() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    cap(&env, 1);
    create(&env.ctx, args(None, None)).await.unwrap();
    let start = Instant::now();
    let e = create(&env.ctx, args(None, Some(1))).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::CapReached);
    assert!(start.elapsed() >= Duration::from_millis(900));
}

#[tokio::test]
async fn ram_budget_full_names_limit() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    env.ctx.set_config(Config {
        ram_budget: Some(1),
        ..env.ctx.config()
    });
    let e = create(&env.ctx, args(None, Some(0))).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::BudgetFull);
    assert!(e.message.contains("ram_budget"), "{}", e.message);
}

#[tokio::test]
async fn queue_is_fifo() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    cap(&env, 1);
    let first = create(&env.ctx, args(None, None)).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    for i in 0..3 {
        let (ctx, tx) = (env.ctx.clone(), tx.clone());
        tokio::spawn(async move {
            let info = create(&ctx, args(None, Some(60))).await.unwrap();
            tx.send((i, info.id)).unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut holder = first.id;
    let mut order = Vec::new();
    for _ in 0..3 {
        destroy(&env.ctx, &holder).await.unwrap();
        let (i, id) = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("a waiter was admitted")
            .unwrap();
        order.push(i);
        holder = id;
    }
    assert_eq!(order, vec![0, 1, 2]);
}

#[tokio::test]
async fn wait_zero_fails_at_once() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    cap(&env, 1);
    create(&env.ctx, args(None, None)).await.unwrap();
    let start = Instant::now();
    let e = create(&env.ctx, args(None, Some(0))).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::CapReached);
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn existing_named_clone_does_not_queue() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    cap(&env, 1);
    create(&env.ctx, args(Some("x"), None)).await.unwrap();
    let again = create(&env.ctx, args(Some("x"), Some(0))).await.unwrap();
    assert!(again.existing);
    assert_eq!(again.waited_ms, 0);
}

#[tokio::test]
async fn disconnected_waiter_leaves_the_queue() {
    use mitosis::client::request;
    use mitosis::protocol::{Request, RequestEnvelope, Response, PROTOCOL_VERSION};
    use tokio::io::AsyncWriteExt;

    let Some(env) = TestEnv::new() else { return };
    let home = mitosis::paths::Home::at(env.tmp.path().to_path_buf());
    let config = Config {
        max_clones: 1,
        ..env.ctx.config()
    };
    let daemon = tokio::spawn(mitosis::daemon::run(home.clone(), config));
    let sock = home.daemon_sock();
    let deadline = Instant::now() + Duration::from_secs(30);
    while tokio::net::UnixStream::connect(&sock).await.is_err() {
        assert!(Instant::now() < deadline, "daemon socket never appeared");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let built = request(
        &home,
        Request::TemplateInit(template_args("main", seed(SEED))),
    )
    .await
    .unwrap();
    assert!(matches!(built, Response::TemplateBuilt(_)));
    let make = |wait| request(&home, Request::CloneCreate(args(None, Some(wait))));
    let Response::CloneCreated(a) = make(0).await.unwrap() else {
        panic!("not created")
    };

    // A client queues for room, then hangs up.
    let mut raw = tokio::net::UnixStream::connect(&sock).await.unwrap();
    let line = serde_json::to_string(&RequestEnvelope {
        v: PROTOCOL_VERSION,
        request: Request::CloneCreate(args(None, Some(10))),
    })
    .unwrap();
    raw.write_all(format!("{line}\n").as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(raw);
    tokio::time::sleep(Duration::from_millis(300)).await;

    request(
        &home,
        Request::CloneDestroy {
            id_or_name: a.id,
            force: false,
            forget: false,
        },
    )
    .await
    .unwrap();
    assert!(matches!(make(5).await.unwrap(), Response::CloneCreated(_)));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let Response::Clones { clones } = request(&home, Request::CloneList).await.unwrap() else {
        panic!("not a list")
    };
    assert_eq!(clones.len(), 1, "the abandoned create made a clone");

    request(&home, Request::Shutdown).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(30), daemon).await;
}
