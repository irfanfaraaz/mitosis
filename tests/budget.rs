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
