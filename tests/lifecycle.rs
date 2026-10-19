mod common;

use common::*;
use mitosis::clones::{create, list};
use mitosis::ctx::{Config, Ctx};
use mitosis::lifecycle::{parent_alive, reap_expired, reconcile, watch_parent};
use mitosis::paths::Home;
use mitosis::pg::{newest, stop};
use mitosis::proc_util::process_start_time;
use mitosis::protocol::CloneCreateArgs;
use mitosis::state::{CloneRow, CloneStatus};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// A second Ctx on the env's home: what the daemon sees after a restart.
fn restarted(env: &TestEnv) -> Arc<Ctx> {
    Arc::new(
        Ctx::open(
            Home::at(env.tmp.path().to_path_buf()),
            env.ctx.config.clone(),
        )
        .unwrap(),
    )
}

fn sleeper() -> Child {
    Command::new("sleep").arg("60").spawn().unwrap()
}

/// Kills and reaps the child, so no zombie keeps its pid alive.
fn kill(mut child: Child) {
    child.kill().unwrap();
    child.wait().unwrap();
}

#[tokio::test]
async fn ttl_reaps() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(
        &env.ctx,
        CloneCreateArgs {
            ttl_secs: 1,
            ..args("main")
        },
    )
    .await
    .unwrap();
    assert!(reap_expired(&env.ctx, now() - 10).await.unwrap().is_empty());
    assert_eq!(
        reap_expired(&env.ctx, now() + 2).await.unwrap(),
        vec![a.id.clone()]
    );
    assert!(!env.ctx.home.clones_dir().join(&a.id).exists());
    assert!(list(&env.ctx).unwrap().is_empty());
}

#[tokio::test]
async fn parent_exit_reaps() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let child = sleeper();
    let pid = child.id() as i32;
    let a = create(
        &env.ctx,
        CloneCreateArgs {
            parent_pid: Some(pid),
            ..args("main")
        },
    )
    .await
    .unwrap();
    watch_parent(env.ctx.clone(), a.id.clone(), pid);
    kill(child);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !list(&env.ctx).unwrap().is_empty() {
        assert!(
            Instant::now() < deadline,
            "clone {} still listed 5 s after its parent exited",
            a.id
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!env.ctx.home.clones_dir().join(&a.id).exists());
}

#[tokio::test]
async fn reconcile_adopts_live_clone() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, args("main")).await.unwrap();
    let report = reconcile(restarted(&env), now()).await.unwrap();
    assert_eq!(report.adopted, vec![a.id.clone()]);
    assert!(report.destroyed.is_empty() && report.records_dropped.is_empty());
    assert!(
        report.orphan_folders_removed.is_empty(),
        "{:?}",
        report.orphan_folders_removed
    );
    assert_eq!(psql_url(&env, &a.url, "select count(*) from t"), "1");
}

/// Needs no Postgres: only folders are involved.
#[tokio::test]
async fn reconcile_removes_orphan_folder() {
    let tmp = tmp_home();
    let ctx = Arc::new(Ctx::open(Home::at(tmp.path().to_path_buf()), Config::default()).unwrap());
    let zzz = ctx.home.clones_dir().join("zzz");
    std::fs::create_dir(&zzz).unwrap();
    std::fs::write(zzz.join("file"), "x").unwrap();
    let sock = ctx.home.sockets_dir().join("cdead00");
    std::fs::create_dir(&sock).unwrap();
    let building = ctx.home.templates_dir().join(".main.building-1");
    let old = ctx.home.templates_dir().join(".main.old-1");
    let template = ctx.home.templates_dir().join("main");
    for d in [&building, &old, &template] {
        std::fs::create_dir(d).unwrap();
    }

    let report = reconcile(ctx.clone(), now()).await.unwrap();
    for p in [&zzz, &sock, &building, &old] {
        assert!(
            report.orphan_folders_removed.contains(p),
            "{} not reported: {:?}",
            p.display(),
            report
        );
        assert!(!p.exists(), "{} still exists", p.display());
    }
    assert!(template.is_dir(), "a built template is not an orphan");
    assert!(
        report.adopted.is_empty()
            && report.destroyed.is_empty()
            && report.records_dropped.is_empty()
    );
}

#[tokio::test]
async fn reconcile_drops_record_without_folder() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, args("main")).await.unwrap();
    let dir = env.ctx.home.clones_dir().join(&a.id);
    stop(
        newest(&env.ctx.installs).unwrap(),
        &dir,
        Duration::from_secs(5),
    )
    .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    let report = reconcile(restarted(&env), now()).await.unwrap();
    assert_eq!(report.records_dropped, vec![a.id.clone()]);
    assert!(report.destroyed.is_empty() && report.adopted.is_empty());
    assert!(list(&env.ctx).unwrap().is_empty());
}

#[tokio::test]
async fn reconcile_undoes_half_created() {
    let Some(env) = TestEnv::new() else { return };
    let tpl = template(&env, "main", SEED).await;
    let id = "cfeed01".to_string();
    let dir = env.ctx.home.clones_dir().join(&id);
    let t = now();
    let row = CloneRow {
        id: id.clone(),
        name: None,
        template: tpl.name.clone(),
        dir: dir.clone(),
        port: *env.ctx.config.port_range.start(),
        socket_dir: env.ctx.home.sockets_dir().join(&id),
        password: "x".repeat(32),
        database: tpl.database.clone(),
        pg_pid: None,
        pg_start_time: None,
        parent_pid: None,
        parent_start_time: None,
        created_at: t,
        expires_at: t + 7200,
        durable: false,
        status: CloneStatus::Creating,
    };
    env.ctx.store().insert_clone(&row).unwrap();
    env.ctx
        .backend
        .clone_dir(&env.ctx.home.templates_dir().join("main"), &dir)
        .unwrap();

    let report = reconcile(restarted(&env), now()).await.unwrap();
    assert_eq!(report.destroyed, vec![id.clone()]);
    assert!(!dir.exists());
    assert!(env.ctx.store().get_clone(&id).unwrap().is_none());
}

#[tokio::test]
async fn reconcile_destroys_clone_of_dead_parent() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let child = sleeper();
    let pid = child.id() as i32;
    let a = create(
        &env.ctx,
        CloneCreateArgs {
            parent_pid: Some(pid),
            ..args("main")
        },
    )
    .await
    .unwrap();
    kill(child);
    let report = reconcile(restarted(&env), now()).await.unwrap();
    assert_eq!(report.destroyed, vec![a.id.clone()]);
    assert!(report.adopted.is_empty());
    assert!(!env.ctx.home.clones_dir().join(&a.id).exists());
    assert!(list(&env.ctx).unwrap().is_empty());
}

/// Needs no Postgres.
#[test]
fn parent_alive_checks_pid_and_start_time() {
    let me = std::process::id() as i32;
    let start = process_start_time(me).unwrap();
    assert!(parent_alive(me, None));
    assert!(parent_alive(me, Some(start)));
    assert!(
        !parent_alive(me, Some(start - 100)),
        "a different start time means a reused pid"
    );

    let mut child = Command::new("true").spawn().unwrap();
    let pid = child.id() as i32;
    child.wait().unwrap();
    assert!(!parent_alive(pid, None));
}
