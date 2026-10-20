mod common;

use common::*;
use mitosis::clones::{create, list};
use mitosis::lifecycle::reconcile;
use mitosis::pg::newest;
use mitosis::protocol::{CloneCreateArgs, CloneInfo, EnvTarget};
use mitosis::state::CloneRow;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::time::Duration;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

const SEED: &str = "create table t(id int); insert into t values (1)";

fn args(name: Option<&str>, durable: bool) -> CloneCreateArgs {
    CloneCreateArgs {
        template: "main".into(),
        name: name.map(String::from),
        ttl_secs: 7200,
        durable,
        parent_pid: None,
        fresh: false,
        protect: false,
        env: None,
        wait_secs: None,
        branch: None,
    }
}

fn row(env: &TestEnv, id: &str) -> Option<CloneRow> {
    env.ctx.store().get_clone(id).unwrap()
}

fn listed(env: &TestEnv, id: &str) -> CloneInfo {
    list(&env.ctx)
        .unwrap()
        .into_iter()
        .find(|c| c.id == id)
        .expect("clone listed")
}

/// Clean shutdown, as at a normal reboot.
fn pg_ctl_stop(env: &TestEnv, info: &CloneInfo) {
    let installs = env.ctx.installs();
    let install = newest(&installs).unwrap();
    let dir = row(env, &info.id).unwrap().dir;
    let out = Command::new(install.bin("pg_ctl"))
        .arg("stop")
        .arg("-D")
        .arg(&dir)
        .args(["-m", "fast", "-w"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Power loss: the postmaster dies without a shutdown checkpoint.
fn kill_9(env: &TestEnv, info: &CloneInfo) {
    let pid = row(env, &info.id).unwrap().pg_pid.unwrap();
    let st = Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .unwrap();
    assert!(st.success());
    // Its children notice the postmaster is gone and exit, releasing shared memory. They all have
    // the data folder as their working folder, so wait until no process has.
    let dir = row(env, &info.id).unwrap().dir;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let users = Command::new("lsof")
            .args(["-t", "+d"])
            .arg(&dir)
            .output()
            .unwrap();
        if users.stdout.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "backends still in {}",
            dir.display()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The router is off in `TestEnv`, so this is the v0.2 restart.
#[tokio::test]
async fn named_clone_restarts_after_clean_stop() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, args(Some("x"), false)).await.unwrap();
    psql_url(&env, &a.url, "insert into t values (2)");
    pg_ctl_stop(&env, &a);
    let report = reconcile(env.ctx.clone(), now()).await.unwrap();
    assert!(report.adopted.contains(&a.id), "{report:?}");
    assert!(report.warnings.is_empty(), "{report:?}");
    assert_eq!(psql_url(&env, &a.url, "select count(*) from t"), "2");
    assert!(!row(&env, &a.id).unwrap().suspect);
    let info = listed(&env, &a.id);
    assert_eq!(info.url, a.url);
    assert!(!info.suspect && info.warnings.is_empty());
}

#[tokio::test]
async fn unclean_fsync_off_named_clone_is_suspect() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let y = create(&env.ctx, args(Some("y"), false)).await.unwrap();
    kill_9(&env, &y);
    let report = reconcile(env.ctx.clone(), now()).await.unwrap();
    assert!(report.adopted.contains(&y.id), "{report:?}");
    assert!(row(&env, &y.id).unwrap().suspect);
    assert_eq!(psql_url(&env, &y.url, "select 1"), "1");
    let hint = "mitosis clone --name y --fresh";
    assert!(
        report.warnings.iter().any(|w| w.contains(hint)),
        "{report:?}"
    );
    let info = listed(&env, &y.id);
    assert!(info.suspect);
    assert!(info.warnings.iter().any(|w| w.contains(hint)), "{info:?}");
}

#[tokio::test]
async fn unclean_unnamed_clone_is_destroyed() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let c = create(&env.ctx, args(None, false)).await.unwrap();
    let dir = row(&env, &c.id).unwrap().dir;
    kill_9(&env, &c);
    let report = reconcile(env.ctx.clone(), now()).await.unwrap();
    assert_eq!(report.destroyed, vec![c.id.clone()], "{report:?}");
    assert!(row(&env, &c.id).is_none());
    assert!(!dir.exists());
}

#[tokio::test]
async fn durable_unclean_clone_is_not_suspect() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let d = create(&env.ctx, args(Some("d"), true)).await.unwrap();
    psql_url(&env, &d.url, "insert into t values (2)");
    kill_9(&env, &d);
    let report = reconcile(env.ctx.clone(), now()).await.unwrap();
    assert!(report.adopted.contains(&d.id), "{report:?}");
    assert!(!row(&env, &d.id).unwrap().suspect);
    assert!(report.warnings.is_empty(), "{report:?}");
    assert_eq!(psql_url(&env, &d.url, "select count(*) from t"), "2");
}

#[tokio::test]
async fn dead_named_clone_restarts_without_reboot() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let z = create(&env.ctx, args(Some("z"), false)).await.unwrap();
    psql_url(&env, &z.url, "insert into t values (2)");
    // No reboot: the server simply died (a manual stop, a full disk). It is restarted, not destroyed.
    pg_ctl_stop(&env, &z);
    let report = reconcile(env.ctx.clone(), now()).await.unwrap();
    assert!(report.adopted.contains(&z.id), "{report:?}");
    assert!(report.destroyed.is_empty(), "{report:?}");
    assert_eq!(psql_url(&env, &z.url, "select count(*) from t"), "2");
    assert_eq!(listed(&env, &z.id).url, z.url);
}

#[tokio::test]
async fn dead_unnamed_clone_with_parent_is_destroyed() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    // This test process is a live parent, so only the dead server decides.
    let c = create(
        &env.ctx,
        CloneCreateArgs {
            parent_pid: Some(std::process::id() as i32),
            ..args(None, false)
        },
    )
    .await
    .unwrap();
    let dir = row(&env, &c.id).unwrap().dir;
    pg_ctl_stop(&env, &c);
    let report = reconcile(env.ctx.clone(), now()).await.unwrap();
    assert_eq!(report.destroyed, vec![c.id.clone()], "{report:?}");
    assert!(row(&env, &c.id).is_none());
    assert!(!dir.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn named_clone_sleeps_after_dead_server_and_router_wakes_it() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    start_router(&env).await;
    let x = create(&env.ctx, args(Some("x"), false)).await.unwrap();
    psql_url(&env, &x.url, "insert into t values (2)");
    pg_ctl_stop(&env, &x);
    // With the router on, recovery marks it sleeping instead of starting it.
    let report = reconcile(env.ctx.clone(), now()).await.unwrap();
    assert!(report.adopted.contains(&x.id), "{report:?}");
    assert!(report.destroyed.is_empty(), "{report:?}");
    let r = row(&env, &x.id).unwrap();
    assert_eq!(r.pg_pid, None);
    assert_eq!(listed(&env, &x.id).status, "sleeping");
    // A second gc leaves it asleep.
    reconcile(env.ctx.clone(), now()).await.unwrap();
    assert_eq!(listed(&env, &x.id).status, "sleeping");
    // The first router connection wakes it, data intact.
    assert_eq!(psql_url(&env, &x.url, "select count(*) from t"), "2");
    let info = listed(&env, &x.id);
    assert_eq!(info.status, "ready");
    assert_eq!(info.url, x.url);
    assert!(!info.suspect);
}

/// After a port move, a `.env` file the daemon cannot write keeps the old URL, and so does its
/// record, so the restore at destroy still finds it.
#[tokio::test]
async fn restart_keeps_record_url_when_rewrite_fails() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let project = tempfile::tempdir().unwrap();
    let file = std::fs::canonicalize(project.path()).unwrap().join(".env");
    let target = EnvTarget {
        file: file.clone(),
        var: "DATABASE_URL".into(),
        previous: Some("old".into()),
    };
    let a = create(
        &env.ctx,
        CloneCreateArgs {
            env: Some(target),
            ..args(Some("w"), false)
        },
    )
    .await
    .unwrap();
    std::fs::write(&file, format!("DATABASE_URL={}\n", a.url)).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o400)).unwrap();
    pg_ctl_stop(&env, &a);
    let Ok(_busy) = std::net::TcpListener::bind(("127.0.0.1", a.port)) else {
        println!("skipping: could not hold port {}", a.port);
        return;
    };
    let report = reconcile(env.ctx.clone(), now()).await.unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(report.adopted.contains(&a.id), "{report:?}");
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("could not update")),
        "{report:?}"
    );
    assert_ne!(row(&env, &a.id).unwrap().port, a.port, "the port moved");
    let recs = env.ctx.store().env_records(&a.id).unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].url, a.url, "the record names what the file holds");
    assert_eq!(recs[0].previous.as_deref(), Some("old"));
}

/// A Ready clone whose server is down says so in `list` until gc recovers it.
#[tokio::test]
async fn list_warns_when_a_ready_server_is_down() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    let a = create(&env.ctx, args(Some("down"), false)).await.unwrap();
    assert!(listed(&env, &a.id).warnings.is_empty());
    pg_ctl_stop(&env, &a);
    let info = listed(&env, &a.id);
    assert!(
        info.warnings.iter().any(|w| w.contains("is not running")),
        "{:?}",
        info.warnings
    );
    reconcile(env.ctx.clone(), now()).await.unwrap();
    assert!(listed(&env, &a.id).warnings.is_empty(), "recovered");
}
