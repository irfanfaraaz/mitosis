mod common;

use common::*;
use mitosis::daemon::run as run_daemon;
use mitosis::doctor::{run, Check, Level};
use mitosis::paths::Home;
use mitosis::protocol::{GenKind, PendingRestore, TemplateSource};
use mitosis::state::{GenerationRow, Store, TemplateRow};
use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, Instant};
use tokio::net::UnixStream;

fn find<'a>(checks: &'a [Check], name: &str) -> &'a Check {
    checks
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("no check named {name}"))
}

fn fresh_home() -> (tempfile::TempDir, Home) {
    let tmp = tmp_home();
    let home = Home::at(tmp.path().to_path_buf());
    home.ensure_dirs().unwrap();
    (tmp, home)
}

#[tokio::test]
async fn doctor_flags_bad_config() {
    let (_tmp, home) = fresh_home();
    std::fs::write(home.config_file(), "ram_budget = \"x\"\n").unwrap();
    let checks = run(&home, false).await;
    let c = find(&checks, "config");
    assert_eq!(c.level, Level::Fail);
    assert!(c.detail.contains("ram_budget"), "{}", c.detail);
    assert!(
        !home.daemon_sock().exists(),
        "doctor must not start the daemon"
    );
}

#[tokio::test]
async fn doctor_flags_open_home_permissions() {
    let (_tmp, home) = fresh_home();
    std::fs::set_permissions(&home.root, std::fs::Permissions::from_mode(0o755)).unwrap();
    let checks = run(&home, false).await;
    assert_eq!(find(&checks, "permissions").level, Level::Fail);
    assert_eq!(
        std::fs::metadata(&home.root).unwrap().permissions().mode() & 0o777,
        0o755,
        "doctor only reports"
    );
}

#[tokio::test]
async fn doctor_reports_missing_major() {
    let (_tmp, home) = fresh_home();
    {
        let store = Store::open(&home.state_db()).unwrap();
        let mut t = TemplateRow::new(
            "main",
            "app",
            TemplateSource::Url {
                url: "postgres://u@h/app".into(),
            },
            None,
            604_800,
        );
        t.current_gen = 1;
        t.next_gen = 2;
        store.insert_template(&t).unwrap();
        store
            .insert_generation(&GenerationRow {
                template: "main".into(),
                gen: 1,
                kind: GenKind::Full,
                dir: home.templates_dir().join("main@1"),
                pg_major: 99,
                bin_dir: "/opt/pg99/bin".into(),
                extensions: vec![],
                migrations_hash: None,
                built_at: 1,
                size_bytes: 1,
                from_clone: None,
                from_gen: None,
            })
            .unwrap();
    }
    let checks = run(&home, false).await;
    let c = find(&checks, "template_installs");
    assert_eq!(c.level, Level::Fail);
    assert!(c.detail.contains("99"), "{}", c.detail);
    assert_eq!(c.fix.as_deref(), Some("brew install postgresql@99"));
    assert!(!home.daemon_sock().exists());
}

#[tokio::test]
async fn doctor_works_with_daemon_down() {
    let (_tmp, home) = fresh_home();
    let checks = run(&home, false).await;
    assert_eq!(find(&checks, "daemon").level, Level::Ok);
    assert!(
        !home.daemon_sock().exists(),
        "doctor must not start the daemon"
    );
    assert_eq!(find(&checks, "permissions").level, Level::Ok);
    // The throwaway servers leave nothing behind.
    let leftovers: Vec<_> = std::fs::read_dir(home.logs_dir())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("doctor-"))
        .collect();
    assert!(leftovers.is_empty());
}

#[tokio::test]
async fn doctor_fix_finishes_pending_restore() {
    let Some(env) = TestEnv::new() else { return };
    let root = env.tmp.path().to_path_buf();
    let daemon = tokio::spawn(run_daemon(Home::at(root.clone()), env.ctx.config()));
    let sock = root.join("daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(30);
    while UnixStream::connect(&sock).await.is_err() {
        assert!(!daemon.is_finished(), "daemon exited before binding");
        assert!(Instant::now() < deadline, "daemon socket never appeared");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let project = tempfile::tempdir().unwrap();
    let file = std::fs::canonicalize(project.path()).unwrap().join(".env");
    let url = "postgres://postgres:pw@127.0.0.1:55000/app";
    std::fs::write(&file, format!("DATABASE_URL={url}\n")).unwrap();
    env.ctx
        .store()
        .add_pending_restore(&PendingRestore {
            id: 0,
            file: file.clone(),
            var: "DATABASE_URL".into(),
            previous: Some("postgres://old/db".into()),
            url: url.into(),
        })
        .unwrap();

    let home = Home::at(root);
    let checks = run(&home, true).await;
    assert!(
        find(&checks, "fix").detail.contains("finished 1 of 1"),
        "{}",
        find(&checks, "fix").detail
    );
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "DATABASE_URL=postgres://old/db\n"
    );
    assert!(env.ctx.store().list_pending_restores().unwrap().is_empty());
    daemon.abort();
}
