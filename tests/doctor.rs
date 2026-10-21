mod common;

use common::*;
use mitosis::client;
use mitosis::daemon::run as run_daemon;
use mitosis::doctor::{run, Check, Level};
use mitosis::mask::MaskFile;
use mitosis::paths::Home;
use mitosis::protocol::{GenKind, PendingRestore, Request, Response, TemplateSource};
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
                mask_hash: None,
            })
            .unwrap();
    }
    let checks = run(&home, false).await;
    let c = find(&checks, "template_installs");
    assert_eq!(c.level, Level::Fail);
    assert!(c.detail.contains("99"), "{}", c.detail);
    assert_eq!(c.fix, Some(mitosis::pg::binaries::install_hint(99)));
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

/// A free port this test holds, outside the default `port_range`, where `router_port` is refused.
fn held_port() -> std::net::TcpListener {
    // Above the router_port() range and outside CI's ephemeral range, so once dropped no
    // outgoing connection takes it before the test checks it is free again.
    (61_450..=61_999)
        .find_map(|p| std::net::TcpListener::bind(("127.0.0.1", p)).ok())
        .expect("a free port in 61450-61999")
}

#[tokio::test]
async fn doctor_flags_router_port_taken() {
    let (_tmp, home) = fresh_home();
    // A port this test holds itself: never the default 54300, which a real daemon may use.
    let blocker = held_port();
    let port = blocker.local_addr().unwrap().port();
    std::fs::write(home.config_file(), format!("router_port = {port}\n")).unwrap();
    let checks = run(&home, false).await;
    let c = find(&checks, "router");
    assert_eq!(c.level, Level::Fail, "{}", c.detail);
    assert!(c.detail.contains(&port.to_string()), "{}", c.detail);
    // lsof names this very process; where lsof is missing the check says so instead.
    let me = format!("pid {}", std::process::id());
    assert!(
        c.detail.contains(&me) || c.detail.contains("could not tell"),
        "{}",
        c.detail
    );
    assert!(c.fix.as_deref().unwrap().contains("router_port"));
    assert!(!home.daemon_sock().exists());

    // Free again: the daemon will be able to bind it.
    drop(blocker);
    let checks = run(&home, false).await;
    assert_eq!(find(&checks, "router").level, Level::Ok);
}

#[tokio::test]
async fn doctor_warns_router_listen_public() {
    let (_tmp, home) = fresh_home();
    let free = held_port();
    let port = free.local_addr().unwrap().port();
    drop(free);
    std::fs::write(
        home.config_file(),
        format!("router_port = {port}\nrouter_listen = \"0.0.0.0\"\n"),
    )
    .unwrap();
    let checks = run(&home, false).await;
    let c = find(&checks, "router_listen");
    assert_eq!(c.level, Level::Warn, "{}", c.detail);
    assert!(c.detail.contains("network"), "{}", c.detail);

    std::fs::write(home.config_file(), format!("router_port = {port}\n")).unwrap();
    let checks = run(&home, false).await;
    assert_eq!(find(&checks, "router_listen").level, Level::Ok);
}

#[tokio::test]
async fn doctor_flags_changed_mask_file() {
    let (_tmp, home) = fresh_home();
    let mask = home.root.join("mask.toml");
    std::fs::write(&mask, "[tables.users]\nemail = \"email\"\n").unwrap();
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
        t.mask_path = Some(mask.clone());
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
                mask_hash: Some(MaskFile::hash(&mask).unwrap()),
            })
            .unwrap();
    }
    let checks = run(&home, false).await;
    assert_eq!(find(&checks, "masking").level, Level::Ok);

    std::fs::write(&mask, "[tables.users]\nemail = \"hash\"\n").unwrap();
    let checks = run(&home, false).await;
    let c = find(&checks, "masking");
    assert_eq!(c.level, Level::Warn, "{}", c.detail);
    assert!(c.detail.contains("changed"), "{}", c.detail);
    assert!(c.detail.contains("main"), "{}", c.detail);

    std::fs::remove_file(&mask).unwrap();
    let checks = run(&home, false).await;
    let c = find(&checks, "masking");
    assert_eq!(c.level, Level::Fail, "{}", c.detail);
    assert!(c.detail.contains("missing"), "{}", c.detail);
    assert!(
        !home.daemon_sock().exists(),
        "doctor must not start the daemon"
    );
}

#[tokio::test]
async fn doctor_reports_no_managed_builds_and_event_log() {
    let (_tmp, home) = fresh_home();
    drop(Store::open(&home.state_db()).unwrap());
    let checks = run(&home, false).await;
    assert_eq!(find(&checks, "managed_postgres").level, Level::Ok);
    assert_eq!(find(&checks, "branch_templates").level, Level::Ok);
    let c = find(&checks, "events");
    assert_eq!(c.level, Level::Ok, "{}", c.detail);
    assert!(c.detail.contains("0 event"), "{}", c.detail);
}

#[tokio::test(flavor = "multi_thread")]
async fn status_shows_router() {
    let Some(env) = TestEnv::new() else { return };
    let root = env.tmp.path().to_path_buf();
    let config = mitosis::ctx::Config {
        router: true,
        ..env.ctx.config()
    };
    let port = config.router_port;
    let daemon = tokio::spawn(run_daemon(Home::at(root.clone()), config));
    let sock = root.join("daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(30);
    while UnixStream::connect(&sock).await.is_err() {
        assert!(!daemon.is_finished(), "daemon exited before binding");
        assert!(Instant::now() < deadline, "daemon socket never appeared");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let home = Home::at(root.clone());
    loop {
        let Response::Status(s) = client::request(&home, Request::Status).await.unwrap() else {
            panic!("not a status");
        };
        let r = s.router.expect("router status with the router on");
        if r.listening {
            assert_eq!((r.listen.as_str(), r.port), ("127.0.0.1", port));
            break;
        }
        assert!(Instant::now() < deadline, "router never listened: {r:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The CLI line.
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_mitosis"))
        .args(["daemon", "status"])
        .env("MITOSIS_HOME", &root)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains(&format!("router    listening on 127.0.0.1:{port}")),
        "{text}"
    );
    // The router checks agree with the daemon.
    let checks = run(&home, false).await;
    assert_eq!(find(&checks, "router").level, Level::Ok);

    let _ = client::request(&home, Request::Shutdown).await;
    let _ = tokio::time::timeout(Duration::from_secs(30), daemon).await;
}

#[test]
fn router_state_lines() {
    use mitosis::cli::router_state;
    use mitosis::protocol::RouterStatus;
    assert_eq!(router_state(&None), "off");
    let up = RouterStatus {
        listening: true,
        listen: "127.0.0.1".into(),
        port: 54300,
        error: None,
    };
    assert_eq!(
        router_state(&Some(up.clone())),
        "listening on 127.0.0.1:54300"
    );
    let down = RouterStatus {
        listening: false,
        error: Some("cannot listen on 127.0.0.1:54300: Address already in use".into()),
        ..up
    };
    let line = router_state(&Some(down));
    assert!(
        line.starts_with("not listening:") && line.contains("Address already in use"),
        "{line}"
    );
}

/// Store schema 3 and the home layout are unchanged since 1.0, so a home made by the current code
/// is a 1.0 home: it must open on the apfs back end and keep its clones.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn one_dot_oh_home_opens_unchanged() {
    use mitosis::ctx::{Config, Ctx};
    let (_tmp, home) = fresh_home();
    let ctx = Ctx::open(home.clone(), Config::default()).unwrap();
    assert_eq!(ctx.backend.name(), "apfs");
    let before = mitosis::clones::list(&ctx).unwrap().len();
    drop(ctx);
    let ctx = Ctx::open(home.clone(), Config::default()).unwrap();
    assert_eq!(ctx.backend.name(), "apfs");
    assert_eq!(mitosis::clones::list(&ctx).unwrap().len(), before);
    let checks = run(&home, false).await;
    let c = find(&checks, "home");
    assert_eq!(c.level, Level::Ok);
    assert!(c.detail.contains("apfs"), "{}", c.detail);
}
