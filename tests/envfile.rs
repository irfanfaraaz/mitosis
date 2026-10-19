mod common;

use common::*;
use mitosis::client;
use mitosis::daemon::run;
use mitosis::envfile::{create_clone, finish_pending, get_var};
use mitosis::error::Result;
use mitosis::lifecycle::reap_expired;
use mitosis::paths::Home;
use mitosis::protocol::{CloneCreateArgs, CloneInfo, Request, Response};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;

const SEED: &str = "create table t(id int); insert into t values (1)";

struct Fixture {
    env: TestEnv,
    home: Home,
    daemon: JoinHandle<Result<()>>,
    project: tempfile::TempDir,
}

impl Fixture {
    /// A built template, a daemon on the env's home, and an empty project folder.
    async fn new() -> Option<Fixture> {
        let env = TestEnv::new()?;
        template(&env, "main", SEED).await;
        let root = env.tmp.path().to_path_buf();
        let daemon = tokio::spawn(run(Home::at(root.clone()), env.ctx.config()));
        let sock = root.join("daemon.sock");
        let deadline = Instant::now() + Duration::from_secs(30);
        while UnixStream::connect(&sock).await.is_err() {
            assert!(!daemon.is_finished(), "daemon exited before binding");
            assert!(Instant::now() < deadline, "daemon socket never appeared");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let project = tempfile::tempdir().unwrap();
        Some(Fixture {
            env,
            home: Home::at(root),
            daemon,
            project,
        })
    }

    fn dotenv(&self, text: &str) -> PathBuf {
        let p = std::fs::canonicalize(self.project.path())
            .unwrap()
            .join(".env");
        std::fs::write(&p, text).unwrap();
        p
    }

    async fn clone(&self, ttl_secs: u64, file: &Path) -> CloneInfo {
        let args = CloneCreateArgs {
            template: "main".into(),
            name: None,
            ttl_secs,
            durable: false,
            parent_pid: None,
            fresh: false,
            protect: false,
            env: None,
        };
        create_clone(&self.home, args, Some(file.to_path_buf()), None)
            .await
            .unwrap()
    }

    async fn destroy(&self, id: &str) {
        let req = Request::CloneDestroy {
            id_or_name: id.into(),
            force: false,
            forget: false,
        };
        assert!(matches!(
            client::request(&self.home, req).await.unwrap(),
            Response::Destroyed { .. }
        ));
    }

    async fn stop(self) {
        let _ = client::request(&self.home, Request::Shutdown).await;
        let _ = tokio::time::timeout(Duration::from_secs(30), self.daemon).await;
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn read(p: &Path) -> String {
    std::fs::read_to_string(p).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn env_written_and_restored_on_destroy() {
    let Some(fx) = Fixture::new().await else {
        return;
    };
    let file = fx.dotenv("DATABASE_URL=old\nOTHER=1\n");
    let info = fx.clone(7200, &file).await;
    let text = read(&file);
    assert_eq!(get_var(&text, "DATABASE_URL").as_deref(), Some(&*info.url));
    assert!(text.contains("OTHER=1\n"), "{text}");
    assert_eq!(
        std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
        0o644
    );

    fx.destroy(&info.id).await;
    assert_eq!(read(&file), "DATABASE_URL=old\nOTHER=1\n");
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn env_line_removed_when_absent_before() {
    let Some(fx) = Fixture::new().await else {
        return;
    };
    let file = fx.dotenv("OTHER=1\n");
    let info = fx.clone(7200, &file).await;
    assert_eq!(
        get_var(&read(&file), "DATABASE_URL").as_deref(),
        Some(&*info.url)
    );
    fx.destroy(&info.id).await;
    assert_eq!(read(&file), "OTHER=1\n");
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn env_left_alone_when_user_changed_it() {
    let Some(fx) = Fixture::new().await else {
        return;
    };
    let file = fx.dotenv("DATABASE_URL=old\n");
    let info = fx.clone(7200, &file).await;
    std::fs::write(&file, "DATABASE_URL=mine\n").unwrap();
    fx.destroy(&info.id).await;
    assert_eq!(read(&file), "DATABASE_URL=mine\n");
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn env_restored_on_ttl_expiry() {
    let Some(fx) = Fixture::new().await else {
        return;
    };
    let file = fx.dotenv("DATABASE_URL=old\n");
    let info = fx.clone(1, &file).await;
    assert_ne!(read(&file), "DATABASE_URL=old\n");
    // The test's own Ctx shares the daemon's state.db and runs the same destroy path as the reaper.
    let reaped = reap_expired(&fx.env.ctx, now() + 5).await.unwrap();
    assert_eq!(reaped, vec![info.id]);
    assert_eq!(read(&file), "DATABASE_URL=old\n");
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unwritable_restore_becomes_pending_and_client_finishes_it() {
    let Some(fx) = Fixture::new().await else {
        return;
    };
    let file = fx.dotenv("DATABASE_URL=old\n");
    let info = fx.clone(1, &file).await;
    let written = read(&file);
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444)).unwrap();

    let reaped = reap_expired(&fx.env.ctx, now() + 5).await.unwrap();
    assert_eq!(reaped, vec![info.id]);
    assert_eq!(read(&file), written, "a read-only file is left alone");
    assert_eq!(fx.env.ctx.store().list_pending_restores().unwrap().len(), 1);

    // Still read-only: a warning, no password in it, and the item stays.
    let warnings = finish_pending(&fx.home).await;
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(!warnings[0].contains("postgres://"), "{}", warnings[0]);
    assert_eq!(fx.env.ctx.store().list_pending_restores().unwrap().len(), 1);

    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(finish_pending(&fx.home).await.is_empty());
    assert_eq!(read(&file), "DATABASE_URL=old\n");
    assert!(fx
        .env
        .ctx
        .store()
        .list_pending_restores()
        .unwrap()
        .is_empty());
    fx.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn git_warning_when_not_ignored() {
    if Command::new("git").arg("--version").output().is_err() {
        println!("skipping: no git");
        return;
    }
    let Some(fx) = Fixture::new().await else {
        return;
    };
    let dir = std::fs::canonicalize(fx.project.path()).unwrap();
    assert!(Command::new("git")
        .arg("-C")
        .arg(&dir)
        .args(["init", "-q"])
        .status()
        .unwrap()
        .success());
    let file = fx.dotenv("A=1\n");
    let a = fx.clone(7200, &file).await;
    assert!(
        a.warnings
            .iter()
            .any(|w| w.contains("is not ignored by git")),
        "{:?}",
        a.warnings
    );

    std::fs::write(dir.join(".gitignore"), ".env\n").unwrap();
    let b = fx.clone(7200, &file).await;
    assert!(
        !b.warnings.iter().any(|w| w.contains("ignored by git")),
        "{:?}",
        b.warnings
    );
    fx.stop().await;
}
