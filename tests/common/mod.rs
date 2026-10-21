#![allow(dead_code)] // each test binary uses a different subset

use mitosis::clone_backend::{select_backend, CloneBackend};
use mitosis::ctx::{Config, Ctx};
use mitosis::error::Result;
use mitosis::names::random_hex;
use mitosis::paths::Home;
use mitosis::pg::{discover, newest, stop};
use mitosis::protocol::{TemplateInfo, TemplateInitArgs, TemplateSource};
use mitosis::templates;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

pub struct TestEnv {
    pub ctx: Arc<Ctx>,
    pub tmp: TempDir,
}

/// A fresh home under /tmp (or `MITOSIS_TEST_ROOT`, the mounted test file system on Linux): short
/// enough for the ~103-byte Unix socket path limit, and on APFS on macOS.
pub fn tmp_home() -> TempDir {
    let root = std::env::var("MITOSIS_TEST_ROOT").unwrap_or_else(|_| "/tmp".into());
    tempfile::Builder::new()
        .prefix("mt")
        .tempdir_in(root)
        .expect("create temp home")
}

/// The selected back end, wrapped: every call is recorded (method and first path) and passed on.
pub struct RecordingBackend {
    inner: Arc<dyn CloneBackend>,
    calls: Mutex<Vec<(&'static str, PathBuf)>>,
}

impl RecordingBackend {
    fn hit(&self, method: &'static str, path: &Path) {
        self.calls.lock().unwrap().push((method, path.to_path_buf()));
    }

    /// The first path of every recorded call of `method`, in call order.
    pub fn calls(&self, method: &str) -> Vec<PathBuf> {
        let calls = self.calls.lock().unwrap();
        calls
            .iter()
            .filter(|(m, _)| *m == method)
            .map(|(_, p)| p.clone())
            .collect()
    }
}

impl CloneBackend for RecordingBackend {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn check_supported(&self, path: &Path) -> Result<()> {
        self.hit("check_supported", path);
        self.inner.check_supported(path)
    }

    fn clone_dir(&self, src: &Path, dst: &Path) -> Result<()> {
        self.hit("clone_dir", src);
        self.inner.clone_dir(src, dst)
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        self.hit("create_dir", path);
        self.inner.create_dir(path)
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        self.hit("remove_dir", path);
        self.inner.remove_dir(path)
    }

    fn rename_dir(&self, from: &Path, to: &Path) -> Result<()> {
        self.hit("rename_dir", from);
        self.inner.rename_dir(from, to)
    }

    fn private_size(&self, path: &Path) -> Option<u64> {
        self.hit("private_size", path);
        self.inner.private_size(path)
    }
}

impl TestEnv {
    pub fn new() -> Option<TestEnv> {
        Self::open(|backend| backend)
    }

    /// Like `new`, with the back end wrapped in a `RecordingBackend`.
    pub fn recording() -> Option<(TestEnv, Arc<RecordingBackend>)> {
        let mut rec = None;
        let env = Self::open(|inner| {
            let r = Arc::new(RecordingBackend {
                inner,
                calls: Mutex::new(Vec::new()),
            });
            rec = Some(r.clone());
            r as Arc<dyn CloneBackend>
        })?;
        Some((env, rec.expect("recording back end")))
    }

    fn open(wrap: impl FnOnce(Arc<dyn CloneBackend>) -> Arc<dyn CloneBackend>) -> Option<TestEnv> {
        if discover().is_empty() {
            println!("skipping: no Postgres installed");
            return None;
        }
        let tmp = tmp_home();
        // One of 250 20-port slices of 55000-59999: a random start per binary, then the next slice
        // per test, so parallel tests in one binary never share a slice.
        static NEXT: std::sync::OnceLock<std::sync::atomic::AtomicU16> = std::sync::OnceLock::new();
        let next = NEXT.get_or_init(|| {
            std::sync::atomic::AtomicU16::new(u16::from_str_radix(&random_hex(2), 16).unwrap())
        });
        let slice = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 250;
        let lo = 55_000 + slice * 20;
        // No router runs for an in-process ctx, so `url` is the direct URL; tests that start a
        // daemon with the router on get this slice's own router port.
        let config = Config {
            port_range: lo..=lo + 19,
            router: false,
            router_port: 60_000 + slice,
            ..Config::default()
        };
        let home = Home::at(tmp.path().to_path_buf());
        let backend = select_backend(&home, &config).expect("select back end");
        let ctx = Ctx::open_with_backend(home, config, wrap(backend)).expect("open ctx");
        Some(TestEnv {
            ctx: Arc::new(ctx),
            tmp,
        })
    }
}

impl Drop for TestEnv {
    /// Stops any Postgres still running in this home so tests never leak servers.
    fn drop(&mut self) {
        let installs = self.ctx.installs();
        let Ok(install) = newest(&installs) else {
            return;
        };
        for parent in [self.ctx.home.clones_dir(), self.ctx.home.templates_dir()] {
            let Ok(rd) = std::fs::read_dir(parent) else {
                continue;
            };
            for entry in rd.flatten() {
                let _ = stop(install, &entry.path(), None, Duration::from_secs(5));
            }
        }
    }
}

/// Runs `sql` with the newest install's `psql` against `url`; returns trimmed unaligned output.
pub fn psql_url(env: &TestEnv, url: &str, sql: &str) -> String {
    let installs = env.ctx.installs();
    let install = newest(&installs).expect("postgres installed");
    let out = Command::new(install.bin("psql"))
        .arg(url)
        .args(["-v", "ON_ERROR_STOP=1", "-X", "-Atc", sql])
        .env("PGCONNECT_TIMEOUT", "5")
        .output()
        .expect("run psql");
    assert!(
        out.status.success(),
        "psql failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// A seed source that runs `sql` with the full path of the newest `psql`.
pub fn seed(sql: &str) -> TemplateSource {
    let installs = discover();
    let install = newest(&installs).expect("postgres installed");
    let psql = install.bin("psql");
    TemplateSource::Seed {
        command: format!(
            "{} \"$DATABASE_URL\" -v ON_ERROR_STOP=1 -X -c {}",
            sh_quote(&psql.to_string_lossy()),
            sh_quote(sql)
        ),
        cwd: None,
        path: None,
    }
}

pub fn template_args(name: &str, source: TemplateSource) -> TemplateInitArgs {
    TemplateInitArgs {
        name: name.into(),
        source,
        database: None,
        migrations: None,
        max_age_secs: None,
        pg_major: None,
        settings: Default::default(),
    }
}

/// Builds template `name` from a seed running `sql`; panics on failure.
pub async fn template(env: &TestEnv, name: &str, sql: &str) -> TemplateInfo {
    templates::init(&env.ctx, template_args(name, seed(sql)))
        .await
        .expect("build template")
}

/// Turns the router on for this env's in-process ctx (on its own slice port) and waits until it
/// listens. Needs a multi-thread runtime: blocking `psql` calls must not starve the router.
pub async fn start_router(env: &TestEnv) {
    env.ctx.set_config(Config {
        router: true,
        ..env.ctx.config()
    });
    let listener = mitosis::router::bind(&env.ctx).await;
    mitosis::router::spawn(env.ctx.clone(), listener);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !env.ctx.router().status.listening {
        assert!(
            std::time::Instant::now() < deadline,
            "router never listened: {:?}",
            env.ctx.router().status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
