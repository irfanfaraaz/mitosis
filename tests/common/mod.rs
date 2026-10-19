#![allow(dead_code)] // each test binary uses a different subset

use mitosis::ctx::{Config, Ctx};
use mitosis::names::random_hex;
use mitosis::paths::Home;
use mitosis::pg::{discover, newest, stop};
use mitosis::protocol::{TemplateInfo, TemplateInitArgs, TemplateSource};
use mitosis::templates;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

pub struct TestEnv {
    pub ctx: Arc<Ctx>,
    pub tmp: TempDir,
}

/// A fresh home under /tmp: short enough for the ~103-byte Unix socket path limit, and on APFS.
pub fn tmp_home() -> TempDir {
    tempfile::Builder::new()
        .prefix("mt")
        .tempdir_in("/tmp")
        .expect("create temp home")
}

impl TestEnv {
    pub fn new() -> Option<TestEnv> {
        if discover().is_empty() {
            println!("skipping: no Postgres installed");
            return None;
        }
        let tmp = tmp_home();
        // One of 250 random 20-port slices of 55000-59999, so parallel test binaries rarely meet.
        let slice = u16::from_str_radix(&random_hex(2), 16).unwrap() % 250;
        let lo = 55_000 + slice * 20;
        let config = Config {
            port_range: lo..=lo + 19,
            ..Config::default()
        };
        let ctx = Ctx::open(Home::at(tmp.path().to_path_buf()), config).expect("open ctx");
        Some(TestEnv {
            ctx: Arc::new(ctx),
            tmp,
        })
    }
}

impl Drop for TestEnv {
    /// Stops any Postgres still running in this home so tests never leak servers.
    fn drop(&mut self) {
        let Ok(install) = newest(&self.ctx.installs) else {
            return;
        };
        for parent in [self.ctx.home.clones_dir(), self.ctx.home.templates_dir()] {
            let Ok(rd) = std::fs::read_dir(parent) else {
                continue;
            };
            for entry in rd.flatten() {
                let _ = stop(install, &entry.path(), Duration::from_secs(5));
            }
        }
    }
}

/// Runs `sql` with the newest install's `psql` against `url`; returns trimmed unaligned output.
pub fn psql_url(env: &TestEnv, url: &str, sql: &str) -> String {
    let install = newest(&env.ctx.installs).expect("postgres installed");
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
    }
}

/// Builds template `name` from a seed running `sql`; panics on failure.
pub async fn template(env: &TestEnv, name: &str, sql: &str) -> TemplateInfo {
    templates::init(&env.ctx, template_args(name, seed(sql)))
        .await
        .expect("build template")
}
