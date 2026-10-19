//! `mitosis doctor`: read-only health checks that work with the daemon down.
//!
//! The store is opened read-only and never migrated; nothing here deletes or starts anything
//! except `--fix` (finish pending `.env` restores, then the usual gc) and the throwaway Postgres
//! per installed major, which lives under `<home>/logs/doctor-*` and is always cleaned up.
use crate::client;
use crate::clone_backend::{free_space, same_volume, ApfsBackend, CloneBackend};
use crate::config::Config;
use crate::envfile;
use crate::names::is_clone_id;
use crate::paths::Home;
use crate::pg::cluster::{check_socket_path, log_tail};
use crate::pg::{check_extensions, discover, find_major, initdb, start, stop, wait_ready};
use crate::pg::{PgInstall, StartOptions};
use crate::protocol::{MigrateCmd, Request, Response, TemplateSource};
use crate::service;
use crate::state::SCHEMA_VERSION;
use crate::templates::{
    age_secs, hash_dir, is_build_socket_name, now, parse_gen_dir, parse_leftover,
};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use std::collections::HashSet;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Ok,
    Warn,
    Fail,
}

#[derive(Serialize, Clone, Debug)]
pub struct Check {
    pub name: &'static str,
    pub level: Level,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
}

impl Check {
    fn new(name: &'static str, level: Level, detail: impl Into<String>) -> Check {
        Check {
            name,
            level,
            detail: detail.into(),
            fix: None,
        }
    }

    fn fix(mut self, fix: impl Into<String>) -> Check {
        self.fix = Some(fix.into());
        self
    }
}

/// 1 when any check failed, else 0.
pub fn exit_code(checks: &[Check]) -> u8 {
    u8::from(checks.iter().any(|c| c.level == Level::Fail))
}

/// One line per check (continuation lines of a detail are indented), then its fix.
pub fn render(checks: &[Check]) -> String {
    let mut out = String::new();
    for c in checks {
        let tag = match c.level {
            Level::Ok => "ok  ",
            Level::Warn => "warn",
            Level::Fail => "FAIL",
        };
        let detail = c.detail.replace('\n', "\n          ");
        out.push_str(&format!("{tag}  {:<18} {detail}\n", c.name));
        if let Some(f) = &c.fix {
            out.push_str(&format!("      {:<18} fix: {f}\n", ""));
        }
    }
    out
}

/// Worst level of `findings`, their text joined; `ok` when there are none.
fn summarize(name: &'static str, findings: Vec<(Level, String)>, ok: &str) -> Check {
    let level = findings.iter().map(|f| f.0).max().unwrap_or(Level::Ok);
    if findings.is_empty() {
        return Check::new(name, Level::Ok, ok);
    }
    let detail = findings
        .into_iter()
        .map(|f| f.1)
        .collect::<Vec<_>>()
        .join("\n");
    Check::new(name, level, detail)
}

// ---- read-only view of state.db -------------------------------------------------------------

struct TplRow {
    name: String,
    source: Option<TemplateSource>,
    migrations: Option<PathBuf>,
    migrate: Option<MigrateCmd>,
    max_age_secs: u64,
    current_gen: u32,
    paused: bool,
    last_error: Option<String>,
}

struct GenRow {
    template: String,
    gen: u32,
    dir: PathBuf,
    pg_major: u32,
    extensions: Vec<String>,
    migrations_hash: Option<String>,
    built_at: i64,
}

struct CloneRow {
    id: String,
    name: Option<String>,
    dir: PathBuf,
    suspect: bool,
}

struct EnvRef {
    /// `None` for a pending restore.
    clone_id: Option<String>,
    file: PathBuf,
}

#[derive(Default)]
struct Snapshot {
    templates: Vec<TplRow>,
    gens: Vec<GenRow>,
    clones: Vec<CloneRow>,
    pending: usize,
    env_files: Vec<EnvRef>,
}

enum StoreView {
    Missing,
    /// A v0.1 store (or an empty file): the daemon upgrades it on its next start.
    Old,
    Broken(String),
    Ready(Snapshot),
}

fn rows<T>(
    conn: &Connection,
    sql: &str,
    f: impl FnMut(&rusqlite::Row) -> rusqlite::Result<T>,
) -> rusqlite::Result<Vec<T>> {
    conn.prepare(sql)?.query_map([], f)?.collect()
}

fn json<T: serde::de::DeserializeOwned>(s: Option<String>) -> Option<T> {
    serde_json::from_str(&s?).ok()
}

fn read_store(home: &Home) -> StoreView {
    let path = home.state_db();
    if !path.is_file() {
        return StoreView::Missing;
    }
    match read_snapshot(&path) {
        Ok(v) => v,
        Err(e) => StoreView::Broken(e.to_string()),
    }
}

fn read_snapshot(path: &Path) -> rusqlite::Result<StoreView> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let version: i32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version > SCHEMA_VERSION {
        return Ok(StoreView::Broken(format!(
            "state.db has schema version {version}; this mitosis knows up to {SCHEMA_VERSION}"
        )));
    }
    if version < SCHEMA_VERSION {
        return Ok(StoreView::Old);
    }
    let templates = rows(
        &conn,
        "SELECT name, source, migrations, migrate, max_age_secs, current_gen, paused, last_error \
         FROM templates ORDER BY name",
        |r| {
            Ok(TplRow {
                name: r.get(0)?,
                source: json(r.get(1)?),
                migrations: r.get::<_, Option<String>>(2)?.map(PathBuf::from),
                migrate: json(r.get(3)?),
                max_age_secs: r.get::<_, i64>(4)?.max(0) as u64,
                current_gen: r.get(5)?,
                paused: r.get(6)?,
                last_error: r.get(7)?,
            })
        },
    )?;
    let gens = rows(
        &conn,
        "SELECT template, gen, dir, pg_major, extensions, migrations_hash, built_at FROM generations",
        |r| {
            Ok(GenRow {
                template: r.get(0)?,
                gen: r.get(1)?,
                dir: PathBuf::from(r.get::<_, String>(2)?),
                pg_major: r.get(3)?,
                extensions: json(r.get(4)?).unwrap_or_default(),
                migrations_hash: r.get(5)?,
                built_at: r.get(6)?,
            })
        },
    )?;
    let clones = rows(&conn, "SELECT id, name, dir, suspect FROM clones", |r| {
        Ok(CloneRow {
            id: r.get(0)?,
            name: r.get(1)?,
            dir: PathBuf::from(r.get::<_, String>(2)?),
            suspect: r.get(3)?,
        })
    })?;
    // Older v2 stores may lack these tables until a daemon opens them: read as empty.
    let pending = rows(&conn, "SELECT file FROM pending_restores", |r| {
        Ok(EnvRef {
            clone_id: None,
            file: PathBuf::from(r.get::<_, String>(0)?),
        })
    })
    .unwrap_or_default();
    let records = rows(&conn, "SELECT clone_id, file FROM env_records", |r| {
        Ok(EnvRef {
            clone_id: Some(r.get(0)?),
            file: PathBuf::from(r.get::<_, String>(1)?),
        })
    })
    .unwrap_or_default();
    let pending_n = pending.len();
    let mut env_files = pending;
    env_files.extend(records);
    Ok(StoreView::Ready(Snapshot {
        templates,
        gens,
        clones,
        pending: pending_n,
        env_files,
    }))
}

impl Snapshot {
    fn current(&self, t: &TplRow) -> Option<&GenRow> {
        self.gens
            .iter()
            .find(|g| g.template == t.name && g.gen == t.current_gen)
    }
}

// ---- gc dry run ------------------------------------------------------------------------------

fn entries(dir: &Path) -> Vec<(String, PathBuf)> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    rd.flatten()
        .map(|e| (e.file_name().to_string_lossy().into_owned(), e.path()))
        .collect()
}

/// Folders gc would remove: the same ownership rules as `lifecycle::remove_orphans`, by name
/// only, and nothing is touched. Entries Mitosis did not create are not listed.
fn orphan_folders(home: &Home, snap: &Snapshot) -> Vec<PathBuf> {
    let ids: HashSet<&str> = snap.clones.iter().map(|c| c.id.as_str()).collect();
    let gen_dirs: HashSet<String> = snap
        .gens
        .iter()
        .filter_map(|g| g.dir.file_name().map(|f| f.to_string_lossy().into_owned()))
        .collect();
    let mut out = Vec::new();
    for (id, path) in entries(&home.clones_dir()) {
        if is_clone_id(&id) && !ids.contains(id.as_str()) {
            out.push(path);
        }
    }
    for (name, path) in entries(&home.sockets_dir()) {
        if is_build_socket_name(&name) || (is_clone_id(&name) && !ids.contains(name.as_str())) {
            out.push(path);
        }
    }
    for (file, path) in entries(&home.templates_dir()) {
        if !gen_dirs.contains(&file)
            && (parse_leftover(&file).is_some() || parse_gen_dir(&file).is_some())
        {
            out.push(path);
        }
    }
    out.sort();
    out
}

/// Rows whose folder is gone.
fn stale_rows(snap: &Snapshot) -> Vec<String> {
    let mut out = Vec::new();
    for c in &snap.clones {
        if !c.dir.exists() {
            out.push(format!(
                "clone {} has no folder {}",
                c.name.as_deref().unwrap_or(&c.id),
                c.dir.display()
            ));
        }
    }
    for g in &snap.gens {
        if !g.dir.exists() {
            out.push(format!(
                "template {} generation {} has no folder {}",
                g.template,
                g.gen,
                g.dir.display()
            ));
        }
    }
    out
}

// ---- throwaway Postgres ----------------------------------------------------------------------

async fn smoke_inner(
    install: &PgInstall,
    dir: &Path,
    port: u16,
    log: &Path,
) -> std::result::Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let data = dir.join("data");
    let (i, d) = (install.clone(), data.clone());
    tokio::task::spawn_blocking(move || initdb(&i, &d))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.message)?;
    let opts = StartOptions {
        port,
        socket_dir: dir.join("s"),
        listen_tcp: false,
        durable: false,
        log_file: log.to_path_buf(),
    };
    let (i, d, o) = (install.clone(), data, opts.clone());
    let pg = tokio::task::spawn_blocking(move || start(&i, &d, &o))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.message)?;
    wait_ready(install, &pg, &opts, Duration::from_secs(30))
        .await
        .map_err(|e| e.message)
}

/// initdb, start and stop one Postgres of `install` in a temp folder, and clean up either way.
/// The server listens on its socket only, so no port is ever taken.
async fn smoke(home: &Home, install: &PgInstall, port: u16) -> std::result::Result<(), String> {
    let dir = home
        .logs_dir()
        .join(format!("doctor-{}-{}", install.major, std::process::id()));
    let log = dir.join("postgres.log");
    let res = smoke_inner(install, &dir, port, &log).await;
    let tail = if res.is_err() {
        log_tail(&log, 10)
    } else {
        String::new()
    };
    let (i, d) = (install.clone(), dir.join("data"));
    let stopped =
        tokio::task::spawn_blocking(move || stop(&i, &d, None, Duration::from_secs(10))).await;
    let stop_err = match stopped {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e.message),
        Err(e) => Some(e.to_string()),
    };
    // A server we could not stop keeps its folder: deleting it under a live postmaster helps nobody.
    if let Some(e) = stop_err {
        return Err(format!(
            "could not stop the throwaway server ({e}); left {} in place",
            dir.display()
        ));
    }
    let _ = std::fs::remove_dir_all(&dir);
    res.map_err(|e| {
        if tail.is_empty() {
            e
        } else {
            format!("{e}\n{tail}")
        }
    })
}

// ---- the checks ------------------------------------------------------------------------------

fn mode(path: &Path) -> Option<u32> {
    std::fs::metadata(path)
        .ok()
        .map(|m| m.permissions().mode() & 0o777)
}

fn check_fs(home: &Home) -> Check {
    if !home.root.is_dir() {
        return Check::new(
            "home",
            Level::Fail,
            format!("{} does not exist", home.root.display()),
        )
        .fix("run any mitosis command to create it, or set MITOSIS_HOME");
    }
    if let Err(e) = ApfsBackend.check_supported(&home.root) {
        return Check::new("home", Level::Fail, e.message)
            .fix(e.hint.unwrap_or_else(|| "use an APFS volume".into()));
    }
    let (t, c) = (home.templates_dir(), home.clones_dir());
    if t.is_dir() && c.is_dir() {
        match same_volume(&t, &c) {
            Ok(true) => {}
            Ok(false) => {
                return Check::new(
                    "home",
                    Level::Fail,
                    "templates/ and clones/ are on different volumes",
                )
                .fix("keep the whole home on one APFS volume");
            }
            Err(e) => return Check::new("home", Level::Fail, e.message),
        }
    }
    Check::new(
        "home",
        Level::Ok,
        format!("{} on APFS, one volume", home.root.display()),
    )
}

fn check_permissions(home: &Home) -> Check {
    if !home.root.is_dir() {
        return Check::new("permissions", Level::Ok, "skipped: no home folder");
    }
    let mut f = Vec::new();
    if !home.marker().exists() {
        f.push((
            Level::Warn,
            "no .mitosis-home marker: gc will not touch this folder".to_string(),
        ));
    }
    for (path, want, level) in [
        (home.root.clone(), 0o700, Level::Fail),
        (home.state_db(), 0o600, Level::Fail),
        (home.daemon_sock(), 0o600, Level::Fail),
    ] {
        if let Some(m) = mode(&path) {
            if m != want {
                f.push((
                    level,
                    format!("{} is {m:04o}, want {want:04o}", path.display()),
                ));
            }
        }
    }
    let fix = format!("chmod 700 \"{}\"", home.root.display());
    let c = summarize("permissions", f, "marker present, modes 0700 / 0600");
    if c.level == Level::Ok {
        c
    } else {
        c.fix(fix)
    }
}

fn check_socket_len(home: &Home, cfg: &Config) -> Check {
    let port = *cfg.port_range.end();
    let sample = home.sockets_dir().join("c000000");
    match check_socket_path(&sample, port) {
        Ok(()) => Check::new(
            "socket_path",
            Level::Ok,
            "clone socket paths fit the 103-byte limit",
        ),
        Err(e) => Check::new("socket_path", Level::Fail, e.message).fix(
            e.hint
                .unwrap_or_else(|| "set MITOSIS_HOME to a shorter path".into()),
        ),
    }
}

async fn check_daemon(home: &Home) -> (Check, bool) {
    let ask = client::request_if_running(home, Request::Status);
    match tokio::time::timeout(Duration::from_secs(5), ask).await {
        Err(_) => (
            Check::new("daemon", Level::Warn, "did not answer within 5 s")
                .fix("restart it: mitosis daemon stop"),
            true,
        ),
        Ok(Ok(None)) => (
            Check::new("daemon", Level::Ok, "not running (starts on demand)"),
            false,
        ),
        Ok(Ok(Some(Response::Status(s)))) => {
            let mine = env!("CARGO_PKG_VERSION");
            if s.version == mine {
                (
                    Check::new("daemon", Level::Ok, format!("running, version {mine}")),
                    true,
                )
            } else {
                (
                    Check::new(
                        "daemon",
                        Level::Warn,
                        format!("running version {}, this binary is {mine}", s.version),
                    )
                    .fix("mitosis daemon stop (the next command starts the new one)"),
                    true,
                )
            }
        }
        Ok(Ok(Some(_))) => (
            Check::new(
                "daemon",
                Level::Warn,
                "unexpected reply to a status request",
            ),
            true,
        ),
        Ok(Err(e)) => {
            let c = Check::new("daemon", Level::Warn, e.message);
            (e.hint.map_or(c.clone(), |h| c.fix(h)), true)
        }
    }
}

async fn service_status() -> Option<service::ServiceStatus> {
    tokio::task::spawn_blocking(service::status)
        .await
        .ok()
        .and_then(|r| r.ok())
}

fn check_service(st: &Option<service::ServiceStatus>) -> Check {
    match st {
        None => Check::new("service", Level::Warn, "could not ask launchd"),
        Some(s) if !s.installed => Check::new("service", Level::Ok, "not installed (optional)"),
        Some(s) if !s.binary_exists => {
            Check::new("service", Level::Warn, "its binary no longer exists")
                .fix("mitosis service install")
        }
        Some(s) if !s.loaded => Check::new("service", Level::Warn, "installed but not loaded")
            .fix("mitosis service install"),
        Some(_) => Check::new("service", Level::Ok, "installed and loaded"),
    }
}

fn check_postgres(installs: &[PgInstall]) -> Check {
    match installs.first() {
        None => Check::new("postgres", Level::Fail, "no Postgres found").fix(format!(
            "brew install postgresql@{}",
            crate::pg::binaries::LATEST_PG_MAJOR
        )),
        Some(_) => Check::new(
            "postgres",
            Level::Ok,
            installs
                .iter()
                .map(|i| format!("{} ({})", i.version, i.bin_dir.display()))
                .collect::<Vec<_>>()
                .join(", "),
        ),
    }
}

fn check_template_installs(snap: &Snapshot, installs: &[PgInstall]) -> Check {
    let mut f = Vec::new();
    let mut hint = None;
    for t in &snap.templates {
        let Some(g) = snap.current(t) else { continue };
        match find_major(installs, g.pg_major) {
            Err(e) => {
                f.push((
                    Level::Fail,
                    format!(
                        "template {} needs Postgres {}: {}",
                        t.name, g.pg_major, e.message
                    ),
                ));
                hint = e.hint;
            }
            Ok(i) => {
                if let Err(e) = check_extensions(i, &g.extensions) {
                    f.push((Level::Fail, format!("template {}: {}", t.name, e.message)));
                    hint = hint.or(e.hint);
                }
            }
        }
    }
    let c = summarize(
        "template_installs",
        f,
        "every template's Postgres major and extensions are installed",
    );
    match hint {
        Some(h) if c.level != Level::Ok => c.fix(h),
        _ => c,
    }
}

async fn check_smoke(home: &Home, cfg: &Config, installs: &[PgInstall]) -> Check {
    if !home.logs_dir().is_dir() {
        return Check::new("postgres_smoke", Level::Ok, "skipped: no home folder");
    }
    let port = *cfg.port_range.start();
    let mut f = Vec::new();
    let mut ok = Vec::new();
    for i in installs {
        match smoke(home, i, port).await {
            Ok(()) => ok.push(i.major.to_string()),
            Err(e) => f.push((Level::Fail, format!("Postgres {}: {e}", i.major))),
        }
    }
    summarize(
        "postgres_smoke",
        f,
        &if ok.is_empty() {
            "nothing to test".to_string()
        } else {
            format!(
                "initdb, start and stop worked for Postgres {}",
                ok.join(", ")
            )
        },
    )
}

fn check_disk(home: &Home, cfg: &Config) -> Check {
    let probe = if home.root.is_dir() {
        &home.root
    } else {
        return Check::new("disk", Level::Ok, "skipped: no home folder");
    };
    match free_space(probe) {
        Err(e) => Check::new("disk", Level::Warn, e.message),
        Ok(free) if free < cfg.min_free_bytes => Check::new(
            "disk",
            Level::Fail,
            format!(
                "{free} bytes free, below min_free_disk {}",
                cfg.min_free_bytes
            ),
        )
        .fix("free disk space, or lower it: mitosis config set min_free_disk 1GB"),
        Ok(free) => Check::new("disk", Level::Ok, format!("{free} bytes free")),
    }
}

fn check_templates(snap: &Snapshot) -> Check {
    let mut f = Vec::new();
    for t in &snap.templates {
        let Some(g) = snap.current(t) else { continue };
        if age_secs(g.built_at, now()) > t.max_age_secs {
            f.push((
                Level::Warn,
                format!(
                    "template {} is stale; run: mitosis template refresh {}",
                    t.name, t.name
                ),
            ));
        }
        if let Some(dir) = t.migrations.as_deref().filter(|d| d.is_dir()) {
            if hash_dir(dir).ok() != g.migrations_hash {
                f.push((
                    Level::Warn,
                    format!(
                        "template {}: migrations in {} changed since it was built",
                        t.name,
                        dir.display()
                    ),
                ));
            }
        }
        if let Some(e) = &t.last_error {
            f.push((
                Level::Warn,
                format!("template {}: last build failed: {e}", t.name),
            ));
        }
        if t.paused {
            f.push((Level::Warn, format!("template {} is paused", t.name)));
        }
    }
    summarize("templates", f, "fresh, no errors, none paused")
}

fn check_suspect(snap: &Snapshot) -> Check {
    let f = snap
        .clones
        .iter()
        .filter(|c| c.suspect)
        .map(|c| {
            let n = c.name.as_deref().unwrap_or(&c.id);
            (
                Level::Warn,
                format!("clone {n} restarted after an unclean shutdown with fsync off"),
            )
        })
        .collect();
    summarize("suspect_clones", f, "none")
}

fn check_gc(home: &Home, snap: &Snapshot, daemon_up: bool) -> Check {
    let orphans = orphan_folders(home, snap);
    let mut f: Vec<(Level, String)> = orphans
        .iter()
        .map(|p| (Level::Warn, format!("orphan folder {}", p.display())))
        .collect();
    f.extend(stale_rows(snap).into_iter().map(|s| (Level::Warn, s)));
    if daemon_up && !orphans.is_empty() {
        f.push((
            Level::Warn,
            "a build or create in progress owns some of these until it finishes".into(),
        ));
    }
    let c = summarize("gc_dry_run", f, "nothing for gc to clean");
    if c.level == Level::Ok {
        c
    } else {
        c.fix("mitosis doctor --fix (or mitosis gc)")
    }
}

fn check_env(snap: &Snapshot) -> Check {
    let ids: HashSet<&str> = snap.clones.iter().map(|c| c.id.as_str()).collect();
    let mut f = Vec::new();
    if snap.pending > 0 {
        f.push((
            Level::Warn,
            format!("{} pending .env restore(s) not finished", snap.pending),
        ));
    }
    for r in &snap.env_files {
        if let Some(id) = &r.clone_id {
            if !ids.contains(id.as_str()) {
                f.push((
                    Level::Warn,
                    format!("env record for gone clone {id} in {}", r.file.display()),
                ));
            }
        }
    }
    let c = summarize("env_records", f, "no pending restores, no orphaned records");
    if c.level == Level::Ok {
        c
    } else {
        c.fix("mitosis doctor --fix")
    }
}

/// Folders macOS guards with privacy prompts a launchd service cannot answer.
fn guarded(path: &Path, home_dir: &Path) -> bool {
    ["Documents", "Desktop", "Library/Mobile Documents"]
        .iter()
        .any(|d| path.starts_with(home_dir.join(d)))
}

fn check_privacy(snap: &Snapshot, svc: &Option<service::ServiceStatus>) -> Check {
    let name = "macos_privacy";
    if !svc.as_ref().is_some_and(|s| s.installed) {
        return Check::new(name, Level::Ok, "no login service installed");
    }
    let Some(home_dir) = std::env::var_os("HOME").map(PathBuf::from) else {
        return Check::new(name, Level::Ok, "HOME is not set; skipped");
    };
    let mut f = Vec::new();
    let mut flag = |what: &str, owner: &str, p: &Path| {
        if guarded(p, &home_dir) {
            f.push((
                Level::Warn,
                format!(
                    "{what} of {owner} is under a macOS-protected folder: {}",
                    p.display()
                ),
            ));
        }
    };
    for t in &snap.templates {
        if let Some(TemplateSource::Seed { cwd: Some(c), .. }) = &t.source {
            flag("seed folder", &t.name, c);
        }
        if let Some(m) = &t.migrations {
            flag("migrations folder", &t.name, m);
        }
        if let Some(m) = &t.migrate {
            flag("migrate folder", &t.name, &m.cwd);
        }
    }
    for r in &snap.env_files {
        flag("env file", "a clone", &r.file);
    }
    let c = summarize(name, f, "nothing under Documents, Desktop or iCloud Drive");
    if c.level == Level::Ok {
        c
    } else {
        c.fix("move it out of Documents/Desktop/iCloud, or grant the binary Full Disk Access (see the README on macOS privacy)")
    }
}

/// `--fix`: finish pending `.env` restores, then gc when the dry run found something. This is the
/// only place that may start the daemon.
async fn apply_fix(home: &Home) -> Check {
    let StoreView::Ready(snap) = read_store(home) else {
        return Check::new("fix", Level::Ok, "nothing to fix: no current-schema store");
    };
    let mut f = Vec::new();
    if snap.pending > 0 {
        let warnings = envfile::finish_pending(home).await;
        let done = snap.pending.saturating_sub(warnings.len());
        f.push((
            Level::Ok,
            format!(
                "finished {done} of {} pending .env restore(s)",
                snap.pending
            ),
        ));
        f.extend(warnings.into_iter().map(|w| (Level::Warn, w)));
    }
    if !orphan_folders(home, &snap).is_empty() || !stale_rows(&snap).is_empty() {
        match client::request(home, Request::Gc).await {
            Ok(Response::Gc(r)) => {
                f.push((
                    Level::Ok,
                    format!(
                        "gc: destroyed {}, orphan folders removed {}, records dropped {}",
                        r.destroyed.len(),
                        r.orphan_folders_removed.len(),
                        r.records_dropped.len()
                    ),
                ));
                f.extend(
                    r.failed
                        .into_iter()
                        .map(|x| (Level::Warn, format!("gc failed: {}: {}", x.item, x.error))),
                );
            }
            Ok(_) => f.push((Level::Warn, "gc: unexpected daemon reply".into())),
            Err(e) => f.push((Level::Warn, format!("gc: {}", e.message))),
        }
    }
    summarize("fix", f, "nothing to fix")
}

pub async fn run(home: &Home, fix: bool) -> Vec<Check> {
    let mut out = Vec::new();
    if fix {
        out.push(apply_fix(home).await);
    }
    out.push(check_fs(home));
    out.push(check_permissions(home));

    let cfg = match Config::load(home) {
        Ok(c) => {
            out.push(Check::new(
                "config",
                Level::Ok,
                "config.toml and environment are valid",
            ));
            c
        }
        Err(e) => {
            let c = Check::new("config", Level::Fail, e.message);
            out.push(e.hint.map_or(c.clone(), |h| c.fix(h)));
            Config::default()
        }
    };
    out.push(check_socket_len(home, &cfg));

    let (daemon, daemon_up) = check_daemon(home).await;
    out.push(daemon);
    let svc = service_status().await;
    out.push(check_service(&svc));

    let installs = tokio::task::spawn_blocking(discover)
        .await
        .unwrap_or_default();
    out.push(check_postgres(&installs));

    let view = read_store(home);
    match &view {
        StoreView::Missing => out.push(Check::new("store", Level::Ok, "no state.db yet")),
        StoreView::Old => out.push(Check::new(
            "store",
            Level::Warn,
            "state.db is from an older version: upgraded on the next daemon start; store checks skipped",
        )),
        StoreView::Broken(e) => out.push(Check::new("store", Level::Fail, format!("cannot read state.db: {e}"))),
        StoreView::Ready(snap) => {
            out.push(Check::new(
                "store",
                Level::Ok,
                format!("{} template(s), {} clone(s)", snap.templates.len(), snap.clones.len()),
            ));
            out.push(check_template_installs(snap, &installs));
        }
    }
    out.push(check_smoke(home, &cfg, &installs).await);
    out.push(check_disk(home, &cfg));
    if let StoreView::Ready(snap) = &view {
        out.push(check_templates(snap));
        out.push(check_suspect(snap));
        out.push(check_gc(home, snap, daemon_up));
        out.push(check_env(snap));
        out.push(check_privacy(snap, &svc));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(level: Level) -> Check {
        Check::new("x", level, "d")
    }

    #[test]
    fn exit_code_is_one_on_fail() {
        assert_eq!(exit_code(&[c(Level::Ok), c(Level::Warn)]), 0);
        assert_eq!(exit_code(&[c(Level::Ok), c(Level::Fail)]), 1);
        assert_eq!(exit_code(&[]), 0);
    }

    #[test]
    fn json_shape() {
        let v = serde_json::to_value([
            Check::new("home", Level::Ok, "fine"),
            Check::new("disk", Level::Fail, "low").fix("free space"),
        ])
        .unwrap();
        assert_eq!(v[0]["name"], "home");
        assert_eq!(v[0]["level"], "ok");
        assert_eq!(v[0]["detail"], "fine");
        assert!(v[0].get("fix").is_none());
        assert_eq!(v[1]["level"], "fail");
        assert_eq!(v[1]["fix"], "free space");
    }

    #[test]
    fn summarize_takes_the_worst_level() {
        let s = summarize(
            "n",
            vec![(Level::Warn, "a".into()), (Level::Fail, "b".into())],
            "ok",
        );
        assert_eq!((s.level, s.detail.as_str()), (Level::Fail, "a\nb"));
        assert_eq!(summarize("n", vec![], "fine").detail, "fine");
    }

    #[test]
    fn guarded_folders() {
        let h = Path::new("/Users/u");
        assert!(guarded(Path::new("/Users/u/Documents/app/.env"), h));
        assert!(guarded(Path::new("/Users/u/Library/Mobile Documents/x"), h));
        assert!(!guarded(Path::new("/Users/u/Developer/app"), h));
        assert!(!guarded(Path::new("/Users/u/DocumentsX"), h));
    }

    #[test]
    fn orphans_follow_gc_rules_and_delete_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let home = Home::at(tmp.path().to_path_buf());
        home.ensure_dirs().unwrap();
        for p in [
            home.clones_dir().join("c0000aa"),
            home.clones_dir().join("notmine"),
            home.sockets_dir().join("build-54321"),
            home.templates_dir().join("main@2"),
            home.templates_dir().join("main@1"),
            home.templates_dir().join(".main.building-5"),
            home.templates_dir().join("random"),
        ] {
            std::fs::create_dir_all(&p).unwrap();
        }
        let snap = Snapshot {
            gens: vec![GenRow {
                template: "main".into(),
                gen: 1,
                dir: home.templates_dir().join("main@1"),
                pg_major: 18,
                extensions: vec![],
                migrations_hash: None,
                built_at: 0,
            }],
            ..Snapshot::default()
        };
        let got: Vec<String> = orphan_folders(&home, &snap)
            .iter()
            .map(|p| p.strip_prefix(tmp.path()).unwrap().display().to_string())
            .collect();
        assert_eq!(
            got,
            [
                "clones/c0000aa",
                "sockets/build-54321",
                "templates/.main.building-5",
                "templates/main@2"
            ]
        );
        assert!(home.clones_dir().join("c0000aa").exists());
    }
}
