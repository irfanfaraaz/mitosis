//! `mitosis doctor`: read-only health checks that work with the daemon down.
//!
//! The store is opened read-only and never migrated; nothing here deletes or starts anything
//! except `--fix` (finish pending `.env` restores, then gc unless it would also destroy clones) and
//! the throwaway Postgres
//! per installed major, which lives under `<home>/logs/doctor-*` and is always cleaned up.
use crate::client;
use crate::clone_backend::{free_space, same_volume, select_backend};
use crate::config::Config;
use crate::envfile;
use crate::mask::MaskFile;
use crate::names::{format_duration, is_clone_id};
use crate::paths::Home;
use crate::pg::cluster::{check_socket_path, log_tail};
use crate::pg::{check_extensions, discover_with, find_major, initdb, start, stop, wait_ready};
use crate::pg::{managed, parse_version, PgInstall, StartOptions};
use crate::proc_util::{is_alive, process_start_time};
use crate::protocol::{DaemonStatus, MigrateCmd, Request, Response, TemplateSource};
use crate::service;
use crate::state::SCHEMA_VERSION;
use crate::templates::{
    age_secs, hash_dir, is_build_socket_name, now, parse_gen_dir, parse_leftover,
};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
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
    expires_at: i64,
    parent_pid: Option<i32>,
    parent_start_time: Option<u64>,
}

struct EnvRef {
    /// `None` for a pending restore.
    clone_id: Option<String>,
    file: PathBuf,
}

/// v1.0 template columns, read apart so a store that lacks one still gets its other checks.
struct TplExtra {
    name: String,
    mask_path: Option<PathBuf>,
    /// A branch template: its base template and git branch.
    base: Option<String>,
    branch: Option<String>,
}

/// Size of the event log.
struct EventStats {
    rows: u64,
    oldest: Option<i64>,
    bytes: u64,
}

#[derive(Default)]
struct Snapshot {
    templates: Vec<TplRow>,
    gens: Vec<GenRow>,
    clones: Vec<CloneRow>,
    pending: usize,
    env_files: Vec<EnvRef>,
    tpl_extra: Vec<TplExtra>,
    /// `(template, gen, mask_hash)` of every generation.
    mask_hashes: Vec<(String, u32, Option<String>)>,
    events: Option<EventStats>,
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
    let clones = rows(
        &conn,
        "SELECT id, name, dir, suspect, expires_at, parent_pid, parent_start_time FROM clones",
        |r| {
            Ok(CloneRow {
                id: r.get(0)?,
                name: r.get(1)?,
                dir: PathBuf::from(r.get::<_, String>(2)?),
                suspect: r.get(3)?,
                expires_at: r.get(4)?,
                parent_pid: r.get(5)?,
                parent_start_time: r.get::<_, Option<i64>>(6)?.map(|t| t as u64),
            })
        },
    )?;
    // A current-schema store has every table: a failed read is a real error, not an empty list.
    let pending = rows(&conn, "SELECT file FROM pending_restores", |r| {
        Ok(EnvRef {
            clone_id: None,
            file: PathBuf::from(r.get::<_, String>(0)?),
        })
    })?;
    let records = rows(&conn, "SELECT clone_id, file FROM env_records", |r| {
        Ok(EnvRef {
            clone_id: Some(r.get(0)?),
            file: PathBuf::from(r.get::<_, String>(1)?),
        })
    })?;
    let pending_n = pending.len();
    let mut env_files = pending;
    env_files.extend(records);
    let tpl_extra = rows(
        &conn,
        "SELECT name, mask_path, base, branch FROM templates",
        |r| {
            Ok(TplExtra {
                name: r.get(0)?,
                mask_path: r.get::<_, Option<String>>(1)?.map(PathBuf::from),
                base: r.get(2)?,
                branch: r.get(3)?,
            })
        },
    )?;
    let mask_hashes = rows(
        &conn,
        "SELECT template, gen, mask_hash FROM generations",
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let events = conn.query_row(
        "SELECT COUNT(*), MIN(ts), COALESCE(SUM(length(detail)), 0) FROM events",
        [],
        |r| {
            Ok(EventStats {
                rows: r.get::<_, i64>(0)?.max(0) as u64,
                oldest: r.get(1)?,
                bytes: r.get::<_, i64>(2)?.max(0) as u64,
            })
        },
    )?;
    let events = Some(events);
    Ok(StoreView::Ready(Snapshot {
        templates,
        gens,
        clones,
        pending: pending_n,
        env_files,
        tpl_extra,
        mask_hashes,
        events,
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

/// Clones a gc would destroy for their TTL or because their parent process is gone, as one line
/// each. Looked at by name and time only: a clone whose server also died is restarted or removed
/// by gc as its kind decides, which this does not predict.
fn doomed_clones(snap: &Snapshot, now: i64) -> Vec<String> {
    let mut out = Vec::new();
    for c in &snap.clones {
        let who = c.name.as_deref().unwrap_or(&c.id);
        if c.expires_at <= now {
            let ago = format_duration(Duration::from_secs((now - c.expires_at) as u64));
            out.push(format!("clone {who} (expired {ago} ago)"));
        } else if let Some(pid) = c.parent_pid {
            let alive = is_alive(pid)
                && c.parent_start_time
                    .is_none_or(|t| process_start_time(pid) == Some(t));
            if !alive {
                out.push(format!("clone {who} (its parent process {pid} is gone)"));
            }
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

/// Removes `logs/doctor-<major>-<pid>` folders that an interrupted doctor run left behind: the
/// process that made one is gone. A server still running in one is stopped first, and a folder
/// whose server will not stop stays.
fn sweep_smoke(home: &Home, installs: &[PgInstall]) {
    for (name, dir) in entries(&home.logs_dir()) {
        let Some(rest) = name.strip_prefix("doctor-") else {
            continue;
        };
        let parts = rest
            .split_once('-')
            .and_then(|(m, p)| Some((m.parse::<u32>().ok()?, p.parse::<i32>().ok()?)));
        let Some((major, pid)) = parts else { continue };
        if pid == std::process::id() as i32 || is_alive(pid) {
            continue;
        }
        let data = dir.join("data");
        if data.join("postmaster.pid").exists() {
            let Some(i) = installs.iter().find(|i| i.major == major) else {
                continue;
            };
            if stop(i, &data, None, Duration::from_secs(10)).is_err() {
                continue;
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
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

fn check_fs(home: &Home, cfg: &Config) -> Check {
    if !home.root.is_dir() {
        return Check::new(
            "home",
            Level::Fail,
            format!("{} does not exist", home.root.display()),
        )
        .fix("run any mitosis command to create it, or set MITOSIS_HOME");
    }
    let backend = match select_backend(home, cfg) {
        Ok(b) => b,
        Err(e) => {
            return Check::new("home", Level::Fail, e.message)
                .fix(e.hint.unwrap_or_else(|| "set backend in config.toml".into()))
        }
    };
    if let Err(e) = backend.check_supported(&home.root) {
        return Check::new("home", Level::Fail, e.message)
            .fix(e.hint.unwrap_or_else(|| "use a supported file system".into()));
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
                .fix("keep the whole home on one volume");
            }
            Err(e) => return Check::new("home", Level::Fail, e.message),
        }
    }
    Check::new(
        "home",
        Level::Ok,
        format!("{} on the {} back end, one volume", home.root.display(), backend.name()),
    )
}

fn check_backend_speed(home: &Home, cfg: &Config) -> Check {
    match select_backend(home, cfg) {
        Ok(b) if b.name() == "copy" => Check::new(
            "backend_speed",
            Level::Warn,
            "backend \"copy\" makes full copies; clones are slow and use full disk space",
        )
        .fix("put MITOSIS_HOME on Btrfs, XFS (mkfs.xfs -m reflink=1) or ZFS and set backend = \"auto\""),
        Ok(b) => Check::new("backend_speed", Level::Ok, format!("{} clones share data", b.name())),
        // `home` already reports an unusable back end.
        Err(_) => Check::new("backend_speed", Level::Ok, "skipped: no usable back end"),
    }
}

/// The ZFS checks of `mitosis zfs setup` for `dataset`: `zfs_tool`, `zfs_dataset`,
/// `zfs_permissions` and `zfs_sudo`; each failure carries its fix.
#[cfg(target_os = "linux")]
pub fn zfs_checks(dataset: &str) -> Vec<Check> {
    crate::clone_backend::zfs::probes(dataset)
        .into_iter()
        .map(|p| {
            let level = match (p.ok, p.warn) {
                (true, _) => Level::Ok,
                (false, true) => Level::Warn,
                (false, false) => Level::Fail,
            };
            let c = Check::new(p.name, level, p.detail);
            match p.fix {
                Some(f) => c.fix(f),
                None => c,
            }
        })
        .collect()
}

/// `zfs_checks` when the settings use ZFS (`backend = "zfs"`, or a `zfs_dataset` is set).
#[cfg(target_os = "linux")]
fn check_zfs(cfg: &Config) -> Vec<Check> {
    match &cfg.zfs_dataset {
        Some(ds) => zfs_checks(ds),
        None if cfg.backend == crate::clone_backend::BackendKind::Zfs => vec![Check::new(
            "zfs_dataset",
            Level::Fail,
            "backend = \"zfs\" needs zfs_dataset",
        )
        .fix("mitosis config set zfs_dataset <dataset>, for example tank/mitosis")],
        None => Vec::new(),
    }
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

async fn check_daemon(home: &Home) -> (Check, bool, Option<DaemonStatus>) {
    let ask = client::request_if_running(home, Request::Status);
    match tokio::time::timeout(Duration::from_secs(5), ask).await {
        Err(_) => (
            Check::new("daemon", Level::Warn, "did not answer within 5 s")
                .fix("restart it: mitosis daemon stop"),
            true,
            None,
        ),
        Ok(Ok(None)) => (
            Check::new("daemon", Level::Ok, "not running (starts on demand)"),
            false,
            None,
        ),
        Ok(Ok(Some(Response::Status(s)))) => {
            let mine = env!("CARGO_PKG_VERSION");
            let check = if s.version == mine {
                Check::new("daemon", Level::Ok, format!("running, version {mine}"))
            } else {
                Check::new(
                    "daemon",
                    Level::Warn,
                    format!("running version {}, this binary is {mine}", s.version),
                )
                .fix("mitosis daemon stop (the next command starts the new one)")
            };
            (check, true, Some(s))
        }
        Ok(Ok(Some(_))) => (
            Check::new(
                "daemon",
                Level::Warn,
                "unexpected reply to a status request",
            ),
            true,
            None,
        ),
        Ok(Err(e)) => {
            let c = Check::new("daemon", Level::Warn, e.message);
            (e.hint.map_or(c.clone(), |h| c.fix(h)), true, None)
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
        None => Check::new("service", Level::Warn, "could not ask the service manager"),
        Some(s) if !s.installed => Check::new("service", Level::Ok, "not installed (optional)"),
        Some(s) if !s.binary_exists => {
            Check::new("service", Level::Warn, "its binary no longer exists")
                .fix("mitosis service install")
        }
        Some(s) if !s.loaded => Check::new("service", Level::Warn, "installed but not loaded")
            .fix("mitosis service install"),
        Some(_) => match service::linger_hint() {
            Some(hint) => Check::new(
                "service",
                Level::Warn,
                "installed and loaded, but it stops when you log out",
            )
            .fix(hint),
            None => Check::new("service", Level::Ok, "installed and loaded"),
        },
    }
}

/// `name (pid N)` of the process listening on TCP `port`, from `lsof`; none when lsof is missing
/// or finds nothing.
async fn port_holder(port: u16) -> Option<String> {
    let run = tokio::process::Command::new("lsof")
        .args(["-nP", "-Fpc", &format!("-iTCP:{port}"), "-sTCP:LISTEN"])
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output();
    let out = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .ok()?
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let (mut pid, mut found) = (None, Vec::new());
    for line in text.lines() {
        match line.split_at_checked(1) {
            Some(("p", p)) => pid = Some(p.to_string()),
            Some(("c", c)) => {
                let who = format!("{c} (pid {})", pid.take().unwrap_or_default());
                if !found.contains(&who) {
                    found.push(who);
                }
            }
            _ => {}
        }
    }
    (!found.is_empty()).then(|| found.join(", "))
}

/// The router is listening, or its port is free for the daemon to bind. `daemon` is the running
/// daemon's answer (none when it is down, or up but silent).
async fn check_router(cfg: &Config, daemon_up: bool, daemon: Option<&DaemonStatus>) -> Check {
    let name = "router";
    if !cfg.router {
        return Check::new(
            name,
            Level::Ok,
            "off (router = off): clone URLs are the direct URLs",
        );
    }
    let addr = SocketAddr::new(cfg.router_listen, cfg.router_port);
    let taken = |what: String, holder: Option<String>| {
        let who = holder.map_or(
            format!(
                "could not tell which process (try: lsof -nP -iTCP:{} -sTCP:LISTEN)",
                addr.port()
            ),
            |h| format!("held by {h}"),
        );
        Check::new(name, Level::Fail, format!("{what}; {who}")).fix(
            "stop that process, or move the router: mitosis config set router_port <port> and mitosis daemon stop. Clones answer on their direct_url meanwhile (mitosis env <clone> --direct, which wakes a sleeping clone; or mitosis wake <clone>)"
            .to_string(),
        )
    };
    if daemon_up {
        return match daemon.map(|d| &d.router) {
            None => Check::new(name, Level::Warn, "the daemon did not report its router"),
            Some(None) => Check::new(
                name,
                Level::Warn,
                "the config has the router on but the running daemon has it off",
            )
            .fix("mitosis daemon stop (the next command starts it with the router)"),
            Some(Some(r)) if r.listening => Check::new(
                name,
                Level::Ok,
                format!("listening on {}:{}", r.listen, r.port),
            ),
            Some(Some(r)) => taken(
                format!(
                    "the daemon is not listening: {}",
                    r.error.as_deref().unwrap_or("no reason given")
                ),
                port_holder(r.port).await,
            ),
        };
    }
    match std::net::TcpListener::bind(addr) {
        Ok(_) => Check::new(
            name,
            Level::Ok,
            format!("{addr} is free; the daemon binds it when it starts"),
        ),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => taken(
            format!("router port {} is taken", addr.port()),
            port_holder(addr.port()).await,
        ),
        Err(e) => Check::new(name, Level::Fail, format!("cannot listen on {addr}: {e}"))
            .fix("check router_listen and router_port: mitosis config"),
    }
}

/// A router that listens beyond loopback is reachable from the network.
fn check_router_listen(cfg: &Config) -> Option<Check> {
    cfg.router.then(|| {
        let ip: IpAddr = cfg.router_listen;
        if ip.is_loopback() {
            return Check::new(
                "router_listen",
                Level::Ok,
                format!("{ip}: this Mac only (Docker Desktop containers reach it as host.docker.internal)"),
            );
        }
        Check::new(
            "router_listen",
            Level::Warn,
            format!("{ip}: the router is reachable from the network (each clone's password still protects it)"),
        )
        .fix("mitosis config set router_listen 127.0.0.1")
    })
}

/// Managed builds under `<home>/pg/`: each one must run. Blocking.
fn check_managed(home: &Home) -> Check {
    let name = "managed_postgres";
    let installs = match managed::list(home) {
        Ok(v) => v,
        Err(e) => return Check::new(name, Level::Warn, e.message),
    };
    if installs.is_empty() {
        return Check::new(
            name,
            Level::Ok,
            "none installed (mitosis pg install <major>)",
        );
    }
    let mut f = Vec::new();
    for i in &installs {
        let bin = i.dir.join("bin/postgres");
        let out = std::process::Command::new(&bin)
            .arg("--version")
            .stdin(std::process::Stdio::null())
            .output();
        let problem = match out {
            Err(e) => Some(e.to_string()),
            Ok(o) if !o.status.success() => Some(format!("exited with {}", o.status)),
            Ok(o) => match parse_version(&String::from_utf8_lossy(&o.stdout)) {
                Some((major, _)) if major == i.major => None,
                _ => Some("its version output is not Postgres ".to_string() + &i.major.to_string()),
            },
        };
        if let Some(p) = problem {
            f.push((
                Level::Warn,
                format!(
                    "managed Postgres {} does not run ({}): {p}",
                    i.version,
                    bin.display()
                ),
            ));
        }
    }
    let c = summarize(
        name,
        f,
        &format!(
            "{} build(s), all run: {}",
            installs.len(),
            installs
                .iter()
                .map(|i| i.version.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    );
    if c.level == Level::Ok {
        c
    } else {
        c.fix("mitosis pg remove <version>, then mitosis pg install <major>")
    }
}

/// Masked templates: the masking file is still there and is the one the current generation was
/// built with.
fn check_masks(snap: &Snapshot) -> Check {
    let mut f = Vec::new();
    let mut masked = 0;
    for x in &snap.tpl_extra {
        let Some(path) = &x.mask_path else { continue };
        masked += 1;
        let Some(t) = snap.templates.iter().find(|t| t.name == x.name) else {
            continue;
        };
        if snap.current(t).is_none() {
            continue; // nothing built yet
        }
        if !path.is_file() {
            f.push((
                Level::Fail,
                format!(
                    "template {}: mask file {} is missing",
                    x.name,
                    path.display()
                ),
            ));
            continue;
        }
        let built = snap
            .mask_hashes
            .iter()
            .find(|(n, g, _)| *n == x.name && *g == t.current_gen)
            .and_then(|(_, _, h)| h.as_deref());
        if MaskFile::hash(path).ok().as_deref() != built {
            f.push((
                Level::Warn,
                format!(
                    "template {}: mask file {} changed since it was built; run: mitosis template refresh {}",
                    x.name,
                    path.display(),
                    x.name
                ),
            ));
        }
    }
    if masked == 0 {
        return Check::new("masking", Level::Ok, "no masked templates");
    }
    summarize("masking", f, "mask files present and unchanged")
}

/// True when git says `branch` is not in the repository at `dir` (`rev-parse --verify` exits 1),
/// the test gc applies (`lifecycle::branch_deleted`, private there). Blocking.
fn branch_gone(dir: &Path, branch: &str) -> bool {
    std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "--verify", "--quiet"])
        .arg(format!("refs/heads/{branch}"))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.code() == Some(1))
}

/// Branch templates whose branch no longer exists in the base's repository: gc drops them.
async fn check_branches(snap: &Snapshot) -> Check {
    let name = "branch_templates";
    let mut jobs = Vec::new();
    let mut n = 0;
    for x in &snap.tpl_extra {
        let (Some(base), Some(branch)) = (&x.base, &x.branch) else {
            continue;
        };
        n += 1;
        // The same repository gc looks in: the base's migrate folder.
        let dir = snap
            .templates
            .iter()
            .find(|t| &t.name == base)
            .and_then(|t| t.migrate.as_ref())
            .map(|m| m.cwd.clone())
            .filter(|d| d.is_dir());
        if let Some(dir) = dir {
            jobs.push((x.name.clone(), dir, branch.clone()));
        }
    }
    if n == 0 {
        return Check::new(name, Level::Ok, "no branch templates");
    }
    let gone = tokio::task::spawn_blocking(move || {
        jobs.into_iter()
            .filter(|(_, dir, branch)| branch_gone(dir, branch))
            .collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();
    let f = gone
        .into_iter()
        .map(|(t, dir, branch)| {
            (
                Level::Warn,
                format!(
                    "template {t}: branch {branch} no longer exists in {}; gc drops it",
                    dir.display()
                ),
            )
        })
        .collect();
    let c = summarize(name, f, "every branch still exists");
    if c.level == Level::Ok {
        c
    } else {
        c.fix("mitosis gc (a template a clone still uses is kept)")
    }
}

/// The scheduler keeps 30 days and 50,000 rows of events.
const EVENT_KEEP_ROWS: u64 = 50_000;
const EVENT_KEEP_SECS: i64 = 30 * 86_400;

fn check_events(snap: &Snapshot) -> Check {
    let name = "events";
    let Some(e) = &snap.events else {
        return Check::new(name, Level::Ok, "no event log yet");
    };
    let age = e.oldest.map_or(0, |ts| now().saturating_sub(ts).max(0));
    let size = format!(
        "{} event(s), about {} kB, oldest {} ago",
        e.rows,
        e.bytes / 1000,
        format_duration(Duration::from_secs(age as u64))
    );
    // A day of slack: the scheduler prunes on its own timer.
    if e.rows > EVENT_KEEP_ROWS || age > EVENT_KEEP_SECS + 86_400 {
        return Check::new(
            name,
            Level::Warn,
            format!(
                "{size}; past the 30 day / 50000 row retention, the daemon prunes it while it runs"
            ),
        )
        .fix("run any mitosis command so the daemon starts and prunes");
    }
    Check::new(name, Level::Ok, size)
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
    // Only names the socket file: the throwaway server opens no TCP listener.
    let port = *cfg.port_range.start();
    let h = home.clone();
    let swept = installs.to_vec();
    let _ = tokio::task::spawn_blocking(move || sweep_smoke(&h, &swept)).await;
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

/// Clones from 0.2 whose name looks like a clone id (new names may not).
fn check_clone_names(snap: &Snapshot) -> Check {
    let f = snap
        .clones
        .iter()
        .filter_map(|c| c.name.as_deref())
        .filter(|n| crate::names::is_clone_id(n))
        .map(|n| {
            (
                Level::Warn,
                format!(
                    "clone {n} has a name that looks like a clone id: router URLs by that name \
                     may reach the clone with that id; rename by recreating"
                ),
            )
        })
        .collect();
    summarize("clone_names", f, "none look like clone ids")
}

fn check_gc(home: &Home, snap: &Snapshot, daemon_up: bool) -> Check {
    let orphans = orphan_folders(home, snap);
    let mut f: Vec<(Level, String)> = orphans
        .iter()
        .map(|p| (Level::Warn, format!("orphan folder {}", p.display())))
        .collect();
    f.extend(stale_rows(snap).into_iter().map(|s| (Level::Warn, s)));
    let doomed = doomed_clones(snap, now());
    f.extend(
        doomed
            .iter()
            .map(|d| (Level::Warn, format!("gc would destroy {d}"))),
    );
    if daemon_up && !orphans.is_empty() {
        f.push((
            Level::Warn,
            "a build or create in progress owns some of these until it finishes".into(),
        ));
    }
    let c = summarize("gc_dry_run", f, "nothing for gc to clean");
    if c.level == Level::Ok {
        c
    } else if doomed.is_empty() {
        c.fix("mitosis doctor --fix (or mitosis gc)")
    } else {
        // --fix leaves gc alone while it would destroy clones; that is a decision for the user.
        c.fix("mitosis gc (it destroys the clones listed above too)")
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
        let (done, warnings) = envfile::finish_pending_counted(home).await;
        f.push((
            Level::Ok,
            format!(
                "finished {done} of {} pending .env restore(s)",
                snap.pending
            ),
        ));
        f.extend(warnings.into_iter().map(|w| (Level::Warn, w)));
    }
    let doomed = doomed_clones(&snap, now());
    if !doomed.is_empty() {
        // gc destroys what is due, along with the orphans; the user decides that, not --fix.
        f.push((
            Level::Warn,
            format!(
                "gc skipped: it would also destroy {} clone(s) you did not ask about: {}",
                doomed.len(),
                doomed.join(", ")
            ),
        ));
        f.push((Level::Warn, "run `mitosis gc` to run it anyway".into()));
    } else if !orphan_folders(home, &snap).is_empty() || !stale_rows(&snap).is_empty() {
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
    let loaded = Config::load(home);
    out.push(check_fs(home, &loaded.clone().unwrap_or_default()));
    out.push(check_backend_speed(home, &loaded.clone().unwrap_or_default()));
    #[cfg(target_os = "linux")]
    out.extend(check_zfs(&loaded.clone().unwrap_or_default()));
    out.push(check_permissions(home));

    let cfg = match loaded {
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

    let (daemon, daemon_up, status) = check_daemon(home).await;
    out.push(daemon);
    out.push(check_router(&cfg, daemon_up, status.as_ref()).await);
    out.extend(check_router_listen(&cfg));
    let svc = service_status().await;
    out.push(check_service(&svc));

    let h = home.clone();
    let installs = tokio::task::spawn_blocking(move || discover_with(Some(&h)))
        .await
        .unwrap_or_default();
    out.push(check_postgres(&installs));
    let h = home.clone();
    out.push(
        tokio::task::spawn_blocking(move || check_managed(&h))
            .await
            .unwrap_or_else(|e| Check::new("managed_postgres", Level::Warn, e.to_string())),
    );

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
        out.push(check_clone_names(snap));
        out.push(check_gc(home, snap, daemon_up));
        out.push(check_env(snap));
        out.push(check_privacy(snap, &svc));
        out.push(check_masks(snap));
        out.push(check_branches(snap).await);
        out.push(check_events(snap));
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

    fn clone_row(id: &str, expires_at: i64, parent: Option<i32>) -> crate::state::CloneRow {
        crate::state::CloneRow {
            id: id.into(),
            name: None,
            template: "main".into(),
            dir: PathBuf::from(format!("/h/clones/{id}")),
            port: 54321,
            socket_dir: PathBuf::from(format!("/h/sockets/{id}")),
            password: "pw".into(),
            database: "app".into(),
            pg_pid: None,
            pg_start_time: None,
            parent_pid: parent,
            parent_start_time: None,
            created_at: 0,
            expires_at,
            durable: false,
            status: crate::state::CloneStatus::Ready,
            generation: 1,
            protected: false,
            suspect: false,
        }
    }

    #[test]
    fn doomed_clones_lists_expired_and_orphaned_ones() {
        let row = |id: &str, expires_at, parent_pid| CloneRow {
            id: id.into(),
            name: None,
            dir: PathBuf::new(),
            suspect: false,
            expires_at,
            parent_pid,
            parent_start_time: None,
        };
        let snap = Snapshot {
            clones: vec![
                row("c000001", 100, None),
                row("c000002", 10_000, None),
                row("c000003", 10_000, Some(std::process::id() as i32)),
                // Far above any real pid limit on macOS.
                row("c000004", 10_000, Some(2_000_000_000)),
            ],
            ..Snapshot::default()
        };
        let got = doomed_clones(&snap, 7_300);
        assert_eq!(got.len(), 2, "{got:?}");
        assert!(got[0].starts_with("clone c000001 (expired 2h"), "{got:?}");
        assert!(
            got[1].contains("c000004") && got[1].contains("parent"),
            "{got:?}"
        );
    }

    #[tokio::test]
    async fn fix_leaves_gc_alone_while_it_would_destroy_clones() {
        let tmp = tempfile::tempdir().unwrap();
        let home = Home::at(tmp.path().to_path_buf());
        home.ensure_dirs().unwrap();
        {
            let store = crate::state::Store::open(&home.state_db()).unwrap();
            store.insert_clone(&clone_row("c000001", 1, None)).unwrap();
        }
        let orphan = home.clones_dir().join("c000002");
        std::fs::create_dir_all(&orphan).unwrap();
        let check = apply_fix(&home).await;
        assert!(check.detail.contains("gc skipped"), "{}", check.detail);
        assert!(check.detail.contains("c000001"), "{}", check.detail);
        // Nothing was removed, and no daemon was asked (there is none to answer).
        assert!(orphan.exists());
        assert!(!home.daemon_sock().exists());
        // The dry run lists the same clone.
        let StoreView::Ready(snap) = read_store(&home) else {
            panic!("store not readable");
        };
        let gc = check_gc(&home, &snap, false);
        assert!(
            gc.detail.contains("gc would destroy clone c000001"),
            "{}",
            gc.detail
        );
    }

    #[test]
    fn sweep_removes_folders_of_dead_runs_only() {
        let tmp = tempfile::tempdir().unwrap();
        let home = Home::at(tmp.path().to_path_buf());
        home.ensure_dirs().unwrap();
        let dead = home.logs_dir().join("doctor-18-2000000000");
        let mine = home
            .logs_dir()
            .join(format!("doctor-18-{}", std::process::id()));
        let other = home.logs_dir().join("service.log.d");
        for d in [&dead, &mine, &other] {
            std::fs::create_dir_all(d.join("data")).unwrap();
        }
        sweep_smoke(&home, &[]);
        assert!(!dead.exists());
        assert!(mine.exists());
        assert!(other.exists());
    }
}
