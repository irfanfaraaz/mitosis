use crate::clone_backend::{free_space, private_size};
use crate::ctx::Ctx;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::names::{random_hex, validate_name};
use crate::pg::{
    check_extensions, find_major, newest, psql, read_pg_version, read_postmaster_pid, start, stop, wait_ready,
    PgInstall, StartOptions,
};
use crate::ports::pick_port;
use crate::proc_util::{is_alive, process_start_time, rss_bytes};
use crate::protocol::{CloneCreateArgs, CloneInfo};
use crate::state::{CloneRow, CloneStatus, TemplateRow};
use crate::templates::{info, now};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};
use std::path::Path;

/// Start attempts when the chosen port turns out to be taken.
const START_TRIES: usize = 3;

fn internal(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, msg)
}

fn invalid(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::InvalidRequest, msg)
}

/// Runs blocking work (Postgres tools, big file operations) off the async runtime.
async fn blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(|e| internal(format!("background task failed: {e}")))?
}

pub fn to_info(row: &CloneRow, template_age_secs: u64) -> CloneInfo {
    CloneInfo {
        id: row.id.clone(),
        name: row.name.clone(),
        template: row.template.clone(),
        url: format!("postgres://postgres:{}@127.0.0.1:{}/{}", row.password, row.port, row.database),
        port: row.port,
        socket_dir: row.socket_dir.clone(),
        template_age_secs,
        created_at: row.created_at,
        expires_at: row.expires_at,
        durable: row.durable,
        parent_pid: row.parent_pid,
        rss_bytes: None,
        own_disk_bytes: None,
    }
}

pub async fn create(ctx: &Ctx, args: CloneCreateArgs) -> Result<CloneInfo> {
    if let Some(name) = &args.name {
        validate_name("clone", name)?;
    }
    if args.ttl_secs == 0 {
        return Err(invalid("ttl must be greater than zero"));
    }
    let tpl = ctx.store().get_template(&args.template)?.ok_or_else(|| {
        MitosisError::new(ErrorCode::TemplateNotFound, format!("template {} not found", args.template))
            .with_hint(format!("mitosis template init {} --seed \"…\"", args.template))
    })?;
    let install = find_major(&ctx.installs, tpl.pg_major)?.clone();
    check_extensions(&install, &tpl.extensions)?;
    let parent_start_time = match args.parent_pid {
        Some(pid) => Some(
            process_start_time(pid)
                .filter(|_| is_alive(pid))
                .ok_or_else(|| invalid(format!("parent process {pid} is not running")))?,
        ),
        None => None,
    };

    let (mut row, _in_flight) = reserve(ctx, &tpl, &args, parent_start_time)?;
    // A vanished row (CloneNotFound from an update) also lands here: stop, remove, report it.
    match bring_up(ctx, &install, &tpl, &mut row).await {
        Ok(info) => Ok(info),
        Err(e) => {
            // A failed cleanup leaves the row in Stopping for gc; the caller still gets the cause.
            if let Err(ce) = stop_and_remove(ctx, &row).await {
                let log = ctx.home.logs_dir().join(format!("{}.log", row.id));
                if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&log) {
                    let _ = writeln!(f, "mitosis: cleanup after failed create failed: {ce}");
                }
            }
            Err(e)
        }
    }
}

/// Marks a clone id as being created (see `Ctx::creating`) until dropped.
struct InFlight<'a> {
    ctx: &'a Ctx,
    id: String,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.ctx.creating.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.id);
    }
}

/// Steps 3-4 under one store lock, so concurrent creates cannot both pass the cap or share a port.
/// The id joins `ctx.creating` under that same lock, so reconcile never sees the row without it.
fn reserve<'a>(
    ctx: &'a Ctx,
    tpl: &TemplateRow,
    args: &CloneCreateArgs,
    parent_start_time: Option<u64>,
) -> Result<(CloneRow, InFlight<'a>)> {
    let store = ctx.store();
    let active: Vec<String> =
        store.list_clones()?.into_iter().filter(|c| c.status != CloneStatus::Stopping).map(|c| c.id).collect();
    if active.len() >= ctx.config.max_clones {
        return Err(MitosisError::new(
            ErrorCode::CapReached,
            format!("clone limit of {} reached: {}", ctx.config.max_clones, active.join(", ")),
        )
        .with_hint("destroy one with: mitosis clone destroy <id>"));
    }
    let clones_dir = ctx.home.clones_dir();
    let free = free_space(&clones_dir)?;
    if free < ctx.config.min_free_bytes {
        return Err(MitosisError::new(
            ErrorCode::LowDisk,
            format!(
                "only {free} bytes free on the volume of {}; need at least {}",
                clones_dir.display(),
                ctx.config.min_free_bytes
            ),
        )
        .with_hint("free disk space, or destroy unused clones"));
    }
    let id = loop {
        let id = format!("c{}", random_hex(3));
        if store.get_clone(&id)?.is_none() && !clones_dir.join(&id).exists() {
            break id;
        }
    };
    let port = pick_port(ctx.config.port_range.clone(), &store.leased_ports()?, &HashSet::new())?;
    let created_at = now();
    let row = CloneRow {
        id: id.clone(),
        name: args.name.clone(),
        template: tpl.name.clone(),
        dir: clones_dir.join(&id),
        port,
        socket_dir: ctx.home.sockets_dir().join(&id),
        password: random_hex(16),
        database: tpl.database.clone(),
        pg_pid: None,
        pg_start_time: None,
        parent_pid: args.parent_pid,
        parent_start_time,
        created_at,
        expires_at: created_at.saturating_add(i64::try_from(args.ttl_secs).unwrap_or(i64::MAX)),
        durable: args.durable,
        status: CloneStatus::Creating,
    };
    store.insert_clone(&row)?;
    ctx.creating.lock().unwrap_or_else(|e| e.into_inner()).insert(id.clone());
    Ok((row, InFlight { ctx, id }))
}

fn set_status(ctx: &Ctx, row: &mut CloneRow, status: CloneStatus) -> Result<()> {
    row.status = status;
    ctx.store().update_clone(row)
}

/// Steps 5-6: clone the template folder, start Postgres, set the password. `row` tracks what was done.
async fn bring_up(ctx: &Ctx, install: &PgInstall, tpl: &TemplateRow, row: &mut CloneRow) -> Result<CloneInfo> {
    let (backend, src, dst) = (ctx.backend.clone(), tpl.dir.clone(), row.dir.clone());
    blocking(move || backend.clone_dir(&src, &dst)).await?;
    set_status(ctx, row, CloneStatus::Starting)?;

    let log_file = ctx.home.logs_dir().join(format!("{}.log", row.id));
    let mut tried = HashSet::new();
    let mut tries = 0;
    let pg = loop {
        tries += 1;
        let opts = StartOptions {
            port: row.port,
            socket_dir: row.socket_dir.clone(),
            listen_tcp: true,
            durable: row.durable,
            log_file: log_file.clone(),
        };
        let pg = start(install, &row.dir, &opts)?;
        match wait_ready(install, &pg, &opts, ctx.config.ready_timeout).await {
            Ok(()) => break pg,
            // Template builds use ports the store does not know about, so a pick can still collide.
            Err(e) if e.code == ErrorCode::PortInUse && tries < START_TRIES => {
                let (i, d, t) = (install.clone(), row.dir.clone(), ctx.config.stop_timeout);
                blocking(move || stop(&i, &d, t)).await?;
                tried.insert(row.port);
                let store = ctx.store();
                row.port = pick_port(ctx.config.port_range.clone(), &store.leased_ports()?, &tried)?;
                store.update_clone(row)?;
            }
            Err(e) => return Err(e),
        }
    };

    // PG 12/13 default to md5; initdb set host auth to scram-sha-256, so store a SCRAM hash.
    let sql =
        format!("SET password_encryption = 'scram-sha-256'; ALTER ROLE postgres PASSWORD '{}'", row.password);
    let (i, s, p) = (install.clone(), row.socket_dir.clone(), row.port);
    blocking(move || psql(&i, &s, p, "postgres", &sql)).await.map_err(|mut e| {
        e.message = e.message.replace(&row.password, "***");
        e
    })?;

    row.pg_pid = Some(pg.pid);
    row.pg_start_time = Some(process_start_time(pg.pid).unwrap_or(pg.start_time));
    set_status(ctx, row, CloneStatus::Ready)?;
    Ok(to_info(row, info(tpl, now()).age_secs))
}

pub async fn destroy(ctx: &Ctx, id_or_name: &str) -> Result<String> {
    let row = ctx.store().get_clone(id_or_name)?.ok_or_else(|| {
        MitosisError::new(ErrorCode::CloneNotFound, format!("clone {id_or_name} not found"))
            .with_hint("see existing clones with: mitosis clone list")
    })?;
    // An in-flight create owns these; removing them under it would orphan its server.
    if matches!(row.status, CloneStatus::Creating | CloneStatus::Starting) {
        return Err(invalid(format!("clone {} is still starting; try again in a moment", row.id)));
    }
    stop_and_remove(ctx, &row).await?;
    Ok(row.id)
}

/// The install whose `pg_ctl` stops the cluster in `dir`: its own major, else the newest.
pub(crate) fn install_for(ctx: &Ctx, dir: &Path) -> Option<PgInstall> {
    read_pg_version(dir)
        .ok()
        .and_then(|m| find_major(&ctx.installs, m).ok())
        .or_else(|| newest(&ctx.installs).ok())
        .cloned()
}

fn remove_if_present(path: &Path, dir: bool) -> Result<()> {
    let res = if dir { std::fs::remove_dir_all(path) } else { std::fs::remove_file(path) };
    match res {
        Err(e) if e.kind() != ErrorKind::NotFound => {
            Err(internal(format!("remove {}: {e}", path.display())))
        }
        _ => Ok(()),
    }
}

/// Marks the clone Stopping, stops its Postgres, removes its folders and log, then drops the row.
/// If Postgres may still be running, nothing is removed and the error is returned.
/// A missing row is fine: the folders are still cleaned up.
pub(crate) async fn stop_and_remove(ctx: &Ctx, row: &CloneRow) -> Result<()> {
    ctx.store().set_clone_status(&row.id, CloneStatus::Stopping)?;
    let dir = ctx.home.clones_dir().join(&row.id);
    let sock = ctx.home.sockets_dir().join(&row.id);
    let log = ctx.home.logs_dir().join(format!("{}.log", row.id));
    let install = install_for(ctx, &dir);
    let timeout = ctx.config.stop_timeout;
    blocking(move || {
        if read_postmaster_pid(&dir)?.is_some() {
            let install = install
                .ok_or_else(|| internal(format!("no Postgres install found to stop the server in {}", dir.display())))?;
            stop(&install, &dir, timeout)?;
        }
        remove_if_present(&dir, true)?;
        remove_if_present(&sock, true)?;
        remove_if_present(&log, false)
    })
    .await?;
    ctx.store().delete_clone(&row.id)
}

pub fn list(ctx: &Ctx) -> Result<Vec<CloneInfo>> {
    let (rows, templates) = {
        let store = ctx.store();
        (store.list_clones()?, store.list_templates()?)
    };
    let now = now();
    let ages: HashMap<&str, u64> = templates.iter().map(|t| (t.name.as_str(), info(t, now).age_secs)).collect();
    Ok(rows
        .iter()
        .map(|r| {
            let mut ci = to_info(r, ages.get(r.template.as_str()).copied().unwrap_or(0));
            // Postmaster plus its backends; skip a pid that was reused by another process.
            ci.rss_bytes =
                r.pg_pid.filter(|pid| process_start_time(*pid) == r.pg_start_time).and_then(rss_bytes);
            ci.own_disk_bytes = private_size(&r.dir);
            ci
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn info_builds_the_url() {
        let row = CloneRow {
            id: "c0a1b2".into(),
            name: Some("feat".into()),
            template: "main".into(),
            dir: PathBuf::from("/h/clones/c0a1b2"),
            port: 54321,
            socket_dir: PathBuf::from("/h/sockets/c0a1b2"),
            password: "ab".repeat(16),
            database: "app".into(),
            pg_pid: Some(1),
            pg_start_time: Some(2),
            parent_pid: Some(3),
            parent_start_time: Some(4),
            created_at: 10,
            expires_at: 20,
            durable: true,
            status: CloneStatus::Ready,
        };
        let i = to_info(&row, 5);
        assert_eq!(i.url, format!("postgres://postgres:{}@127.0.0.1:54321/app", "ab".repeat(16)));
        assert_eq!((i.id.as_str(), i.template_age_secs, i.parent_pid, i.durable), ("c0a1b2", 5, Some(3), true));
        assert_eq!((i.rss_bytes, i.own_disk_bytes), (None, None));
    }

    // The daemon spawns these futures on a multi-thread runtime, so they must be Send.
    #[allow(dead_code)]
    fn futures_are_send(ctx: &'static Ctx, args: CloneCreateArgs, row: &'static CloneRow) {
        fn send<T: Send>(_: T) {}
        send(create(ctx, args));
        send(destroy(ctx, "x"));
        send(stop_and_remove(ctx, row));
    }
}
