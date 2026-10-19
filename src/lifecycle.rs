use crate::clones::{destroy, install_for, stop_and_remove};
use crate::ctx::Ctx;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::pg::{read_postmaster_pid, stop, PostmasterInfo};
use crate::proc_util::{is_alive, process_start_time};
use crate::protocol::GcReport;
use crate::state::{CloneRow, CloneStatus};
use crate::templates::now;
use nix::errno::Errno;
use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Allowed difference, in seconds, between a recorded and a measured process start time.
const START_SLACK: u64 = 1;
/// Destroy attempts by a parent watcher while the clone is still starting.
const DESTROY_TRIES: u32 = 20;
const DESTROY_RETRY_DELAY: Duration = Duration::from_millis(500);

fn internal(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, msg)
}

/// Appends one line to `logs/daemon.log`; lifecycle work has no caller to report errors to.
fn log(ctx: &Ctx, msg: &str) {
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(ctx.home.logs_dir().join("daemon.log")) {
        let _ = writeln!(f, "[{}] {msg}", now());
    }
}

fn start_matches(pid: i32, start_time: u64) -> bool {
    process_start_time(pid).is_some_and(|t| t.abs_diff(start_time) <= START_SLACK)
}

/// True while `pid` runs and, when a start time was recorded, is still the same process.
pub fn parent_alive(pid: i32, start_time: Option<u64>) -> bool {
    is_alive(pid) && start_time.is_none_or(|t| start_matches(pid, t))
}

/// The postmaster named by `dir/postmaster.pid` when it is alive and really serves `dir`.
fn live_postmaster(dir: &Path) -> Option<PostmasterInfo> {
    let info = read_postmaster_pid(dir).ok()??;
    let same_dir = matches!((info.data_dir.canonicalize(), dir.canonicalize()), (Ok(a), Ok(b)) if a == b);
    // Same slack as `cluster::stop` uses for the pid file's own start time.
    let same_proc = process_start_time(info.pid).is_some_and(|t| t.abs_diff(info.start_time) <= 2);
    (same_dir && same_proc && is_alive(info.pid)).then_some(info)
}

/// Removes a `postmaster.pid` that names no live server for `dir`, so `pg_ctl stop` never signals
/// an unrelated process that reused the pid.
fn clear_stale_pid(dir: &Path) {
    let file = dir.join("postmaster.pid");
    if file.exists() && live_postmaster(dir).is_none() {
        let _ = std::fs::remove_file(file);
    }
}

fn in_flight(ctx: &Ctx, id: &str) -> bool {
    ctx.creating.lock().unwrap_or_else(|e| e.into_inner()).contains(id)
}

/// Destroys Ready clones whose TTL has run out; returns the ids destroyed.
pub async fn reap_expired(ctx: &Ctx, now: i64) -> Result<Vec<String>> {
    let rows = ctx.store().list_clones()?;
    let mut reaped = Vec::new();
    // Creating/Starting rows belong to an in-flight create; the next pass gets them.
    for row in rows.iter().filter(|r| r.status == CloneStatus::Ready && r.expires_at <= now) {
        match destroy(ctx, &row.id).await {
            Ok(id) => reaped.push(id),
            Err(e) if e.code == ErrorCode::CloneNotFound => {}
            Err(e) => log(ctx, &format!("reaper: clone {}: {e}", row.id)),
        }
    }
    Ok(reaped)
}

/// Watches `parent_pid` on a thread; when it exits, destroys the clone on the current tokio runtime.
/// Must be called from within the runtime. A second watch for the same clone is a no-op.
pub fn watch_parent(ctx: Arc<Ctx>, clone_id: String, parent_pid: i32) {
    let parent_start_time = match ctx.store().get_clone(&clone_id) {
        Ok(Some(row)) => row.parent_start_time,
        Ok(None) => return,
        Err(e) => {
            log(&ctx, &format!("parent watch for clone {clone_id}: {e}"));
            None
        }
    };
    let handle = match tokio::runtime::Handle::try_current() {
        Ok(h) => h,
        Err(e) => return log(&ctx, &format!("parent watch for clone {clone_id}: no tokio runtime: {e}")),
    };
    if !ctx.watching.lock().unwrap_or_else(|e| e.into_inner()).insert(clone_id.clone()) {
        return;
    }
    std::thread::spawn(move || {
        if let Err(e) = wait_exit_kqueue(parent_pid, parent_start_time) {
            log(&ctx, &format!("parent watch for clone {clone_id}: kqueue failed ({e}); polling instead"));
            while parent_alive(parent_pid, parent_start_time) {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        handle.spawn(destroy_after_parent_exit(ctx, clone_id));
    });
}

/// Blocks until `pid` exits. Returns at once if it is already gone or is no longer the same process.
fn wait_exit_kqueue(pid: i32, start_time: Option<u64>) -> nix::Result<()> {
    let kq = Kqueue::new()?;
    let watch = KEvent::new(
        pid as usize,
        EventFilter::EVFILT_PROC,
        EvFlags::EV_ADD | EvFlags::EV_ONESHOT,
        FilterFlag::NOTE_EXIT,
        0,
        0,
    );
    // An empty event list makes kevent only register, reporting failures as its own error.
    match kq.kevent(&[watch], &mut [], None) {
        Err(Errno::ESRCH) => return Ok(()),
        r => r?,
    };
    // Covers an exit just before registering, and a pid reused by another process.
    if !parent_alive(pid, start_time) {
        return Ok(());
    }
    let mut out = [watch];
    loop {
        match kq.kevent(&[], &mut out, None) {
            Ok(n) if n > 0 => return Ok(()),
            Ok(_) | Err(Errno::EINTR) => {}
            Err(e) => return Err(e),
        }
    }
}

async fn destroy_after_parent_exit(ctx: Arc<Ctx>, id: String) {
    for attempt in 1..=DESTROY_TRIES {
        match destroy(&ctx, &id).await {
            Ok(_) => break,
            Err(e) if e.code == ErrorCode::CloneNotFound => break,
            // destroy refuses a Creating/Starting row; the create finishes shortly.
            Err(e) if e.code == ErrorCode::InvalidRequest && attempt < DESTROY_TRIES => {
                tokio::time::sleep(DESTROY_RETRY_DELAY).await
            }
            Err(e) => {
                log(&ctx, &format!("parent of clone {id} exited, but destroy failed: {e}"));
                break;
            }
        }
    }
    ctx.watching.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
}

/// Brings the store and the home folder back in line after a crash; safe while requests are served.
/// Clones with an in-flight create and templates with a running build are left alone.
/// A failure on one entry is logged to `logs/daemon.log` and the rest still run.
pub async fn reconcile(ctx: Arc<Ctx>, now: i64) -> Result<GcReport> {
    let mut report = GcReport::default();
    let ids: Vec<String> = ctx.store().list_clones()?.into_iter().map(|r| r.id).collect();
    for id in ids {
        if in_flight(&ctx, &id) {
            continue;
        }
        // Re-read after the in-flight check: a create that just finished shows its final status.
        let row = match ctx.store().get_clone(&id)? {
            Some(row) if row.id == id => row,
            _ => continue,
        };
        if let Err(e) = reconcile_clone(&ctx, &row, now, &mut report).await {
            log(&ctx, &format!("gc: clone {id}: {e}"));
        }
    }
    remove_orphans(&ctx, &mut report).await;
    Ok(report)
}

async fn reconcile_clone(ctx: &Arc<Ctx>, row: &CloneRow, now: i64, report: &mut GcReport) -> Result<()> {
    let dir = ctx.home.clones_dir().join(&row.id);
    if !dir.exists() {
        stop_and_remove(ctx, row).await?; // also clears its socket folder and log
        report.records_dropped.push(row.id.clone());
        return Ok(());
    }
    let running = row.status == CloneStatus::Ready
        && live_postmaster(&dir).is_some_and(|pm| {
            row.pg_pid == Some(pm.pid) && row.pg_start_time.is_some_and(|t| start_matches(pm.pid, t))
        });
    let keep = running
        && row.expires_at > now
        && row.parent_pid.is_none_or(|p| parent_alive(p, row.parent_start_time));
    if !keep {
        clear_stale_pid(&dir);
        stop_and_remove(ctx, row).await?;
        report.destroyed.push(row.id.clone());
        return Ok(());
    }
    report.adopted.push(row.id.clone());
    if let Some(pid) = row.parent_pid {
        watch_parent(ctx.clone(), row.id.clone(), pid);
    }
    Ok(())
}

fn entries(dir: &Path) -> Vec<(String, PathBuf)> {
    let Ok(rd) = std::fs::read_dir(dir) else { return Vec::new() };
    rd.flatten().map(|e| (e.file_name().to_string_lossy().into_owned(), e.path())).collect()
}

fn remove_path(path: &Path) -> Result<()> {
    let res = if path.is_dir() { std::fs::remove_dir_all(path) } else { std::fs::remove_file(path) };
    match res {
        Err(e) if e.kind() != ErrorKind::NotFound => Err(internal(format!("remove {}: {e}", path.display()))),
        _ => Ok(()),
    }
}

/// Stops the Postgres serving `dir`, if any, then deletes the folder.
async fn remove_data_dir(ctx: &Ctx, dir: PathBuf) -> Result<()> {
    let install = install_for(ctx, &dir);
    let timeout = ctx.config.stop_timeout;
    tokio::task::spawn_blocking(move || {
        if live_postmaster(&dir).is_some() {
            let install = install
                .ok_or_else(|| internal(format!("no Postgres install found to stop the server in {}", dir.display())))?;
            stop(&install, &dir, timeout)?;
        }
        remove_path(&dir)
    })
    .await
    .map_err(|e| internal(format!("background task failed: {e}")))?
}

/// Step 6: folders in `clones/`, `sockets/` and `templates/` that nothing owns any more.
async fn remove_orphans(ctx: &Ctx, report: &mut GcReport) {
    let mut done = |path: PathBuf, res: Result<()>| match res {
        Ok(()) => report.orphan_folders_removed.push(path),
        Err(e) => log(ctx, &format!("gc: orphan {}: {e}", path.display())),
    };

    for (id, path) in entries(&ctx.home.clones_dir()) {
        // Store and in-flight set are read under the store lock, as `clones::reserve` writes them.
        let owned = {
            let store = ctx.store();
            store.get_clone(&id).map(|r| r.is_some() || in_flight(ctx, &id))
        };
        match owned {
            Ok(false) => {}
            Ok(true) => continue,
            Err(e) => {
                log(ctx, &format!("gc: orphan check for {}: {e}", path.display()));
                continue;
            }
        }
        let res = remove_data_dir(ctx, path.clone()).await;
        done(path, res);
    }

    for (name, path) in entries(&ctx.home.sockets_dir()) {
        let res = if name.starts_with("build-") {
            // Holding the build set keeps a new build from taking its guard and reusing this folder.
            let building = ctx.building.lock().unwrap_or_else(|e| e.into_inner());
            if !building.is_empty() {
                continue;
            }
            remove_path(&path)
        } else {
            // Under the store lock so a concurrent create cannot claim this id meanwhile.
            let store = ctx.store();
            match store.get_clone(&name) {
                Ok(Some(_)) => continue,
                Ok(None) if in_flight(ctx, &name) => continue,
                Ok(None) => remove_path(&path),
                Err(e) => Err(e),
            }
        };
        done(path, res);
    }

    for (name, path) in entries(&ctx.home.templates_dir()) {
        // Leftovers are `.<name>.building-<ts>` and `.<name>.old-<ts>`; template names have no dots.
        let Some(rest) = name.strip_prefix('.') else { continue };
        let leftover = match rest.split_once('.') {
            Some((tpl, suffix)) if suffix.starts_with("building-") => {
                !ctx.building.lock().unwrap_or_else(|e| e.into_inner()).contains(tpl)
            }
            Some((_, suffix)) => suffix.starts_with("old-"),
            None => false,
        };
        if leftover {
            let res = remove_data_dir(ctx, path.clone()).await;
            done(path, res);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The daemon runs these on a multi-thread runtime, so they must be Send.
    #[allow(dead_code)]
    fn futures_are_send(ctx: &'static Ctx, arc: Arc<Ctx>) {
        fn send<T: Send>(_: T) {}
        send(reap_expired(ctx, 0));
        send(reconcile(arc.clone(), 0));
        send(destroy_after_parent_exit(arc, String::new()));
    }
}
