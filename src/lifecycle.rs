use crate::clones::{
    destroy_for, install_for, mark, restart_or_undo, sleep, stop_and_remove, suspect_warning,
};
use crate::ctx::{Ctx, InFlightOp};
use crate::error::{ErrorCode, MitosisError, Result};
use crate::events::{self, reason};
use crate::names::{is_clone_id, random_hex};
use crate::pg::{control_state, psql, running_server, stop, ControlState, RunningPg};
use crate::proc_util::{has_exited, is_alive, process_start_time};
use crate::protocol::{GcFailure, GcReport};
use crate::state::{CloneRow, CloneStatus};
use crate::templates::{
    folder_stem, is_build_socket_name, now, parse_gen_dir, parse_leftover, Leftover,
};
#[cfg(target_os = "macos")]
use nix::errno::Errno;
#[cfg(target_os = "macos")]
use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Allowed difference, in seconds, between a recorded and a measured process start time.
const START_SLACK: u64 = 1;
/// How long a parent watcher keeps retrying a clone that is still starting (a slow create).
const STARTING_WAIT: Duration = Duration::from_secs(60);
const DESTROY_RETRY_DELAY: Duration = Duration::from_millis(500);
/// How often a parent watcher wakes to check that its clone still exists.
#[cfg(target_os = "macos")]
const WATCH_RECHECK: Duration = Duration::from_secs(30);
/// gc drops a branch template no clone uses after this long without a `clone --branch` for it.
const BRANCH_UNUSED_SECS: i64 = 14 * 86_400;

fn internal(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, msg)
}

/// Appends one line to `logs/daemon.log`; lifecycle work has no caller to report errors to.
pub(crate) fn log(ctx: &Ctx, msg: &str) {
    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(ctx.home.logs_dir().join("daemon.log"))
    {
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

/// True while the clone's recorded postmaster still runs: same pid, exactly the same kernel start
/// time (both recorded from the kernel at create). A dead child is reaped, not counted.
fn postmaster_running(row: &CloneRow) -> bool {
    match (row.pg_pid, row.pg_start_time) {
        (Some(pid), Some(t)) => !has_exited(pid) && process_start_time(pid) == Some(t),
        _ => false,
    }
}

fn failed(report: &mut GcReport, item: impl Into<String>, e: &MitosisError) {
    report.failed.push(GcFailure {
        item: item.into(),
        error: e.message.clone(),
    });
}

fn in_flight(ctx: &Ctx, id: &str) -> bool {
    ctx.in_flight
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains_key(id)
}

/// A name's port and password are kept this long after its last create.
const RESERVATION_KEEP_SECS: i64 = 30 * 24 * 3600;

/// Destroys Ready and Sleeping clones whose TTL has run out; returns the ids destroyed.
pub async fn reap_expired(ctx: &Ctx, now: i64) -> Result<Vec<String>> {
    let rows = ctx.store().list_clones()?;
    let mut reaped = Vec::new();
    // Creating/Starting rows belong to an in-flight create; the next pass gets them.
    for row in rows.iter().filter(|r| {
        matches!(r.status, CloneStatus::Ready | CloneStatus::Sleeping)
            && !r.protected
            && r.expires_at <= now
    }) {
        if in_flight(ctx, &row.id) {
            continue;
        }
        match destroy_for(ctx, &row.id, reason::TTL).await {
            Ok(id) => reaped.push(id),
            Err(e) if e.code == ErrorCode::CloneNotFound => {}
            Err(e) => log(ctx, &format!("reaper: clone {}: {e}", row.id)),
        }
    }
    Ok(reaped)
}

/// Client backends other than this check's own session, from `pg_stat_activity`: the users of
/// the direct port, whom the router cannot see. Blocking work runs off the runtime.
pub(crate) async fn client_backends(ctx: &Ctx, row: &CloneRow) -> Result<usize> {
    let install = install_for(ctx, &row.dir).ok_or_else(|| {
        internal(format!(
            "no Postgres install found for {}",
            row.dir.display()
        ))
    })?;
    let (sock, port, db) = (row.socket_dir.clone(), row.port, row.database.clone());
    let out = tokio::task::spawn_blocking(move || {
        psql(
            &install,
            &sock,
            port,
            &db,
            "select count(*) from pg_stat_activity \
             where backend_type = 'client backend' and pid <> pg_backend_pid()",
        )
    })
    .await
    .map_err(|e| internal(format!("background task failed: {e}")))??;
    out.trim()
        .parse()
        .map_err(|_| internal(format!("unexpected pg_stat_activity count {out:?}")))
}

/// Ready and Sleeping clones (no operation running) with no connections right now, each with
/// the time its idle spell began: the latest of its last router activity, the last time a check
/// saw a client backend, its creation and this daemon's start. A Ready clone with a client
/// backend counts as active at `now` and is left out. Clones are read from the store, so router
/// entries of clones that are gone play no part.
pub(crate) async fn idle_clones(ctx: &Ctx, now: i64) -> Result<Vec<(CloneRow, i64)>> {
    // From the wall clock, not `now`: a caller may pass a later `now` to look ahead.
    let started = crate::templates::now()
        .saturating_sub(i64::try_from(ctx.started_at.elapsed().as_secs()).unwrap_or(0));
    let rows = ctx.store().list_clones()?;
    let mut idle = Vec::new();
    for row in rows
        .into_iter()
        .filter(|r| matches!(r.status, CloneStatus::Ready | CloneStatus::Sleeping))
    {
        if in_flight(ctx, &row.id) {
            continue;
        }
        let (conns, last) = {
            let r = ctx.router();
            (r.open_conns(&row.id), r.last_active(&row.id))
        };
        if conns > 0 {
            continue;
        }
        if row.status == CloneStatus::Ready {
            match client_backends(ctx, &row).await {
                Ok(0) => {}
                Ok(_) => {
                    ctx.router().last_active.insert(row.id.clone(), now);
                    continue;
                }
                Err(e) => {
                    // A crashed server fails the check every pass until gc recovers it: say it once
                    // there, not here every 30 s.
                    if postmaster_running(&row) {
                        log(ctx, &format!("idle check of clone {}: {e}", row.id));
                    }
                    continue;
                }
            }
        }
        let since = last.unwrap_or(0).max(row.created_at).max(started);
        idle.push((row, since));
    }
    Ok(idle)
}

/// The reaper's idle pass: destroys unnamed, unprotected clones idle for `idle_reap` (reason
/// `idle`), then puts Ready clones idle for `idle_sleep` to sleep. Sleeping needs the router
/// listening (it is what wakes them). Returns `(slept, destroyed)` ids.
pub async fn idle_pass(ctx: &Ctx, now: i64) -> Result<(Vec<String>, Vec<String>)> {
    let cfg = ctx.config();
    let sleep_after = cfg.idle_sleep.filter(|_| crate::router::listening(ctx));
    let (mut slept, mut reaped) = (Vec::new(), Vec::new());
    if sleep_after.is_none() && cfg.idle_reap.is_none() {
        return Ok((slept, reaped));
    }
    let longer = |idle: i64, d: Option<Duration>| {
        d.is_some_and(|d| idle >= i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
    };
    for (row, since) in idle_clones(ctx, now).await? {
        let idle = now.saturating_sub(since);
        if row.name.is_none() && !row.protected && longer(idle, cfg.idle_reap) {
            match destroy_for(ctx, &row.id, reason::IDLE).await {
                Ok(id) => reaped.push(id),
                Err(e) if e.code == ErrorCode::CloneNotFound => {}
                Err(e) => log(ctx, &format!("idle reap: clone {}: {e}", row.id)),
            }
        } else if row.status == CloneStatus::Ready && longer(idle, sleep_after) {
            match sleep(ctx, &row.id, reason::IDLE).await {
                Ok(()) => slept.push(row.id),
                Err(e) => log(ctx, &format!("idle sleep: clone {}: {e}", row.id)),
            }
        }
    }
    Ok((slept, reaped))
}

/// Watches `parent_pid` on a thread; when it exits, destroys the clone on the current tokio runtime.
/// The thread ends early once the clone is gone or being destroyed some other way.
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
        Err(e) => {
            return log(
                &ctx,
                &format!("parent watch for clone {clone_id}: no tokio runtime: {e}"),
            )
        }
    };
    if !ctx
        .watching
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(clone_id.clone())
    {
        return;
    }
    let watch = Watch { ctx, id: clone_id };
    std::thread::spawn(move || {
        if wait_for_parent_exit(&watch, parent_pid, parent_start_time) {
            // If the runtime is gone the future is dropped, and `watch` with it.
            handle.spawn(destroy_after_parent_exit(watch));
        }
    });
}

/// One parent watcher; leaves `Ctx::watching` when dropped, on every exit path.
struct Watch {
    ctx: Arc<Ctx>,
    id: String,
}

impl Drop for Watch {
    fn drop(&mut self) {
        self.ctx
            .watching
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

/// False once the clone is gone or already being torn down, so its watcher can stop.
fn still_watched(ctx: &Ctx, id: &str) -> bool {
    if ctx
        .in_flight
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(id)
        == Some(&InFlightOp::Destroy)
    {
        return false;
    }
    match ctx.store().get_clone(id) {
        // `get_clone` also matches names; a row with another id means ours is gone.
        Ok(Some(row)) => row.id == id && row.status != CloneStatus::Stopping,
        Ok(None) => false,
        Err(_) => true, // keep watching; the next check may succeed
    }
}

#[cfg(target_os = "macos")]
/// Registers a NOTE_EXIT watch on `pid`; `None` when the pid is already gone.
fn register_exit(pid: i32) -> nix::Result<Option<Kqueue>> {
    let kq = Kqueue::new()?;
    let ev = KEvent::new(
        pid as usize,
        EventFilter::EVFILT_PROC,
        EvFlags::EV_ADD | EvFlags::EV_ONESHOT,
        FilterFlag::NOTE_EXIT,
        0,
        0,
    );
    // An empty event list makes kevent only register, reporting failures as its own error.
    match kq.kevent(&[ev], &mut [], None) {
        Err(Errno::ESRCH) => Ok(None),
        Err(e) => Err(e),
        Ok(_) => Ok(Some(kq)),
    }
}

/// Blocks until the parent exits (true), or until the clone no longer needs watching (false).
/// Linux has no kqueue: this polls once a second.
#[cfg(not(target_os = "macos"))]
fn wait_for_parent_exit(watch: &Watch, pid: i32, start_time: Option<u64>) -> bool {
    loop {
        if !parent_alive(pid, start_time) {
            return true;
        }
        if !still_watched(&watch.ctx, &watch.id) {
            return false;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Blocks until the parent exits (true), or until the clone no longer needs watching (false).
/// Uses kqueue, waking every `WATCH_RECHECK`; polls once a second if kqueue is unavailable.
#[cfg(target_os = "macos")]
fn wait_for_parent_exit(watch: &Watch, pid: i32, start_time: Option<u64>) -> bool {
    let mut kq = match register_exit(pid) {
        Ok(None) => return true,
        Ok(Some(kq)) => Some(kq),
        Err(e) => {
            log(
                &watch.ctx,
                &format!(
                    "parent watch for clone {}: kqueue failed ({e}); polling",
                    watch.id
                ),
            );
            None
        }
    };
    let recheck = libc::timespec {
        tv_sec: WATCH_RECHECK.as_secs() as libc::time_t,
        tv_nsec: 0,
    };
    let mut out = [KEvent::new(
        0,
        EventFilter::EVFILT_PROC,
        EvFlags::empty(),
        FilterFlag::empty(),
        0,
        0,
    )];
    loop {
        // Also covers an exit just before registering, and a pid reused by another process.
        if !parent_alive(pid, start_time) {
            return true;
        }
        if !still_watched(&watch.ctx, &watch.id) {
            return false;
        }
        match &kq {
            Some(q) => match q.kevent(&[], &mut out, Some(recheck)) {
                Ok(n) if n > 0 => return true,
                Ok(_) | Err(Errno::EINTR) => {}
                Err(e) => {
                    log(
                        &watch.ctx,
                        &format!(
                            "parent watch for clone {}: kevent failed ({e}); polling",
                            watch.id
                        ),
                    );
                    kq = None;
                }
            },
            None => std::thread::sleep(Duration::from_secs(1)),
        }
    }
}

async fn destroy_after_parent_exit(watch: Watch) {
    let (ctx, id) = (&watch.ctx, &watch.id);
    let deadline = tokio::time::Instant::now() + STARTING_WAIT;
    loop {
        match destroy_for(ctx, id, reason::PARENT).await {
            Ok(_) => break,
            Err(e) if e.code == ErrorCode::CloneNotFound => break,
            // destroy refuses a clone whose create has not finished; a slow create can take a while.
            Err(e)
                if e.code == ErrorCode::InvalidRequest
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(DESTROY_RETRY_DELAY).await
            }
            Err(e) => {
                log(
                    ctx,
                    &format!("parent of clone {id} exited, but destroy failed: {e}"),
                );
                break;
            }
        }
    }
}

/// Brings the store and the home folder back in line after a crash; safe while requests are served.
/// Clones with an in-flight create and templates with a running build are left alone.
/// A failure on one entry is logged to `logs/daemon.log` and the rest still run. Sleeping clones
/// that nothing would wake (router not listening) are woken at the end, within the budget.
pub async fn reconcile(ctx: Arc<Ctx>, now: i64) -> Result<GcReport> {
    let (mut report, to_wake) = reconcile_deferred(ctx.clone(), now).await?;
    report.warnings.extend(wake_sleepers(&ctx, to_wake).await);
    Ok(report)
}

/// `reconcile` without the wakes: also returns the sleeping clones to wake, for the daemon's
/// startup gc, which hands them to `wake_sleepers` on a task once its socket accepts.
pub async fn reconcile_deferred(ctx: Arc<Ctx>, now: i64) -> Result<(GcReport, Vec<String>)> {
    let mut report = GcReport::default();
    let mut to_wake = Vec::new();
    let ids: Vec<String> = ctx
        .store()
        .list_clones()?
        .into_iter()
        .map(|r| r.id)
        .collect();
    for id in ids {
        if in_flight(&ctx, &id) {
            continue;
        }
        // Re-read after the in-flight check: a create that just finished shows its final status.
        let row = match ctx.store().get_clone(&id)? {
            Some(row) if row.id == id => row,
            _ => continue,
        };
        if let Err(e) = reconcile_clone(&ctx, &row, now, &mut report, &mut to_wake).await {
            log(&ctx, &format!("gc: clone {id}: {e}"));
            failed(&mut report, id, &e);
        }
    }
    // Before the orphan pass, which then removes the dropped templates' generation folders.
    drop_branch_templates(&ctx, now, &mut report).await;
    remove_orphans(&ctx, &mut report).await;
    drop_unused_reservations(&ctx, now);
    events::emit(
        &ctx,
        events::GC,
        None,
        None,
        serde_json::json!({
            "adopted": report.adopted.len(),
            "destroyed": report.destroyed.len(),
            "orphan_folders_removed": report.orphan_folders_removed.len(),
            "records_dropped": report.records_dropped.len(),
            "failed": report.failed.len(),
        }),
    );
    Ok((report, to_wake))
}

/// Wakes the sleeping clones `ids` (from `reconcile_deferred`) one by one, most recently active
/// first, while the router is not listening and the budget has room for one more running server;
/// returns the warnings (also logged). The rest stay asleep with one warning naming them.
pub async fn wake_sleepers(ctx: &Arc<Ctx>, ids: Vec<String>) -> Vec<String> {
    let mut warnings = Vec::new();
    let mut rows: Vec<(CloneRow, std::time::SystemTime)> = Vec::new();
    for id in ids {
        let row = match ctx.store().get_clone(&id) {
            Ok(Some(row)) if row.status == CloneStatus::Sleeping => row,
            _ => continue,
        };
        // A data folder's mtime moves when its server stops (postmaster.pid goes): the sleep
        // time, which follows the clone's last activity.
        let dir = row.dir.clone();
        let stopped = tokio::task::spawn_blocking(move || std::fs::metadata(dir)?.modified())
            .await
            .ok()
            .and_then(|r| r.ok())
            .unwrap_or(std::time::UNIX_EPOCH);
        rows.push((row, stopped));
    }
    rows.sort_by_key(|(_, stopped)| std::cmp::Reverse(*stopped));
    let mut left: Vec<String> = Vec::new();
    let mut why = String::new();
    for (row, _) in rows {
        let label = row.name.clone().unwrap_or_else(|| row.id.clone());
        if !left.is_empty() {
            left.push(label);
            continue;
        }
        // The router came up meanwhile (its bind retry): a connection wakes the rest.
        if crate::router::listening(ctx) {
            break;
        }
        match crate::budget::wake_blocked(ctx).await {
            Ok(None) => {}
            Ok(Some(limit)) => why = format!("waking more would exceed {limit}"),
            Err(e) => why = format!("the budget check failed: {e}"),
        }
        if !why.is_empty() {
            left.push(label);
            continue;
        }
        match crate::clones::wake(ctx, &row.id).await {
            Ok((woken, ws)) => {
                let suspect = woken.suspect.then(|| suspect_warning(&woken));
                for w in ws.into_iter().chain(suspect) {
                    log(ctx, &format!("gc: clone {}: {w}", row.id));
                    warnings.push(w);
                }
            }
            Err(e) => {
                let w = format!(
                    "clone {label} sleeps and could not be started ({e}); mitosis wake {label} \
                     starts it"
                );
                log(ctx, &format!("gc: {w}"));
                warnings.push(w);
            }
        }
    }
    if !left.is_empty() {
        let w = format!(
            "sleeping clones left asleep ({why}): {}; mitosis wake <clone> (or mitosis env \
             <clone> --direct) starts one",
            left.join(", ")
        );
        log(ctx, &format!("gc: {w}"));
        warnings.push(w);
    }
    warnings
}

/// True when git says `branch` does not exist in the repository of `dir` (`rev-parse --verify`
/// exits 1); false when it exists or git cannot tell. Blocking.
fn branch_deleted(dir: &Path, branch: &str) -> bool {
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

/// Drops the rows of branch templates no clone uses whose branch is gone from the base's
/// repository or that were not used for `BRANCH_UNUSED_SECS`; logs, never fails.
async fn drop_branch_templates(ctx: &Ctx, now: i64, report: &mut GcReport) {
    let rows = match ctx.store().list_templates() {
        Ok(ts) => ts.into_iter().filter(|t| t.base.is_some()),
        Err(e) => return log(ctx, &format!("gc: list templates: {e}")),
    };
    for t in rows {
        let (Some(base), Some(branch)) = (t.base.clone(), t.branch.clone()) else {
            continue;
        };
        let unused = t
            .last_used
            .is_none_or(|u| now.saturating_sub(u) >= BRANCH_UNUSED_SECS);
        let repo = ctx
            .store()
            .get_template(&base)
            .ok()
            .flatten()
            .and_then(|b| b.migrate)
            .map(|m| m.cwd);
        let gone = match repo.filter(|_| !unused) {
            Some(dir) => tokio::task::spawn_blocking(move || branch_deleted(&dir, &branch))
                .await
                .unwrap_or(false),
            None => false,
        };
        if !unused && !gone {
            continue;
        }
        // Under the write guard: no clone creation is between reading the template and cloning it.
        let _swap = ctx.template_swap.write().await;
        if ctx
            .building
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&t.name)
        {
            continue;
        }
        // Re-read under the guard: a `clone --branch` may have used it since the first read.
        let fresh = match ctx.store().get_template(&t.name) {
            Ok(Some(f)) => f,
            Ok(None) => continue,
            Err(e) => {
                log(ctx, &format!("gc: drop template {}: {e}", t.name));
                failed(report, t.name.clone(), &e);
                continue;
            }
        };
        let unused = fresh
            .last_used
            .is_none_or(|u| now.saturating_sub(u) >= BRANCH_UNUSED_SECS);
        if !unused && !gone {
            continue;
        }
        let res = {
            let store = ctx.store();
            match store.list_clones() {
                Ok(cs) if cs.iter().any(|c| c.template == t.name) => continue,
                Ok(_) => store.delete_template(&t.name),
                Err(e) => Err(e),
            }
        };
        match res {
            Ok(()) => {
                let why = if gone { "branch deleted" } else { "unused" };
                log(ctx, &format!("gc: dropped template {} ({why})", t.name));
                let build_log = crate::templates::build_log_path(&ctx.home, &t.name);
                // `remove_path` ignores NotFound and names the path in its error.
                if let Ok(Err(e)) =
                    tokio::task::spawn_blocking(move || remove_path(&build_log)).await
                {
                    log(ctx, &format!("gc: {e}"));
                }
                events::emit(
                    ctx,
                    events::TEMPLATE_PRUNED,
                    None,
                    Some(&t.name),
                    serde_json::json!({ "dropped": true, "cause": why }),
                );
                report.records_dropped.push(format!("template {}", t.name));
            }
            Err(e) => {
                log(ctx, &format!("gc: drop template {}: {e}", t.name));
                failed(report, t.name.clone(), &e);
            }
        }
    }
}

/// Drops name reservations unused for 30 days whose name has no clone; logs, never fails. Run by
/// gc and, for a long-running daemon, once a day by the reaper.
pub fn drop_unused_reservations(ctx: &Ctx, now: i64) {
    match ctx
        .store()
        .drop_stale_reservations(now.saturating_sub(RESERVATION_KEEP_SECS))
    {
        Ok(names) if !names.is_empty() => log(
            ctx,
            &format!("gc: dropped unused reservations: {}", names.join(", ")),
        ),
        Err(e) => log(ctx, &format!("gc: reservations: {e}")),
        _ => {}
    }
}

async fn reconcile_clone(
    ctx: &Arc<Ctx>,
    row: &CloneRow,
    now: i64,
    report: &mut GcReport,
    to_wake: &mut Vec<String>,
) -> Result<()> {
    let dir = ctx.home.clones_dir().join(&row.id);
    if !dir.exists() {
        // A destroy that started since the in-flight check owns it now.
        let Ok(_mark) = mark(ctx, &row.id, InFlightOp::Destroy) else {
            return Ok(());
        };
        stop_and_remove(ctx, row, reason::GC).await?; // also clears its socket folder and log
        report.records_dropped.push(row.id.clone());
        return Ok(());
    }
    if row.status == CloneStatus::Sleeping {
        return reconcile_sleeping(ctx, row, now, report, to_wake).await;
    }
    let running = row.status == CloneStatus::Ready && postmaster_running(row);
    // Whether the machine rebooted or the server just died (a full disk, a manual stop, a
    // logout), a clone someone may want back is restarted, never destroyed outright.
    let kept = row.name.is_some() || row.protected || row.parent_pid.is_none();
    if row.status == CloneStatus::Ready && !running && kept {
        return recover_dead(ctx, row, now, report).await;
    }
    let keep = running
        && row.expires_at > now
        && row
            .parent_pid
            .is_none_or(|p| parent_alive(p, row.parent_start_time));
    if !keep {
        let Ok(_mark) = mark(ctx, &row.id, InFlightOp::Destroy) else {
            return Ok(());
        };
        // Stops only a server identified exactly (recorded pid + start time, or cwd in `dir`).
        stop_and_remove(ctx, row, reason::GC).await?;
        report.destroyed.push(row.id.clone());
        return Ok(());
    }
    report.adopted.push(row.id.clone());
    if let Some(pid) = row.parent_pid {
        watch_parent(ctx.clone(), row.id.clone(), pid);
    }
    Ok(())
}

/// A sleeping clone keeps sleeping under the TTL and parent rules of a running one. A server
/// left running by a wake or sleep that crashed half-way is stopped, so Sleeping means stopped.
/// While the router is not listening (off, or its port taken) nothing would wake it on connect,
/// so it goes on `to_wake` (see `wake_sleepers`).
async fn reconcile_sleeping(
    ctx: &Arc<Ctx>,
    row: &CloneRow,
    now: i64,
    report: &mut GcReport,
    to_wake: &mut Vec<String>,
) -> Result<()> {
    let parent_gone = row
        .parent_pid
        .is_some_and(|p| !parent_alive(p, row.parent_start_time));
    if parent_gone || row.expires_at <= now {
        let Ok(_mark) = mark(ctx, &row.id, InFlightOp::Destroy) else {
            return Ok(());
        };
        stop_and_remove(ctx, row, reason::GC).await?;
        report.destroyed.push(row.id.clone());
        return Ok(());
    }
    if postmaster_running(row) {
        let Ok(_mark) = mark(ctx, &row.id, InFlightOp::Create) else {
            return Ok(());
        };
        let install = install_for(ctx, &row.dir).ok_or_else(|| {
            internal(format!(
                "no Postgres install found for {}",
                row.dir.display()
            ))
        })?;
        let expected = row
            .pg_pid
            .zip(row.pg_start_time)
            .map(|(pid, start_time)| RunningPg { pid, start_time });
        let (dir, t) = (row.dir.clone(), ctx.config().stop_timeout);
        tokio::task::spawn_blocking(move || stop(&install, &dir, expected, t))
            .await
            .map_err(|e| internal(format!("background task failed: {e}")))??;
        let mut row = row.clone();
        (row.pg_pid, row.pg_start_time) = (None, None);
        ctx.store().update_clone(&row)?;
    }
    if !crate::router::listening(ctx) {
        to_wake.push(row.id.clone());
    }
    report.adopted.push(row.id.clone());
    if let Some(pid) = row.parent_pid {
        watch_parent(ctx.clone(), row.id.clone(), pid);
    }
    Ok(())
}

/// True when a non-durable clone's cluster did not shut down cleanly (fsync was off, so recent
/// writes may be lost). Durable clones are never unclean in this sense. The server must be down.
pub(crate) async fn unclean(ctx: &Ctx, row: &CloneRow) -> Result<bool> {
    if row.durable {
        return Ok(false);
    }
    let install = install_for(ctx, &row.dir).ok_or_else(|| {
        internal(format!(
            "no Postgres install found for {}",
            row.dir.display()
        ))
    })?;
    let dir = row.dir.clone();
    let state = tokio::task::spawn_blocking(move || control_state(&install, &dir))
        .await
        .map_err(|e| internal(format!("background task failed: {e}")))??;
    Ok(state != ControlState::ShutDown)
}

/// Records that the clone may hold lost or corrupt writes.
pub(crate) fn mark_suspect(ctx: &Ctx, row: &mut CloneRow) -> Result<()> {
    row.suspect = true;
    ctx.store().update_clone(row)?;
    events::emit_clone(
        ctx,
        events::CLONE_SUSPECT,
        row,
        serde_json::json!({ "cause": "unclean shutdown with fsync off" }),
    );
    Ok(())
}

/// Destroys a clone gc decided to drop, unless another operation owns it now.
async fn drop_clone(ctx: &Ctx, row: &CloneRow, report: &mut GcReport) -> Result<()> {
    let Ok(_mark) = mark(ctx, &row.id, InFlightOp::Destroy) else {
        return Ok(());
    };
    stop_and_remove(ctx, row, reason::RECOVERY).await?;
    report.destroyed.push(row.id.clone());
    Ok(())
}

/// A named, protected or parentless Ready clone whose server is gone (a reboot or a crash). With
/// the router listening it is marked Sleeping (the router or `clone --name` wakes it), else
/// restarted as in v0.2; a non-durable one that did not shut down cleanly is suspect (fsync was
/// off), and a suspect clone without a name is destroyed instead, as is an expired one or one
/// whose parent exited.
async fn recover_dead(
    ctx: &Arc<Ctx>,
    row: &CloneRow,
    now: i64,
    report: &mut GcReport,
) -> Result<()> {
    let parent_gone = row
        .parent_pid
        .is_some_and(|p| !parent_alive(p, row.parent_start_time));
    if parent_gone || row.expires_at <= now {
        return drop_clone(ctx, row, report).await;
    }
    let Ok(mark) = mark(ctx, &row.id, InFlightOp::Create) else {
        return Ok(());
    };
    let mut row = row.clone();
    if unclean(ctx, &row).await? {
        if row.name.is_none() {
            drop(mark);
            log(
                ctx,
                &format!(
                    "gc: clone {} did not shut down cleanly with fsync off; destroyed",
                    row.id
                ),
            );
            return drop_clone(ctx, &row, report).await;
        }
        mark_suspect(ctx, &mut row)?;
    }
    if crate::router::listening(ctx) {
        // Not started now: the first router connection (or `clone --name`) wakes it.
        row.status = CloneStatus::Sleeping;
        (row.pg_pid, row.pg_start_time) = (None, None);
        ctx.store().update_clone(&row)?;
        events::emit_clone(
            ctx,
            events::RECOVERY,
            &row,
            serde_json::json!({ "restarted": false, "sleeping": true, "suspect": row.suspect }),
        );
        report.adopted.push(row.id.clone());
        if let Some(w) = row.suspect.then(|| suspect_warning(&row)) {
            log(ctx, &format!("gc: clone {}: {w}", row.id));
            report.warnings.push(w);
        }
        if let Some(pid) = row.parent_pid {
            watch_parent(ctx.clone(), row.id.clone(), pid);
        }
        return Ok(());
    }
    let old_port = row.port;
    match restart_or_undo(ctx, &mut row).await {
        Ok(warnings) => {
            events::emit_clone(
                ctx,
                events::RECOVERY,
                &row,
                serde_json::json!({
                    "restarted": true,
                    "suspect": row.suspect,
                    "port_moved": row.port != old_port,
                }),
            );
            report.adopted.push(row.id.clone());
            let suspect = row.suspect.then(|| suspect_warning(&row));
            for w in warnings.into_iter().chain(suspect) {
                log(ctx, &format!("gc: clone {}: {w}", row.id));
                report.warnings.push(w);
            }
            if let Some(pid) = row.parent_pid {
                watch_parent(ctx.clone(), row.id.clone(), pid);
            }
            Ok(())
        }
        // Kept, not destroyed: it may hold data someone wants. `restart_or_undo` put the old
        // identity back, so the next daemon start or gc tries again.
        Err(e) => Err(e),
    }
}

fn entries(dir: &Path) -> Vec<(String, PathBuf)> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    rd.flatten()
        .map(|e| (e.file_name().to_string_lossy().into_owned(), e.path()))
        .collect()
}

fn remove_path(path: &Path) -> Result<()> {
    let res = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    match res {
        Err(e) if e.kind() != ErrorKind::NotFound => {
            Err(internal(format!("remove {}: {e}", path.display())))
        }
        _ => Ok(()),
    }
}

/// Stops the Postgres serving `dir`, if any, then deletes the folder.
async fn remove_data_dir(ctx: &Ctx, dir: PathBuf) -> Result<()> {
    let install = install_for(ctx, &dir);
    let timeout = ctx.config().stop_timeout;
    let backend = ctx.backend.clone();
    tokio::task::spawn_blocking(move || {
        // No row, so the server (if any) is the process whose cwd is this folder.
        if running_server(&dir, None)?.is_some() {
            let install = install.ok_or_else(|| {
                internal(format!(
                    "no Postgres install found to stop the server in {}",
                    dir.display()
                ))
            })?;
            stop(&install, &dir, None, timeout)?;
        }
        // A stray file under a clone or generation name is not the back end's.
        if dir.is_dir() {
            backend.remove_dir(&dir)
        } else {
            remove_path(&dir)
        }
    })
    .await
    .map_err(|e| internal(format!("background task failed: {e}")))?
}

/// Leaves an entry gc does not recognise alone, with one log line.
fn foreign(ctx: &Ctx, path: &Path) {
    log(
        ctx,
        &format!(
            "gc: left {} alone: Mitosis did not create it",
            path.display()
        ),
    );
}

/// Step 6: folders in `clones/`, `sockets/` and `templates/` that nothing owns any more.
/// Only names Mitosis itself creates are touched; anything else is logged and left alone.
async fn remove_orphans(ctx: &Ctx, report: &mut GcReport) {
    let done = |report: &mut GcReport, path: PathBuf, res: Result<()>| match res {
        Ok(()) => report.orphan_folders_removed.push(path),
        Err(e) => {
            log(ctx, &format!("gc: orphan {}: {e}", path.display()));
            failed(report, path.display().to_string(), &e);
        }
    };

    for (id, path) in entries(&ctx.home.clones_dir()) {
        if !is_clone_id(&id) {
            foreign(ctx, &path);
            continue;
        }
        // Store and in-flight set are read under the store lock, as `clones::reserve` writes them.
        let owned = {
            let store = ctx.store();
            store
                .get_clone(&id)
                .map(|r| r.is_some() || in_flight(ctx, &id))
        };
        match owned {
            Ok(false) => {}
            Ok(true) => continue,
            Err(e) => {
                log(
                    ctx,
                    &format!("gc: orphan check for {}: {e}", path.display()),
                );
                continue;
            }
        }
        let res = remove_data_dir(ctx, path.clone()).await;
        done(report, path, res);
    }

    for (name, path) in entries(&ctx.home.sockets_dir()) {
        let res = if diff_tag(&name, "d", "").is_some() {
            continue; // a diff's, maybe running now; daemon start removes leftovers
        } else if is_build_socket_name(&name) {
            // Holding the build set keeps a new build from taking its guard and reusing this folder.
            let building = ctx.building.lock().unwrap_or_else(|e| e.into_inner());
            if !building.is_empty() {
                continue;
            }
            remove_path(&path)
        } else if is_clone_id(&name) {
            // Under the store lock so a concurrent create cannot claim this id meanwhile.
            let store = ctx.store();
            match store.get_clone(&name) {
                Ok(Some(_)) => continue,
                Ok(None) if in_flight(ctx, &name) => continue,
                Ok(None) => remove_path(&path),
                Err(e) => Err(e),
            }
        } else {
            foreign(ctx, &path);
            continue;
        };
        done(report, path, res);
    }

    // `logs/upgrade-*` work folders: an upgrade runs as a build, so with no build running they
    // are leftovers of a crash. The build set is held across the removal, as for build sockets.
    for (name, path) in entries(&ctx.home.logs_dir()) {
        if !name.starts_with("upgrade-") || !path.is_dir() {
            continue;
        }
        let building = ctx.building.lock().unwrap_or_else(|e| e.into_inner());
        if !building.is_empty() {
            break;
        }
        let res = remove_path(&path);
        drop(building);
        done(report, path, res);
    }

    let templates_dir = ctx.home.templates_dir();
    // Read guard: a commit renames a folder to `<name>@<gen>` and inserts its row under the write
    // guard, so a generation folder is never seen here without its row.
    let swap = ctx.template_swap.read().await;
    let gen_dirs = match generation_folders(ctx) {
        Ok(d) => d,
        Err(e) => {
            // Without the rows nothing in templates/ can be judged; leave it all for the next gc.
            log(ctx, &format!("gc: list generations: {e}"));
            failed(report, templates_dir.display().to_string(), &e);
            return;
        }
    };
    let mut leftovers = Vec::new();
    let mut rowless = Vec::new();
    for (file, path) in entries(&templates_dir) {
        if gen_dirs.contains(&file) {
            continue;
        }
        if diff_tag(&file, ".diff-", "").is_some() {
            continue; // a diff's, maybe running now; daemon start removes leftovers
        } else if diff_tag(&file, GC_TRASH, "").is_some() {
            // Moved aside by a gc that stopped before removing it.
            let res = remove_data_dir(ctx, path.clone()).await;
            done(report, path, res);
        } else if let Some(l) = parse_leftover(&file) {
            leftovers.push((l, path));
        } else if parse_gen_dir(&file).is_some() || migrated_rowless(ctx, &file, &path) {
            // A generation folder with no row: a commit that crashed, or a prune that did not
            // finish (also the plain-named folder of a v0.1 store's migrated generation).
            rowless.push(path);
        } else {
            foreign(ctx, &path);
        }
    }
    restore_old_templates(ctx, &templates_dir, &gen_dirs, &mut leftovers, report);
    drop(swap);
    for path in rowless {
        // A clone that read the row before a prune dropped it may still be copying the folder
        // under the read guard: the write guard waits for it, and the check is repeated there.
        // Only the rename is guarded; stopping a stray server can take a while and would stall
        // every commit and create.
        let moved = {
            let _swap = ctx.template_swap.write().await;
            let file = path.file_name().map(|f| f.to_string_lossy().into_owned());
            match generation_folders(ctx) {
                Ok(dirs) if file.as_ref().is_some_and(|f| dirs.contains(f)) => continue,
                Ok(_) => {
                    let trash = templates_dir.join(format!("{GC_TRASH}{}", random_hex(4)));
                    ctx.backend.rename_dir(&path, &trash).map(|()| trash).map_err(|e| {
                        internal(format!("move {} aside: {}", path.display(), e.message))
                    })
                }
                Err(e) => Err(e),
            }
        };
        let res = match moved {
            Ok(trash) => remove_data_dir(ctx, trash).await,
            Err(e) => Err(e),
        };
        done(report, path, res);
    }
    for (l, path) in leftovers {
        // A running build owns its own leftovers (its side folder, the folders its prune removes).
        let tpl = match &l {
            Leftover::Building(tpl) | Leftover::Old(tpl, _) => tpl,
        };
        // `tpl` is a folder stem: a branch template's build is known by its full name.
        let remove = !ctx
            .building
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|b| folder_stem(b) == *tpl);
        if remove {
            let res = remove_data_dir(ctx, path.clone()).await;
            done(report, path, res);
        }
    }
}

/// Prefix of a row-less generation folder gc moved aside to remove (`.gc-<8 hex>`).
const GC_TRASH: &str = ".gc-";

/// The file names of every generation folder the store knows (they all sit in `templates/`).
fn generation_folders(ctx: &Ctx) -> Result<HashSet<String>> {
    let store = ctx.store();
    let mut dirs = HashSet::new();
    for t in store.list_templates()? {
        dirs.extend(
            store
                .list_generations(&t.name)?
                .into_iter()
                .filter_map(|g| g.dir.file_name().map(|f| f.to_string_lossy().into_owned())),
        );
    }
    Ok(dirs)
}

/// True for `templates/<name>` named like a template that exists, holding a cluster, that no
/// generation row names (the caller checked): a v0.1 store's migrated generation 1 whose prune
/// crashed between dropping the row and moving the folder.
fn migrated_rowless(ctx: &Ctx, file: &str, path: &Path) -> bool {
    crate::names::validate_name("template", file).is_ok()
        && path.join("PG_VERSION").is_file()
        && ctx.store().get_template(file).ok().flatten().is_some()
}

/// The 8-hex tag of a name `diff` makes (`<prefix><tag><suffix>`), else `None`.
fn diff_tag<'a>(name: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    name.strip_prefix(prefix)
        .and_then(|r| r.strip_suffix(suffix))
        .filter(|t| t.len() == 8 && t.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
}

/// Removes what a diff left behind when the daemon crashed or stopped under it: its base folder
/// `templates/.diff-<tag>` (stopping the server in it first), `sockets/d<tag>` and
/// `logs/diff-<tag>.log`. Only at daemon start, before the socket binds: no diff runs then, and
/// nothing records the running ones. Logs, never fails; returns what it removed.
pub async fn remove_diff_leftovers(ctx: &Ctx) -> Vec<PathBuf> {
    let mut removed = Vec::new();
    let found = [
        (ctx.home.templates_dir(), ".diff-", ""),
        (ctx.home.sockets_dir(), "d", ""),
        (ctx.home.logs_dir(), "diff-", ".log"),
    ]
    .into_iter()
    .flat_map(|(dir, prefix, suffix)| {
        entries(&dir)
            .into_iter()
            .filter(move |(name, _)| diff_tag(name, prefix, suffix).is_some())
            .map(move |(_, path)| (path, prefix == ".diff-"))
    })
    .collect::<Vec<_>>();
    for (path, base) in found {
        let res = if base {
            remove_data_dir(ctx, path.clone()).await
        } else {
            remove_path(&path)
        };
        match res {
            Ok(()) => removed.push(path),
            Err(e) => log(ctx, &format!("diff leftover {}: {e}", path.display())),
        }
    }
    if !removed.is_empty() {
        log(ctx, &format!("removed {} diff leftover(s)", removed.len()));
    }
    removed
}

/// A v0.1 swap that crashed between its two renames leaves `.<name>.old-*` but no `templates/<name>`.
/// When an upgraded store still has a generation in `templates/<name>`, the newest such folder is
/// moved back (and dropped from `leftovers`) before the rest are removed. v0.2 commits never move
/// a live folder, so its `.old-` folders are only pruned generations, whose rows are gone.
fn restore_old_templates(
    ctx: &Ctx,
    templates_dir: &Path,
    gen_dirs: &HashSet<String>,
    leftovers: &mut Vec<(Leftover, PathBuf)>,
    report: &mut GcReport,
) {
    let mut newest: std::collections::HashMap<String, (i64, usize)> = Default::default();
    for (i, (l, _)) in leftovers.iter().enumerate() {
        if let Leftover::Old(name, ts) = l {
            let e = newest.entry(name.clone()).or_insert((*ts, i));
            if *ts > e.0 {
                *e = (*ts, i);
            }
        }
    }
    let mut handled = Vec::new();
    for (name, (_, i)) in newest {
        let final_dir = templates_dir.join(&name);
        // The build set is held across the rename, so no build can swap this template meanwhile.
        let building = ctx.building.lock().unwrap_or_else(|e| e.into_inner());
        if building.contains(&name) || final_dir.exists() || !gen_dirs.contains(&name) {
            continue;
        }
        let old = &leftovers[i].1;
        match ctx.backend.rename_dir(old, &final_dir) {
            Ok(()) => {
                drop(building);
                log(
                    ctx,
                    &format!("gc: restored template {name} from {}", old.display()),
                );
                handled.push(i);
            }
            Err(e) => {
                drop(building);
                let e = internal(format!(
                    "restore {} to {}: {e}",
                    old.display(),
                    final_dir.display()
                ));
                log(ctx, &format!("gc: {e}"));
                failed(report, old.display().to_string(), &e);
                handled.push(i); // keep it on disk: it may be the only copy
            }
        }
    }
    handled.sort_unstable_by(|a, b| b.cmp(a));
    for i in handled {
        leftovers.remove(i);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clones::destroy;
    use crate::paths::Home;

    fn tmp_ctx() -> (tempfile::TempDir, Arc<Ctx>) {
        // Under /tmp: short paths, on the default back end.
        let tmp = tempfile::Builder::new()
            .prefix("mt")
            .tempdir_in("/tmp")
            .unwrap();
        let ctx = Ctx::open(
            Home::at(tmp.path().to_path_buf()),
            crate::ctx::Config::default(),
        )
        .unwrap();
        (tmp, Arc::new(ctx))
    }

    /// Inserts a row and creates its (empty) clone folder; no Postgres involved.
    fn fake_clone(ctx: &Ctx, id: &str, status: CloneStatus) -> CloneRow {
        let t = now();
        let row = CloneRow {
            id: id.into(),
            name: None,
            template: "main".into(),
            dir: ctx.home.clones_dir().join(id),
            port: 54320,
            socket_dir: ctx.home.sockets_dir().join(id),
            password: "x".repeat(32),
            database: "app".into(),
            pg_pid: None,
            pg_start_time: None,
            parent_pid: None,
            parent_start_time: None,
            created_at: t,
            expires_at: t + 7200,
            durable: false,
            status,
            generation: 1,
            protected: false,
            suspect: false,
        };
        ctx.store().insert_clone(&row).unwrap();
        std::fs::create_dir(&row.dir).unwrap();
        row
    }

    #[tokio::test]
    async fn reconcile_skips_in_flight_clones() {
        let (_tmp, ctx) = tmp_ctx();
        let row = fake_clone(&ctx, "cb0550a", CloneStatus::Creating);
        let m = mark(&ctx, &row.id, InFlightOp::Create).unwrap();
        let report = reconcile(ctx.clone(), now()).await.unwrap();
        assert!(
            report.destroyed.is_empty() && report.orphan_folders_removed.is_empty(),
            "{report:?}"
        );
        assert!(row.dir.is_dir() && ctx.store().get_clone(&row.id).unwrap().is_some());

        drop(m); // the create "crashed": now it is debris
        let report = reconcile(ctx.clone(), now()).await.unwrap();
        assert_eq!(report.destroyed, vec![row.id.clone()]);
        assert!(!row.dir.exists() && ctx.store().get_clone(&row.id).unwrap().is_none());
    }

    #[tokio::test]
    async fn reap_and_destroy_respect_in_flight_ops() {
        let (_tmp, ctx) = tmp_ctx();
        let row = fake_clone(&ctx, "cb0550b", CloneStatus::Ready);
        {
            let _m = mark(&ctx, &row.id, InFlightOp::Destroy).unwrap();
            assert!(reap_expired(&ctx, now() + 10_000).await.unwrap().is_empty());
            assert_eq!(
                destroy(&ctx, &row.id).await.unwrap(),
                row.id,
                "a second destroy is a quiet success"
            );
            assert!(row.dir.is_dir(), "the running destroy owns the folder");
        }
        {
            let _m = mark(&ctx, &row.id, InFlightOp::Create).unwrap();
            assert_eq!(
                destroy(&ctx, &row.id).await.unwrap_err().code,
                ErrorCode::InvalidRequest
            );
        }
        assert!(
            mark(&ctx, &row.id, InFlightOp::Destroy).is_ok(),
            "marks are released on drop"
        );
        assert_eq!(
            reap_expired(&ctx, now() + 10_000).await.unwrap(),
            vec![row.id.clone()]
        );
        assert!(!row.dir.exists());
    }

    #[tokio::test]
    async fn destroy_during_wake_or_sleep_says_busy() {
        let (_tmp, ctx) = tmp_ctx();
        let row = fake_clone(&ctx, "cb0550c", CloneStatus::Sleeping);
        let _m = mark(&ctx, &row.id, InFlightOp::Create).unwrap(); // what a wake or sleep holds
        let e = destroy(&ctx, &row.id).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidRequest);
        assert!(
            e.message.contains("waking or going to sleep"),
            "{}",
            e.message
        );
        assert!(!e.message.contains("still starting"), "{}", e.message);
    }

    #[tokio::test]
    async fn destroy_forgets_router_activity() {
        let (_tmp, ctx) = tmp_ctx();
        let row = fake_clone(&ctx, "cb0550d", CloneStatus::Ready);
        ctx.router().last_active.insert(row.id.clone(), now());
        destroy(&ctx, &row.id).await.unwrap();
        assert_eq!(ctx.router().last_active(&row.id), None);
    }

    #[tokio::test]
    async fn idle_check_of_a_dead_server_is_not_logged() {
        let (_tmp, ctx) = tmp_ctx();
        let row = fake_clone(&ctx, "cb0550e", CloneStatus::Ready); // no server runs
        let idle = idle_clones(&ctx, now()).await.unwrap();
        assert!(
            idle.iter().all(|(r, _)| r.id != row.id),
            "a failed check is not idle"
        );
        let log =
            std::fs::read_to_string(ctx.home.logs_dir().join("daemon.log")).unwrap_or_default();
        assert!(!log.contains("idle check"), "{log}");
    }

    #[test]
    fn unused_reservations_are_dropped() {
        let (_tmp, ctx) = tmp_ctx();
        let t = now();
        let res = |name: &str, last_used: i64| crate::state::ReservationRow {
            name: name.into(),
            port: 54321,
            password: "x".repeat(32),
            template: "main".into(),
            last_used,
        };
        ctx.store()
            .upsert_reservation(&res("old", t - RESERVATION_KEEP_SECS - 10))
            .unwrap();
        ctx.store()
            .upsert_reservation(&res("recent", t - 10))
            .unwrap();
        drop_unused_reservations(&ctx, t);
        assert!(ctx.store().get_reservation("old").unwrap().is_none());
        assert!(ctx.store().get_reservation("recent").unwrap().is_some());
    }

    #[tokio::test]
    async fn diff_leftovers_go_at_daemon_start_only() {
        let (_tmp, ctx) = tmp_ctx();
        let home = &ctx.home;
        let base = home.templates_dir().join(".diff-0123abcd");
        let sock = home.sockets_dir().join("d0123abcd");
        let log = home.logs_dir().join("diff-0123abcd.log");
        let not_diff = home.templates_dir().join(".diff-notours");
        std::fs::create_dir(&base).unwrap();
        std::fs::create_dir(&sock).unwrap();
        std::fs::write(&log, "x").unwrap();
        std::fs::create_dir(&not_diff).unwrap();
        // gc may run beside a diff: it leaves them be, and does not call them foreign.
        reconcile(ctx.clone(), now()).await.unwrap();
        assert!(base.is_dir() && sock.is_dir() && log.is_file());
        let daemon_log = std::fs::read_to_string(home.logs_dir().join("daemon.log")).unwrap();
        assert!(!daemon_log.contains("0123abcd"), "{daemon_log}");

        let removed = remove_diff_leftovers(&ctx).await;
        assert_eq!(removed.len(), 3, "{removed:?}");
        assert!(!base.exists() && !sock.exists() && !log.exists());
        assert!(not_diff.is_dir(), "not a name diff makes");
    }

    #[tokio::test]
    async fn gc_removes_rowless_and_crash_leftover_folders() {
        let (_tmp, ctx) = tmp_ctx();
        let home = &ctx.home;
        let src = crate::protocol::TemplateSource::Seed {
            command: "true".into(),
            cwd: None,
            path: None,
        };
        ctx.store()
            .insert_template(&crate::state::TemplateRow::new("old", "app", src, None, 0))
            .unwrap();
        // A v0.1 store's migrated generation whose prune crashed after its row went.
        let migrated = home.templates_dir().join("old");
        std::fs::create_dir(&migrated).unwrap();
        std::fs::write(migrated.join("PG_VERSION"), "17\n").unwrap();
        let rowless = home.templates_dir().join("main@3");
        std::fs::create_dir(&rowless).unwrap();
        let trash = home.templates_dir().join(".gc-0123abcd");
        std::fs::create_dir(&trash).unwrap();
        let upgrade = home.logs_dir().join("upgrade-main-1700000000");
        std::fs::create_dir(&upgrade).unwrap();
        // A plain folder of no template, or with no cluster in it, stays.
        let foreign = home.templates_dir().join("notes");
        std::fs::create_dir(&foreign).unwrap();

        let report = reconcile(ctx.clone(), now()).await.unwrap();
        for p in [&migrated, &rowless, &trash, &upgrade] {
            assert!(!p.exists(), "{} left: {report:?}", p.display());
            assert!(report.orphan_folders_removed.contains(p), "{report:?}");
        }
        assert!(foreign.is_dir());
        let left: Vec<_> = entries(&home.templates_dir())
            .into_iter()
            .map(|(n, _)| n)
            .filter(|n| n.starts_with(GC_TRASH))
            .collect();
        assert!(left.is_empty(), "{left:?}");
    }

    // The daemon runs these on a multi-thread runtime, so they must be Send.
    #[allow(dead_code)]
    fn futures_are_send(ctx: &'static Ctx, arc: Arc<Ctx>) {
        fn send<T: Send>(_: T) {}
        send(reap_expired(ctx, 0));
        send(reconcile(arc.clone(), 0));
        send(idle_pass(ctx, 0));
        send(remove_diff_leftovers(ctx));
        send(destroy_after_parent_exit(Watch {
            ctx: arc,
            id: String::new(),
        }));
    }
}
