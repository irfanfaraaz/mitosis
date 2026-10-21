use crate::budget::admit;
use crate::clone_backend::free_space;
use crate::ctx::{Ctx, InFlightOp};
use crate::envfile;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::events::{self, reason};
use crate::lifecycle::log;
use crate::names::{new_clone_id, random_hex, validate_name};
use crate::pg::{
    check_extensions, find_major, newest, psql, read_pg_version, running_server, start, stop,
    wait_ready, PgInstall, RunningPg, StartOptions,
};
use crate::ports::pick_port;
use crate::proc_util::{is_alive, process_start_time, rss_bytes};
use crate::protocol::{CloneCreateArgs, CloneInfo, OnStale, PendingRestore};
use crate::scheduler;
use crate::state::{CloneRow, CloneStatus, GenerationRow, ReservationRow, TemplateRow};
use crate::templates::{age_secs, now};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Start attempts when the chosen port turns out to be taken.
pub(crate) const START_TRIES: usize = 3;

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
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| internal(format!("background task failed: {e}")))?
}

/// The URL straight to the clone's own port (the v0.2 URL).
pub fn direct_url(row: &CloneRow) -> String {
    format!(
        "postgres://postgres:{}@127.0.0.1:{}/{}",
        row.password,
        row.port,
        crate::templates::url_encode(&row.database)
    )
}

/// The URL through the router: the clone's name (else its id) as the database, so a named
/// clone's URL stays the same when its direct port moves.
pub fn router_url(ctx: &Ctx, row: &CloneRow) -> String {
    format!(
        "postgres://postgres:{}@127.0.0.1:{}/{}",
        row.password,
        ctx.config().router_port,
        row.name.as_deref().unwrap_or(&row.id)
    )
}

/// `CloneInfo.status`.
fn status_str(s: CloneStatus) -> &'static str {
    match s {
        CloneStatus::Creating | CloneStatus::Starting => "starting",
        CloneStatus::Ready => "ready",
        CloneStatus::Stopping => "stopping",
        CloneStatus::Sleeping => "sleeping",
    }
}

/// Why a direct port moved: the old one was taken.
const TAKEN: &str = "was taken";
/// Why a direct port moved: the reserved one is outside `port_range` now.
const OUT_OF_RANGE: &str = "is outside port_range";

/// The warning for a direct port that had to move from `old`; `why` is `TAKEN` or `OUT_OF_RANGE`.
fn port_moved(ctx: &Ctx, old: u16, why: &str) -> String {
    if ctx.config().router {
        format!("port {old} {why}; the direct URL changed, the router URL did not")
    } else {
        format!("port {old} {why}; the URL changed")
    }
}

/// `url` is the router URL while `router` is on (even while its port is taken), else the direct one.
pub fn to_info(ctx: &Ctx, row: &CloneRow, template_age_secs: u64) -> CloneInfo {
    let direct = direct_url(row);
    CloneInfo {
        id: row.id.clone(),
        name: row.name.clone(),
        template: row.template.clone(),
        url: if ctx.config().router {
            router_url(ctx, row)
        } else {
            direct.clone()
        },
        direct_url: direct,
        status: status_str(row.status).to_string(),
        branch: row.template.split_once('~').map(|(_, b)| b.to_string()),
        rebuilt: false,
        port: row.port,
        socket_dir: row.socket_dir.clone(),
        template_age_secs,
        created_at: row.created_at,
        expires_at: row.expires_at,
        durable: row.durable,
        parent_pid: row.parent_pid,
        rss_bytes: None,
        own_disk_bytes: None,
        generation: row.generation,
        stale: false,
        refreshing: false,
        existing: false,
        protected: row.protected,
        warnings: row
            .suspect
            .then(|| suspect_warning(row))
            .into_iter()
            .collect(),
        duration_ms: 0,
        waited_ms: 0,
        disk_warn: false,
        suspect: row.suspect,
    }
}

fn not_found(id: &str) -> MitosisError {
    MitosisError::new(ErrorCode::CloneNotFound, format!("clone {id} not found"))
        .with_hint("see existing clones with `mitosis list` (or the clone_list tool)")
}

/// The clone's row when it can take connections now; a sleeping clone is woken first.
pub async fn ensure_running(ctx: &Arc<Ctx>, id: &str) -> Result<CloneRow> {
    let row = ctx.store().get_clone(id)?.ok_or_else(|| not_found(id))?;
    let label = row.name.as_deref().unwrap_or(&row.id);
    match row.status {
        CloneStatus::Ready => Ok(row),
        CloneStatus::Sleeping => {
            let (row, warnings) = wake(ctx, &row.id).await?;
            for w in warnings {
                log(ctx, &format!("wake of clone {}: {w}", row.id));
            }
            Ok(row)
        }
        CloneStatus::Stopping => Err(invalid(format!("clone {label} is being destroyed"))),
        CloneStatus::Creating | CloneStatus::Starting => Err(invalid(format!(
            "clone {label} is still starting; try again in a moment"
        ))),
    }
}

/// The lock that one wake or sleep of clone `id` holds at a time.
fn clone_turn(ctx: &Ctx, id: &str) -> Arc<tokio::sync::Mutex<()>> {
    ctx.waking
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(id.to_string())
        .or_default()
        .clone()
}

/// The row of `id` (an id, else a name) and that row re-read under its wake/sleep lock.
async fn locked_row(
    ctx: &Ctx,
    id_or_name: &str,
) -> Result<(CloneRow, tokio::sync::OwnedMutexGuard<()>)> {
    let id = ctx
        .store()
        .get_clone(id_or_name)?
        .ok_or_else(|| not_found(id_or_name))?
        .id;
    let turn = clone_turn(ctx, &id).lock_owned().await;
    // A destroy, wake or sleep may have finished while this one waited.
    let row = ctx
        .store()
        .get_clone(&id)?
        .filter(|r| r.id == id)
        .ok_or_else(|| not_found(&id))?;
    Ok((row, turn))
}

/// Starts a sleeping clone: the suspect check of recovery, then Postgres on the same folder,
/// port (moved when taken, with the reservation and `.env` records following) and password.
/// One wake per clone at a time; a caller that waited on another's wake gets the woken row.
/// Returns the warnings of the start (a moved port).
pub async fn wake(ctx: &Arc<Ctx>, id_or_name: &str) -> Result<(CloneRow, Vec<String>)> {
    let (mut row, _turn) = locked_row(ctx, id_or_name).await?;
    let label = row.name.clone().unwrap_or_else(|| row.id.clone());
    match row.status {
        CloneStatus::Ready => return Ok((row, Vec::new())),
        CloneStatus::Sleeping => {}
        CloneStatus::Stopping => return Err(invalid(format!("clone {label} is being destroyed"))),
        CloneStatus::Creating | CloneStatus::Starting => {
            return Err(invalid(format!(
                "clone {label} is still starting; try again in a moment"
            )))
        }
    }
    let _mark = mark(ctx, &row.id, InFlightOp::Create)
        .map_err(|_| invalid(format!("clone {label} is busy; try again in a moment")))?;
    let started = Instant::now();
    let old_port = row.port;
    let res = async {
        if crate::lifecycle::unclean(ctx, &row).await? && !row.suspect {
            crate::lifecycle::mark_suspect(ctx, &mut row)?;
        }
        let warnings = restart_or_undo(ctx, &mut row).await?;
        row.status = CloneStatus::Ready;
        ctx.store().update_clone(&row)?;
        Ok::<_, MitosisError>(warnings)
    }
    .await;
    let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    match res {
        Ok(warnings) => {
            // The idle clock starts again now, also for a wake by hand.
            ctx.router().last_active.insert(row.id.clone(), now());
            events::emit_clone(
                ctx,
                events::CLONE_WOKE,
                &row,
                serde_json::json!({
                    "ok": true,
                    "duration_ms": ms,
                    "port_moved": row.port != old_port,
                    "suspect": row.suspect,
                }),
            );
            Ok((row, warnings))
        }
        Err(e) => {
            log(ctx, &format!("wake of clone {} failed: {e}", row.id));
            events::emit_clone(
                ctx,
                events::CLONE_WOKE,
                &row,
                serde_json::json!({ "ok": false, "duration_ms": ms, "error": e.code }),
            );
            Err(e)
        }
    }
}

/// `mitosis wake`: wakes the clone (a no-op when it runs) and returns its info.
pub async fn wake_info(ctx: &Arc<Ctx>, id_or_name: &str) -> Result<CloneInfo> {
    let (row, warnings) = wake(ctx, id_or_name).await?;
    let age = ctx
        .store()
        .current(&row.template)?
        .map_or(0, |(_, g)| age_secs(g.built_at, now()));
    let mut info = to_info(ctx, &row, age);
    info.warnings.extend(warnings);
    info.warnings.extend(crate::router::warning(ctx));
    Ok(info)
}

/// Stops a running clone cleanly and marks it `Sleeping` (folder, port, password and `.env`
/// records kept). Refuses while the router has a connection open to it; already sleeping is fine.
/// `why` is the `reason` of the `clone.slept` event.
pub async fn sleep(ctx: &Ctx, id_or_name: &str, why: &str) -> Result<()> {
    let (mut row, _turn) = locked_row(ctx, id_or_name).await?;
    let label = row.name.clone().unwrap_or_else(|| row.id.clone());
    match row.status {
        CloneStatus::Ready => {}
        CloneStatus::Sleeping => return Ok(()),
        _ => return Err(invalid(format!("clone {label} is not running"))),
    }
    let _mark = mark(ctx, &row.id, InFlightOp::Create)
        .map_err(|_| invalid(format!("clone {label} is busy; try again in a moment")))?;
    let open = || invalid(format!("clone {label} has open connections"));
    if ctx.router().open_conns(&row.id) > 0 {
        return Err(open());
    }
    // Marked first: a router connection that starts after this sees Sleeping and waits for the
    // wake; one counted before it shows up in the second check.
    ctx.store()
        .set_clone_status(&row.id, CloneStatus::Sleeping)?;
    if ctx.router().open_conns(&row.id) > 0 {
        ctx.store().set_clone_status(&row.id, CloneStatus::Ready)?;
        return Err(open());
    }
    // The idle check that chose this clone may be several psql calls old: a direct-port session
    // opened since then would be cut by the stop. One that opens after this check still can be.
    let backends = crate::lifecycle::client_backends(ctx, &row).await;
    if !matches!(backends, Ok(0)) {
        ctx.store().set_clone_status(&row.id, CloneStatus::Ready)?;
        return Err(match backends {
            Err(e) => e,
            Ok(_) => open(),
        });
    }
    ctx.router().close_clone(&row.id);
    let install = install_for(ctx, &row.dir).ok_or_else(|| {
        internal(format!(
            "no Postgres install found for {}",
            row.dir.display()
        ))
    });
    let expected = row
        .pg_pid
        .zip(row.pg_start_time)
        .map(|(pid, start_time)| RunningPg { pid, start_time });
    let (dir, t) = (row.dir.clone(), ctx.config().stop_timeout);
    let stopped = match install {
        Ok(i) => blocking(move || stop(&i, &dir, expected, t)).await,
        Err(e) => Err(e),
    };
    if let Err(e) = stopped {
        // Possibly still running: it stays Ready, so nothing treats it as stopped.
        if let Err(re) = ctx.store().set_clone_status(&row.id, CloneStatus::Ready) {
            log(
                ctx,
                &format!(
                    "sleep of clone {}: the stop failed and the row could not go back to ready, \
                     so it says sleeping while its server may run: {re}",
                    row.id
                ),
            );
        }
        return Err(e);
    }
    row.status = CloneStatus::Sleeping;
    row.pg_pid = None;
    row.pg_start_time = None;
    ctx.store().update_clone(&row)?;
    events::emit_clone(
        ctx,
        events::CLONE_SLEPT,
        &row,
        serde_json::json!({ "reason": why }),
    );
    Ok(())
}

/// Why a suspect clone should be replaced, and how.
pub fn suspect_warning(row: &CloneRow) -> String {
    let name = row.name.as_deref().unwrap_or(&row.id);
    format!(
        "clone {name} was restarted after an unclean shutdown with fsync off; recent writes may be \
         lost or corrupt (hint: mitosis clone --name {name} --fresh)"
    )
}

pub async fn create(ctx: &Arc<Ctx>, mut args: CloneCreateArgs) -> Result<CloneInfo> {
    let started = Instant::now();
    // Before any work: `--fresh` resets the clone of a name, so without one it means nothing.
    if args.fresh && args.name.is_none() {
        return Err(invalid("--fresh needs a clone name")
            .with_hint("mitosis clone --name <name> --fresh (MCP: pass `name` with `fresh`)"));
    }
    // `--branch`: clone the base's branch template, made or rebuilt first when needed.
    let branch = match args.branch.take() {
        Some(b) => {
            if let Some(name) = &args.name {
                check_clone_name(ctx, name)?;
            }
            let used = crate::templates::ensure_branch(ctx, &args.template, &b).await?;
            args.template = used.name.clone();
            Some(used)
        }
        None => None,
    };
    // The `.env` records a `--fresh` took from the clone it destroyed.
    let mut taken = Vec::new();
    let mut info = match create_inner(ctx, &mut args, &mut taken).await {
        Ok(info) => info,
        Err(e) => {
            // The old clone is gone and its files still hold its URL: the next client command
            // puts the user's values back.
            for rec in &taken {
                if let Err(pe) = ctx.store().add_pending_restore(rec) {
                    log(ctx, &format!("pending restore of {}: {pe}", rec.var));
                }
            }
            return Err(e);
        }
    };
    if let Some(used) = branch {
        // A reused named clone was made before this rebuild.
        info.rebuilt = used.rebuilt && !info.existing;
        info.warnings.extend(used.warnings);
        info.stale |= used.stale;
        info.refreshing |= used.refreshing;
    }
    info.warnings.extend(crate::router::warning(ctx));
    info.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(info)
}

/// Records the clone's `.env` entries: those `taken` from the clone a `--fresh` destroyed (each
/// keeping the user's `previous`, pointed at the new URL) and the one named by `args.env`. A new
/// clone calls this while its create mark is held, so no destroy can come before the records.
async fn attach_env(
    ctx: &Ctx,
    info: &mut CloneInfo,
    args: &CloneCreateArgs,
    taken: Vec<PendingRestore>,
) {
    let remember = |info: &mut CloneInfo, rec: &PendingRestore| {
        if let Err(e) = ctx.store().add_env_record(&info.id, rec) {
            info.warnings.push(format!(
                "could not remember the .env entry, so it will not be restored: {}",
                e.message
            ));
        }
    };
    for mut rec in taken {
        if args
            .env
            .as_ref()
            .is_some_and(|t| t.file == rec.file && t.var == rec.var)
        {
            continue;
        }
        if rec.url != info.url {
            let (r, url) = (rec.clone(), info.url.clone());
            match blocking(move || envfile::rewrite(&r.file, &r.var, &r.url, &url)).await {
                Ok(_) => rec.url = info.url.clone(),
                // The file still holds the old URL, which is what a later restore looks for.
                Err(e) => info.warnings.push(format!(
                    "could not update {} in {}: {}",
                    rec.var,
                    rec.file.display(),
                    e.message
                )),
            }
        }
        remember(info, &rec);
    }
    // Also for a reused or `--fresh` clone: the client writes its URL into the file either way.
    if let Some(t) = &args.env {
        let rec = PendingRestore {
            id: 0,
            file: t.file.clone(),
            var: t.var.clone(),
            previous: t.previous.clone(),
            url: info.url.clone(),
        };
        remember(info, &rec);
    }
}

/// A named create when a clone of that name exists: its info if it can be reused (`existing`),
/// an error if not, `None` after `--fresh` destroyed it (its `.env` records are then in `taken`).
async fn reuse_named(
    ctx: &Arc<Ctx>,
    name: &str,
    args: &mut CloneCreateArgs,
    taken: &mut Vec<PendingRestore>,
) -> Result<Option<CloneInfo>> {
    let found = ctx
        .store()
        .list_clones()?
        .into_iter()
        .find(|c| c.name.as_deref() == Some(name));
    let Some(mut row) = found else {
        return Ok(None);
    };
    if row.status == CloneStatus::Stopping {
        return Err(invalid(format!(
            "clone {name} is being destroyed; try again in a moment"
        )));
    }
    // The same pid with another start time is a new session that got a dead session's pid.
    let other_session = args.parent_pid.is_some_and(|p| {
        row.parent_pid != Some(p)
            || row
                .parent_start_time
                .is_some_and(|t| !crate::lifecycle::parent_alive(p, Some(t)))
    });
    if other_session {
        return Err(
            invalid(format!("a clone named {name} belongs to another session"))
                .with_hint("use another name"),
        );
    }
    if args.fresh {
        if row.protected && !args.protect {
            return Err(invalid(format!("clone {name} is protected"))
                .with_hint(format!("mitosis destroy {name} --force")));
        }
        // Checked before the destroy, so a typo or a missing install does not cost the old clone.
        let (_, gen) = ctx
            .store()
            .current(&args.template)?
            .ok_or_else(|| template_missing(&args.template))?;
        let install = ctx.install_major(gen.pg_major).await?;
        let ext = gen.extensions.clone();
        blocking(move || check_extensions(&install, &ext)).await?;
        // The new clone takes over all .env entries, so the old one must not restore them: their
        // `previous` is the user's value, the one to put back when the new clone goes.
        let recs = {
            let store = ctx.store();
            let recs = store.env_records(&row.id)?;
            store.delete_env_records(&row.id)?;
            recs
        };
        if let Some(t) = &mut args.env {
            if let Some(r) = recs.iter().find(|r| r.file == t.file && r.var == t.var) {
                t.previous = r.previous.clone();
            }
        }
        if let Err(e) = destroy_for(ctx, &row.id, reason::FRESH).await {
            for rec in &recs {
                if let Err(re) = ctx.store().add_env_record(&row.id, rec) {
                    log(
                        ctx,
                        &format!(
                            "--fresh of clone {}: could not put back the .env entry of {}: {re}",
                            row.id, rec.var
                        ),
                    );
                }
            }
            return Err(e);
        }
        *taken = recs;
        return Ok(None);
    }
    if row.template != args.template {
        return Err(invalid(format!(
            "clone {name} is from template {}, not {}",
            row.template, args.template
        ))
        .with_hint("use another name, or --fresh"));
    }
    if row.durable != args.durable || row.protected != args.protect {
        return Err(invalid(format!(
            "clone {name} is already running with different durable/protect settings"
        ))
        .with_hint("use --fresh"));
    }
    let mut woke = Vec::new();
    if row.status == CloneStatus::Sleeping {
        (row, woke) = wake(ctx, &row.id).await?;
    }
    let busy = ctx
        .in_flight
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains_key(&row.id);
    if busy || row.status != CloneStatus::Ready {
        return Err(invalid(format!(
            "clone {name} is busy; try again in a moment"
        )));
    }
    let t = now();
    if !row.protected {
        let wanted = t.saturating_add(i64::try_from(args.ttl_secs).unwrap_or(i64::MAX));
        if wanted > row.expires_at {
            row.expires_at = wanted;
            ctx.store().update_clone(&row)?;
        }
    }
    let age = {
        let store = ctx.store();
        if let Some(mut r) = store.get_reservation(name)? {
            r.last_used = t;
            store.upsert_reservation(&r)?;
        }
        store
            .current(&row.template)?
            .map_or(0, |(_, g)| age_secs(g.built_at, t))
    };
    let mut info = to_info(ctx, &row, age);
    info.warnings.extend(woke);
    info.existing = true;
    Ok(Some(info))
}

fn template_missing(name: &str) -> MitosisError {
    MitosisError::new(
        ErrorCode::TemplateNotFound,
        format!("template {name} not found"),
    )
    .with_hint(format!("mitosis template init {name} --seed \"…\""))
}

/// `clone --name`: the name rules, with the id-shape rule only for a name no clone has yet, so a
/// 0.2 clone named like an id can still be reused or recreated with `--fresh`.
fn check_clone_name(ctx: &Ctx, name: &str) -> Result<()> {
    validate_name("clone", name)?;
    let exists = ctx
        .store()
        .list_clones()?
        .iter()
        .any(|c| c.name.as_deref() == Some(name));
    if exists {
        return Ok(());
    }
    crate::names::check_new_clone_name(name)
}

async fn create_inner(
    ctx: &Arc<Ctx>,
    args: &mut CloneCreateArgs,
    taken: &mut Vec<PendingRestore>,
) -> Result<CloneInfo> {
    if let Some(name) = &args.name {
        check_clone_name(ctx, name)?;
    }
    if args.protect && args.name.is_none() {
        return Err(invalid("a protected clone needs a name")
            .with_hint("mitosis clone --name <name> --protect"));
    }
    if args.ttl_secs == 0 && !args.protect {
        return Err(invalid("ttl must be greater than zero"));
    }
    if let Some(name) = args.name.clone() {
        if let Some(mut info) = reuse_named(ctx, &name, args, taken).await? {
            attach_env(ctx, &mut info, args, std::mem::take(taken)).await;
            return Ok(info);
        }
    }
    // Checked before queueing, so a missing template, install or extension, or a parent that is
    // gone, fails at once and does not hold the line (re-checked below if the generation changed).
    let (_, first) = ctx
        .store()
        .current(&args.template)?
        .ok_or_else(|| template_missing(&args.template))?;
    let mut install = ctx.install_major(first.pg_major).await?;
    let (i, ext) = (install.clone(), first.extensions.clone());
    blocking(move || check_extensions(&i, &ext)).await?;
    // A protected clone has no parent.
    let parent_start_time = match args.parent_pid.filter(|_| !args.protect) {
        Some(pid) => Some(
            process_start_time(pid)
                .filter(|_| is_alive(pid))
                .ok_or_else(|| invalid(format!("parent process {pid} is not running")))?,
        ),
        None => None,
    };
    // Admission comes before the template guard and the store lock, and is held until the row is
    // reserved, so no other create can pass the limits in between.
    let wait = args
        .wait_secs
        .map_or(ctx.config().queue_wait, Duration::from_secs);
    let admission = admit(ctx, wait).await?;
    let waited_ms = u64::try_from(admission.waited.as_millis()).unwrap_or(u64::MAX);
    // Held from reading the current generation until its folder is cloned, so no commit or prune
    // moves that folder in between.
    let swap = ctx.template_swap.read().await;
    let (tpl, gen) = ctx
        .store()
        .current(&args.template)?
        .ok_or_else(|| template_missing(&args.template))?;
    // A build committed while this create waited may need another major or other extensions.
    if gen.pg_major != first.pg_major || gen.extensions != first.extensions {
        install = ctx.install_major(gen.pg_major).await?;
        let (i, ext) = (install.clone(), gen.extensions.clone());
        blocking(move || check_extensions(&i, &ext)).await?;
    }

    let reserved = reserve(ctx, &tpl, &gen, args, parent_start_time);
    drop(admission);
    let (mut row, _in_flight, mut warnings, prior) = reserved?;
    // `reserve` already warned when the reserved port could not be used.
    let moved_in_reserve = prior.as_ref().is_some_and(|p| p.port != row.port);
    // A vanished row (CloneNotFound from an update) also lands here: stop, remove, report it.
    match bring_up(ctx, &install, swap, &gen, &mut row).await {
        Ok(mut info) => {
            (info.stale, info.refreshing) = stale_while_refresh(ctx, &tpl, &gen);
            // The port moved during start-up (it was taken after the pick): keep the reservation.
            if let Some(name) = &row.name {
                let store = ctx.store();
                // Best effort: the clone is up; a failed write only leaves the old port reserved.
                if let Some(mut r) = store
                    .get_reservation(name)
                    .ok()
                    .flatten()
                    .filter(|r| r.port != row.port)
                {
                    // A brand-new name had no URL before, so nothing changed for anyone.
                    if prior.is_some() && !moved_in_reserve {
                        warnings.push(port_moved(ctx, r.port, TAKEN));
                    }
                    r.port = row.port;
                    let _ = store.upsert_reservation(&r);
                }
            }
            let slow = ctx.backend.name() == "copy";
            if slow {
                warnings.push(crate::clone_backend::SLOW_WARNING.to_string());
            }
            info.warnings = warnings;
            info.waited_ms = waited_ms;
            let mut detail = serde_json::json!({
                "gen": row.generation,
                "durable": row.durable,
                "protected": row.protected,
                "waited_ms": waited_ms,
            });
            if slow {
                detail["slow_backend"] = serde_json::json!(true);
            }
            events::emit_clone(ctx, events::CLONE_CREATED, &row, detail);
            // While `_in_flight` is held: no reaper or parent-exit destroy comes before the records.
            attach_env(ctx, &mut info, args, std::mem::take(taken)).await;
            Ok(info)
        }
        Err(e) => {
            // A failed cleanup leaves the row in Stopping for gc; the caller still gets the cause.
            if let Err(ce) = stop_and_remove(ctx, &row, reason::FAILED).await {
                let log = ctx.home.logs_dir().join(format!("{}.log", row.id));
                if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&log) {
                    let _ = writeln!(f, "mitosis: cleanup after failed create failed: {ce}");
                }
            }
            // The name's reservation goes back to how it was: a name never created keeps nothing.
            if let Some(name) = &row.name {
                let store = ctx.store();
                let undone = match &prior {
                    Some(r) => store.upsert_reservation(r),
                    None => store.delete_reservation(name),
                };
                if let Err(re) = undone {
                    drop(store);
                    log(
                        ctx,
                        &format!("reservation of {name} after a failed create: {re}"),
                    );
                }
            }
            Err(e)
        }
    }
}

/// `(stale, refreshing)` for a clone just made from `gen`, the current generation of `tpl`. A stale
/// template with `on_stale = refresh` that is not paused gets a background refresh in its
/// `every_mode` unless one runs; `refreshing` is also true when a build was already running.
fn stale_while_refresh(ctx: &Arc<Ctx>, tpl: &TemplateRow, gen: &GenerationRow) -> (bool, bool) {
    let now = now();
    if age_secs(gen.built_at, now) <= tpl.max_age_secs {
        return (false, false);
    }
    if tpl.on_stale == OnStale::Warn || scheduler::full_refresh_refused(tpl) {
        return (true, false);
    }
    if !tpl.paused && scheduler::may_retry(tpl, now) {
        scheduler::start(ctx, &tpl.name, tpl.every_mode);
    }
    (true, crate::templates::is_building(ctx, &tpl.name))
}

/// Marks a clone id as having a create or destroy running (see `Ctx::in_flight`) until dropped.
pub(crate) struct InFlight<'a> {
    ctx: &'a Ctx,
    id: String,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.ctx
            .in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

/// Marks `id` for `op`; when it already has an operation running, returns that operation.
pub(crate) fn mark<'a>(
    ctx: &'a Ctx,
    id: &str,
    op: InFlightOp,
) -> std::result::Result<InFlight<'a>, InFlightOp> {
    let mut map = ctx.in_flight.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(running) = map.get(id) {
        return Err(*running);
    }
    map.insert(id.to_string(), op);
    Ok(InFlight {
        ctx,
        id: id.to_string(),
    })
}

/// What `reserve` made: the row, its in-flight mark, warnings, and the name's reservation as it
/// was before this create (`None`: a new name), so a failed create can put it back.
type Reserved<'a> = (CloneRow, InFlight<'a>, Vec<String>, Option<ReservationRow>);

/// Steps 3-4 under one store lock, so concurrent creates cannot both pass the cap or share a port.
/// The id joins `ctx.in_flight` under that same lock, so reconcile never sees the row without it.
fn reserve<'a>(
    ctx: &'a Ctx,
    tpl: &TemplateRow,
    gen: &GenerationRow,
    args: &CloneCreateArgs,
    parent_start_time: Option<u64>,
) -> Result<Reserved<'a>> {
    let cfg = ctx.config();
    let store = ctx.store();
    let active: Vec<String> = store
        .list_clones()?
        .into_iter()
        .filter(|c| c.status != CloneStatus::Stopping)
        .map(|c| c.name.unwrap_or(c.id))
        .collect();
    if active.len() >= cfg.max_clones {
        return Err(crate::budget::cap_reached(cfg.max_clones, &active));
    }
    let clones_dir = ctx.home.clones_dir();
    let free = free_space(&clones_dir)?;
    if free < cfg.min_free_bytes {
        return Err(MitosisError::new(
            ErrorCode::LowDisk,
            format!(
                "only {free} bytes free on the volume of {}; need at least {}",
                clones_dir.display(),
                cfg.min_free_bytes
            ),
        )
        .with_hint("free disk space, or destroy unused clones"));
    }
    let id = loop {
        let id = new_clone_id();
        let marked = ctx
            .in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&id);
        if !marked && store.get_clone(&id)?.is_none() && !clones_dir.join(&id).exists() {
            break id;
        }
    };
    let created_at = now();
    let leased = store.leased_ports()?;
    let mut reserved = store.reserved_ports()?;
    let mut warnings = Vec::new();
    let mut new_res = None;
    let mut prior = None;
    // A named clone keeps the port and password of its reservation while that port is free.
    let (port, password) = match &args.name {
        None => (
            pick_port(cfg.port_range.clone(), &leased, &HashSet::new(), &reserved)?,
            random_hex(16),
        ),
        Some(name) => {
            let res = store.get_reservation(name)?;
            prior = res.clone();
            if let Some(r) = &res {
                reserved.remove(&r.port);
            }
            let (port, password) = match res {
                Some(r)
                    if cfg.port_range.contains(&r.port)
                        && !leased.contains(&r.port)
                        && TcpListener::bind(("127.0.0.1", r.port)).is_ok() =>
                {
                    (r.port, r.password)
                }
                other => {
                    let port =
                        pick_port(cfg.port_range.clone(), &leased, &HashSet::new(), &reserved)?;
                    if let Some(r) = &other {
                        let why = if cfg.port_range.contains(&r.port) {
                            TAKEN
                        } else {
                            OUT_OF_RANGE
                        };
                        warnings.push(port_moved(ctx, r.port, why));
                    }
                    (port, other.map_or_else(|| random_hex(16), |r| r.password))
                }
            };
            // Written after the clone row, so a create that loses on the unique name leaves it be.
            new_res = Some(ReservationRow {
                name: name.clone(),
                port,
                password: password.clone(),
                template: tpl.name.clone(),
                last_used: created_at,
            });
            (port, password)
        }
    };
    let row = CloneRow {
        id: id.clone(),
        name: args.name.clone(),
        template: tpl.name.clone(),
        dir: clones_dir.join(&id),
        port,
        socket_dir: ctx.home.sockets_dir().join(&id),
        password,
        database: tpl.database.clone(),
        pg_pid: None,
        pg_start_time: None,
        parent_pid: args.parent_pid.filter(|_| !args.protect),
        parent_start_time,
        created_at,
        expires_at: if args.protect {
            i64::MAX
        } else {
            created_at.saturating_add(i64::try_from(args.ttl_secs).unwrap_or(i64::MAX))
        },
        durable: args.durable,
        status: CloneStatus::Creating,
        generation: gen.gen,
        protected: args.protect,
        suspect: false,
    };
    store.insert_clone(&row)?;
    if let Some(r) = &new_res {
        store.upsert_reservation(r)?;
    }
    let mark = mark(ctx, &id, InFlightOp::Create)
        .map_err(|_| internal(format!("clone {id} is already in use")))?;
    Ok((row, mark, warnings, prior))
}

/// Test hook, called with (home root, port) right before each Postgres start in `bring_up`, so a
/// test can occupy the port after `pick_port`'s pre-bind check. Always `None` outside tests.
#[doc(hidden)]
pub type PreStartHook = Box<dyn Fn(&Path, u16) + Send + Sync>;
#[doc(hidden)]
pub static PRE_START_HOOK: std::sync::Mutex<Option<PreStartHook>> = std::sync::Mutex::new(None);

fn set_status(ctx: &Ctx, row: &mut CloneRow, status: CloneStatus) -> Result<()> {
    row.status = status;
    ctx.store().update_clone(row)
}

/// Steps 5-6: clone the generation folder, start Postgres, set the password. `row` tracks what was
/// done. `swap` (the template read guard) is released once the folder is cloned.
async fn bring_up(
    ctx: &Ctx,
    install: &PgInstall,
    swap: tokio::sync::RwLockReadGuard<'_, ()>,
    gen: &GenerationRow,
    row: &mut CloneRow,
) -> Result<CloneInfo> {
    let (backend, src, dst) = (ctx.backend.clone(), gen.dir.clone(), row.dir.clone());
    let cloned = blocking(move || backend.clone_dir(&src, &dst)).await;
    drop(swap);
    cloned?;
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
        if let Some(hook) = &*PRE_START_HOOK.lock().unwrap_or_else(|e| e.into_inner()) {
            hook(&ctx.home.root, row.port);
        }
        let pg = start(install, &row.dir, &opts)?;
        // Recorded at once, so a failed create stops exactly this server.
        row.pg_pid = Some(pg.pid);
        row.pg_start_time = Some(pg.start_time);
        match wait_ready(install, &pg, &opts, ctx.config().ready_timeout).await {
            Ok(()) => break pg,
            // Template builds use ports the store does not know about, so a pick can still collide.
            Err(e) if e.code == ErrorCode::PortInUse && tries < START_TRIES => {
                let (i, d, t) = (install.clone(), row.dir.clone(), ctx.config().stop_timeout);
                blocking(move || stop(&i, &d, Some(pg), t)).await?;
                tried.insert(row.port);
                let store = ctx.store();
                row.port = pick_port(
                    ctx.config().port_range,
                    &store.leased_ports()?,
                    &tried,
                    &store.reserved_ports()?,
                )?;
                store.update_clone(row)?;
            }
            Err(e) => return Err(e),
        }
    };

    set_password(install, row).await?;

    row.pg_pid = Some(pg.pid);
    row.pg_start_time = Some(process_start_time(pg.pid).unwrap_or(pg.start_time));
    set_status(ctx, row, CloneStatus::Ready)?;
    Ok(to_info(ctx, row, age_secs(gen.built_at, now())))
}

/// Sets the clone's `postgres` password over its private socket.
async fn set_password(install: &PgInstall, row: &CloneRow) -> Result<()> {
    // PG 12/13 default to md5; initdb set host auth to scram-sha-256, so store a SCRAM hash.
    // log_min_error_statement=panic keeps a failing statement (with the password) out of the log.
    let sql = format!(
        "SET log_min_error_statement = panic; SET password_encryption = 'scram-sha-256'; \
         ALTER ROLE postgres PASSWORD '{}'",
        row.password
    );
    let (i, s, p) = (install.clone(), row.socket_dir.clone(), row.port);
    blocking(move || psql(&i, &s, p, "postgres", &sql))
        .await
        .map_err(|mut e| {
            e.message = e.message.replace(&row.password, "***");
            e
        })?;
    Ok(())
}

/// Starts Postgres again on a clone whose server is gone (after a reboot): same folder, port,
/// socket folder (recreated) and password. If the port is taken it moves, and the reservation and
/// `.env` records follow; the returned warnings say so. On error the caller cleans up.
pub async fn restart(ctx: &Ctx, row: &mut CloneRow) -> Result<Vec<String>> {
    let install = install_for(ctx, &row.dir).ok_or_else(|| {
        internal(format!(
            "no Postgres install found for {}",
            row.dir.display()
        ))
    })?;
    let (dir, sock) = (row.dir.clone(), row.socket_dir.clone());
    blocking(move || {
        // A stale pid file can name a pid that now belongs to another process after a reboot.
        if running_server(&dir, None)?.is_none() {
            remove_if_present(&dir.join("postmaster.pid"), false)?;
        }
        remove_if_present(&sock, true)
    })
    .await?;

    let old_port = row.port;
    let log_file = ctx.home.logs_dir().join(format!("{}.log", row.id));
    let mut tried = HashSet::new();
    let mut tries = 0;
    loop {
        tries += 1;
        let opts = StartOptions {
            port: row.port,
            socket_dir: row.socket_dir.clone(),
            listen_tcp: true,
            durable: row.durable,
            log_file: log_file.clone(),
        };
        let pg = start(&install, &row.dir, &opts)?;
        // Recorded at once, so a later crash or cleanup stops exactly this server.
        row.pg_pid = Some(pg.pid);
        row.pg_start_time = Some(pg.start_time);
        ctx.store().update_clone(row)?;
        match wait_ready(&install, &pg, &opts, ctx.config().ready_timeout).await {
            Ok(()) => break,
            Err(e) if e.code == ErrorCode::PortInUse && tries < START_TRIES => {
                let (i, d, t) = (install.clone(), row.dir.clone(), ctx.config().stop_timeout);
                blocking(move || stop(&i, &d, Some(pg), t)).await?;
                tried.insert(row.port);
                let store = ctx.store();
                row.port = pick_port(
                    ctx.config().port_range,
                    &store.leased_ports()?,
                    &tried,
                    &store.reserved_ports()?,
                )?;
                store.update_clone(row)?;
            }
            Err(e) => return Err(e),
        }
    }
    // With synchronous_commit off, an unclean stop can lose the create's ALTER ROLE; set it again.
    set_password(&install, row).await?;
    if let Some(pid) = row.pg_pid {
        row.pg_start_time = process_start_time(pid).or(row.pg_start_time);
    }
    ctx.store().update_clone(row)?;
    if row.port == old_port {
        return Ok(Vec::new());
    }
    let mut warnings = vec![port_moved(ctx, old_port, TAKEN)];
    let new_url = to_info(ctx, row, 0).url;
    let recs = {
        let store = ctx.store();
        if let Some(name) = &row.name {
            if let Some(mut r) = store.get_reservation(name)? {
                r.port = row.port;
                store.upsert_reservation(&r)?;
            }
        }
        store.env_records(&row.id)?
    };
    // Records already holding the router URL need no rewrite. The files are edited off the
    // runtime and without the store lock, as `--fresh` does.
    for mut rec in recs.into_iter().filter(|r| r.url != new_url) {
        let (r, url) = (rec.clone(), new_url.clone());
        match blocking(move || envfile::rewrite(&r.file, &r.var, &r.url, &url)).await {
            Ok(_) => rec.url = new_url.clone(),
            // The file still holds the old URL and so does the record: a later restore matches.
            Err(e) => {
                warnings.push(format!(
                    "could not update {} in {}: {}",
                    rec.var,
                    rec.file.display(),
                    e.message
                ));
                continue;
            }
        }
        ctx.store().add_env_record(&row.id, &rec)?;
    }
    Ok(warnings)
}

/// `restart`, and on error stops whatever it started and puts the old pid and port back, so the
/// next try starts from the recorded identity (the status is left as it was).
pub(crate) async fn restart_or_undo(ctx: &Ctx, row: &mut CloneRow) -> Result<Vec<String>> {
    let (old_pid, old_start, old_port) = (row.pg_pid, row.pg_start_time, row.port);
    let err = match restart(ctx, row).await {
        Ok(warnings) => return Ok(warnings),
        Err(e) => e,
    };
    let started = row
        .pg_pid
        .zip(row.pg_start_time)
        .filter(|_| row.pg_pid != old_pid)
        .map(|(pid, start_time)| RunningPg { pid, start_time });
    if let (Some(pg), Some(install)) = (started, install_for(ctx, &row.dir)) {
        let (dir, t) = (row.dir.clone(), ctx.config().stop_timeout);
        if let Err(se) = blocking(move || stop(&install, &dir, Some(pg), t)).await {
            // It may still run: keep the identity that names it, so gc sees it and nothing else
            // starts a second server on the folder.
            log(
                ctx,
                &format!(
                    "restart of clone {}: could not stop the server it started (pid {}): {se}",
                    row.id, pg.pid
                ),
            );
            let _ = ctx.store().update_clone(row);
            return Err(err);
        }
    }
    row.pg_pid = old_pid;
    row.pg_start_time = old_start;
    // A port move already saved must be undone too, or the retry would think the URL never
    // changed and leave the reservation and .env records on the old port.
    row.port = old_port;
    let _ = ctx.store().update_clone(row);
    Err(err)
}

/// Destroy as a user asks for it: a protected clone needs `force`; `forget` also drops the
/// name's reservation (even when no clone of that name is left).
pub async fn destroy_with(
    ctx: &Ctx,
    id_or_name: &str,
    force: bool,
    forget: bool,
) -> Result<String> {
    let row = ctx.store().get_clone(id_or_name)?;
    if let Some(r) = row.as_ref().filter(|r| r.protected && !force) {
        return Err(invalid(format!(
            "clone {} is protected",
            r.name.as_deref().unwrap_or(&r.id)
        ))
        .with_hint(format!(
            "mitosis destroy {} --force",
            r.name.as_deref().unwrap_or(&r.id)
        )));
    }
    match destroy(ctx, id_or_name).await {
        Ok(id) => {
            if let Some(name) = row.and_then(|r| r.name).filter(|_| forget) {
                ctx.store().delete_reservation(&name)?;
            }
            Ok(id)
        }
        Err(e)
            if forget
                && e.code == ErrorCode::CloneNotFound
                && ctx.store().get_reservation(id_or_name)?.is_some() =>
        {
            ctx.store().delete_reservation(id_or_name)?;
            Ok(id_or_name.to_string())
        }
        Err(e) => Err(e),
    }
}

/// Destroys regardless of protection, as a user's request (see `destroy_for`).
pub async fn destroy(ctx: &Ctx, id_or_name: &str) -> Result<String> {
    destroy_for(ctx, id_or_name, reason::USER).await
}

/// Destroys regardless of protection (the reaper, the parent watcher and `--fresh` use it);
/// `why` is the `reason` of the `clone.destroyed` event.
pub(crate) async fn destroy_for(ctx: &Ctx, id_or_name: &str, why: &str) -> Result<String> {
    let row = ctx.store().get_clone(id_or_name)?.ok_or_else(|| {
        MitosisError::new(
            ErrorCode::CloneNotFound,
            format!("clone {id_or_name} not found"),
        )
        .with_hint("see existing clones with `mitosis list` (or the clone_list tool)")
    })?;
    let starting = || {
        invalid(format!(
            "clone {} is still starting; try again in a moment",
            row.id
        ))
    };
    // An in-flight create owns these; removing them under it would orphan its server.
    if matches!(row.status, CloneStatus::Creating | CloneStatus::Starting) {
        return Err(starting());
    }
    let _mark = match mark(ctx, &row.id, InFlightOp::Destroy) {
        Ok(m) => m,
        // Ready with a create that has not returned yet, or a wake or sleep under way (they hold
        // the same mark). The parent watcher retries on this InvalidRequest.
        Err(InFlightOp::Create) => {
            return Err(invalid(format!(
                "clone {} is busy (starting, waking or going to sleep); try again in a moment",
                row.id
            )))
        }
        Err(InFlightOp::Destroy) => return Ok(row.id), // user, reaper or parent watcher got there first
    };
    stop_and_remove(ctx, &row, why).await?;
    Ok(row.id)
}

/// The install whose `pg_ctl` stops the cluster in `dir`: its own major, else the newest.
pub(crate) fn install_for(ctx: &Ctx, dir: &Path) -> Option<PgInstall> {
    let installs = ctx.installs();
    read_pg_version(dir)
        .ok()
        .and_then(|m| find_major(&installs, m).ok())
        .or_else(|| newest(&installs).ok())
        .cloned()
}

fn remove_if_present(path: &Path, dir: bool) -> Result<()> {
    let res = if dir {
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

/// Marks the clone Stopping, stops its Postgres, removes its folders and log, then drops the row.
/// If Postgres may still be running, nothing is removed and the error is returned.
/// A missing row is fine: the folders are still cleaned up. Records `clone.destroyed` with `why`
/// as its reason once the clone is gone.
pub(crate) async fn stop_and_remove(ctx: &Ctx, row: &CloneRow, why: &str) -> Result<()> {
    ctx.store()
        .set_clone_status(&row.id, CloneStatus::Stopping)?;
    // Router clients go first, so the stop does not wait on them and none stays half-open.
    ctx.router().close_clone(&row.id);
    let dir = ctx.home.clones_dir().join(&row.id);
    let sock = ctx.home.sockets_dir().join(&row.id);
    let log = ctx.home.logs_dir().join(format!("{}.log", row.id));
    let install = install_for(ctx, &dir);
    let timeout = ctx.config().stop_timeout;
    let backend = ctx.backend.clone();
    let expected = row
        .pg_pid
        .zip(row.pg_start_time)
        .map(|(pid, start_time)| RunningPg { pid, start_time });
    blocking(move || {
        if running_server(&dir, expected)?.is_some() {
            let install = install.ok_or_else(|| {
                internal(format!(
                    "no Postgres install found to stop the server in {}",
                    dir.display()
                ))
            })?;
            stop(&install, &dir, expected, timeout)?;
        }
        backend.remove_dir(&dir)?;
        remove_if_present(&sock, true)?;
        remove_if_present(&log, false)
    })
    .await?;
    ctx.waking
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&row.id);
    ctx.router().last_active.remove(&row.id);
    restore_env(ctx, &row.id).await;
    let deleted = ctx.store().delete_clone(&row.id);
    if deleted.is_ok() {
        events::emit_clone(
            ctx,
            events::CLONE_DESTROYED,
            row,
            serde_json::json!({ "reason": why }),
        );
    }
    // Every destroy path ends here (user, --fresh, reaper, parent watch, gc, failed create).
    ctx.room.notify_waiters();
    deleted
}

/// Puts back the `.env` values the clone replaced. A restore that fails (a read-only file, say)
/// is kept as a pending restore for a client to finish; it never fails the destroy. The files are
/// edited off the runtime.
async fn restore_env(ctx: &Ctx, clone_id: &str) {
    let recs = match ctx.store().env_records(clone_id) {
        Ok(r) => r,
        Err(e) => {
            log(ctx, &format!("env records of {clone_id}: {e}"));
            return;
        }
    };
    let failed = blocking(move || {
        Ok(recs
            .into_iter()
            .filter_map(|rec| envfile::restore(&rec).err().map(|e| (rec, e)))
            .collect::<Vec<_>>())
    })
    .await
    .unwrap_or_else(|e| {
        log(ctx, &format!("env restores of {clone_id}: {e}"));
        Vec::new()
    });
    for (rec, e) in failed {
        log(
            ctx,
            &format!(
                "restore of {} in {} deferred: {}",
                rec.var,
                rec.file.display(),
                e.message
            ),
        );
        if let Err(e) = ctx.store().add_pending_restore(&rec) {
            log(ctx, &format!("pending restore of {}: {e}", rec.var));
        }
    }
    if let Err(e) = ctx.store().delete_env_records(clone_id) {
        log(ctx, &format!("env records of {clone_id}: {e}"));
    }
}

/// True while the row's recorded postmaster runs (same pid, same start time).
fn server_up(r: &CloneRow) -> bool {
    r.pg_pid
        .is_some_and(|pid| is_alive(pid) && process_start_time(pid) == r.pg_start_time)
}

pub fn list(ctx: &Ctx) -> Result<Vec<CloneInfo>> {
    let now = now();
    let warn_at = ctx.config().clone_disk_warn;
    let (rows, ages) = {
        let store = ctx.store();
        let mut ages: HashMap<String, u64> = HashMap::new();
        for t in store.list_templates()? {
            if let Some((_, cur)) = store.current(&t.name)? {
                ages.insert(t.name, age_secs(cur.built_at, now));
            }
        }
        (store.list_clones()?, ages)
    };
    Ok(rows
        .iter()
        .map(|r| {
            let mut ci = to_info(ctx, r, ages.get(r.template.as_str()).copied().unwrap_or(0));
            // Postmaster plus its backends; skip a pid that was reused by another process.
            ci.rss_bytes = r
                .pg_pid
                .filter(|pid| process_start_time(*pid) == r.pg_start_time)
                .and_then(rss_bytes);
            ci.own_disk_bytes = ctx.backend.private_size(&r.dir);
            ci.disk_warn = ci.own_disk_bytes.is_some_and(|b| b > warn_at);
            // A restart that keeps failing leaves the row Ready; say so instead of looking fine.
            if r.status == CloneStatus::Ready && ci.rss_bytes.is_none() && !server_up(r) {
                let label = r.name.as_deref().unwrap_or(&r.id);
                ci.warnings.push(format!(
                    "the server of clone {label} is not running; `mitosis gc` recovers it"
                ));
            }
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
            generation: 3,
            protected: false,
            suspect: false,
        };
        let tmp = tempfile::tempdir().unwrap();
        let ctx = Ctx::open(
            crate::paths::Home::at(tmp.path().join("m")),
            crate::ctx::Config::default(),
        )
        .unwrap();
        let i = to_info(&ctx, &row, 5);
        let direct = format!(
            "postgres://postgres:{}@127.0.0.1:54321/app",
            "ab".repeat(16)
        );
        assert_eq!(i.direct_url, direct);
        assert_eq!(
            i.url,
            format!(
                "postgres://postgres:{}@127.0.0.1:54300/feat",
                "ab".repeat(16)
            )
        );
        assert_eq!(i.status, "ready");
        let unnamed = CloneRow {
            name: None,
            status: CloneStatus::Creating,
            ..row.clone()
        };
        let u = to_info(&ctx, &unnamed, 0);
        assert!(u.url.ends_with(":54300/c0a1b2"), "{}", u.url);
        assert_eq!(u.status, "starting");
        ctx.set_config(crate::ctx::Config {
            router: false,
            ..ctx.config()
        });
        assert_eq!(to_info(&ctx, &row, 5).url, direct);
        assert_eq!(
            (i.id.as_str(), i.template_age_secs, i.parent_pid, i.durable),
            ("c0a1b2", 5, Some(3), true)
        );
        assert_eq!((i.rss_bytes, i.own_disk_bytes), (None, None));
        assert_eq!(i.generation, 3);
    }

    #[test]
    fn info_url_encodes_the_database_name() {
        let mut row = CloneRow {
            id: "c".into(),
            name: None,
            template: "main".into(),
            dir: PathBuf::from("/h/c"),
            port: 54321,
            socket_dir: PathBuf::from("/h/s"),
            password: "pw".into(),
            database: "my db/x?y%z".into(),
            pg_pid: None,
            pg_start_time: None,
            parent_pid: None,
            parent_start_time: None,
            created_at: 0,
            expires_at: 0,
            durable: false,
            status: CloneStatus::Ready,
            generation: 1,
            protected: false,
            suspect: false,
        };
        assert_eq!(
            direct_url(&row),
            format!(
                "postgres://postgres:{}@127.0.0.1:54321/my%20db%2Fx%3Fy%25z",
                row.password
            )
        );
        row.database = "app".into();
        assert!(direct_url(&row).ends_with("/app"));
    }

    // The daemon spawns these futures on a multi-thread runtime, so they must be Send.
    #[allow(dead_code)]
    fn futures_are_send(ctx: &'static Arc<Ctx>, args: CloneCreateArgs, row: &'static CloneRow) {
        fn send<T: Send>(_: T) {}
        send(create(ctx, args));
        send(destroy(ctx, "x"));
        send(stop_and_remove(ctx, row, "user"));
        send(restart(ctx, Box::leak(Box::new(row.clone()))));
        send(wake(ctx, "x"));
        send(sleep(ctx, "x", "idle"));
        send(ensure_running(ctx, "x"));
    }
}
