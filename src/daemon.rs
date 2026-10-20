use crate::clones;
use crate::ctx::{Config, Ctx};
use crate::error::{ErrorCode, MitosisError, Result};
use crate::lifecycle;
use crate::paths::Home;
use crate::promote;
use crate::protocol::{
    DaemonStatus, Request, RequestEnvelope, Response, ResponseEnvelope, PROTOCOL_VERSION,
};
use crate::scheduler;
use crate::templates::{self, now};
use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use std::fs::{File, OpenOptions, Permissions};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, watch};

const ACCEPT_LOG_EVERY: Duration = Duration::from_secs(10);
const REAP_EVERY: Duration = Duration::from_secs(30);
/// Seconds between drops of unused name reservations by a long-running daemon (gc also does it).
const RESERVATIONS_EVERY_SECS: i64 = 24 * 3600;
/// Longest request line accepted; a longer one closes the connection.
const MAX_LINE: usize = 1 << 20;

fn internal(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, msg)
}

/// Appends one timestamped line to `logs/daemon.log`. Never pass clone URLs: they hold passwords.
fn log(home: &Home, msg: &str) {
    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(home.logs_dir().join("daemon.log"))
    {
        let _ = writeln!(f, "[{}] {msg}", now());
    }
}

/// Takes the exclusive daemon lock; it is released when the returned value drops.
fn take_lock(home: &Home) -> Result<Flock<File>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(home.daemon_lock())?;
    Flock::lock(file, FlockArg::LockExclusiveNonblock).map_err(|(_, errno)| match errno {
        Errno::EWOULDBLOCK => MitosisError::new(
            ErrorCode::AlreadyRunning,
            format!(
                "a mitosis daemon is already running for {}",
                home.root.display()
            ),
        ),
        e => internal(format!("lock {}: {e}", home.daemon_lock().display())),
    })
}

/// What every connection shares.
#[derive(Clone)]
struct Shared {
    ctx: Arc<Ctx>,
    /// Each running request holds an upgraded sender; shutdown waits until none are left.
    work: mpsc::WeakSender<()>,
    shutdown: Arc<watch::Sender<bool>>,
}

/// Runs the daemon for `home` until a `Shutdown` request arrives. Clones keep running afterwards.
pub async fn run(home: Home, config: Config) -> Result<()> {
    home.ensure_dirs()?;
    let lock = take_lock(&home)?;
    let ctx = Arc::new(
        tokio::task::spawn_blocking(move || Ctx::open(home, config))
            .await
            .map_err(|e| internal(format!("background task failed: {e}")))??,
    );
    let home = ctx.home.clone();
    log(
        &home,
        &format!(
            "daemon started: pid {}, home {}",
            std::process::id(),
            home.root.display()
        ),
    );
    match lifecycle::reconcile(ctx.clone(), now()).await {
        Ok(r) => log(
            &home,
            &format!(
                "startup gc: {}",
                serde_json::to_string(&r).unwrap_or_default()
            ),
        ),
        Err(e) => log(&home, &format!("startup gc failed: {e}")),
    }

    // Safe to replace: we hold the lock, so no other daemon owns this socket.
    let sock = home.daemon_sock();
    match std::fs::remove_file(&sock) {
        Err(e) if e.kind() != ErrorKind::NotFound => return Err(e.into()),
        _ => {}
    }
    let listener = UnixListener::bind(&sock)?;
    std::fs::set_permissions(&sock, Permissions::from_mode(0o600))?;

    let (work_tx, mut work_rx) = mpsc::channel::<()>(1);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let shared = Shared {
        ctx: ctx.clone(),
        work: work_tx.downgrade(),
        shutdown: Arc::new(shutdown_tx),
    };
    tokio::spawn(reaper(shared.clone(), shutdown_rx.clone()));
    scheduler::run(ctx.clone(), shutdown_rx.clone());

    let mut last_accept_log: Option<std::time::Instant> = None;
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            acc = listener.accept() => match acc {
                Ok((stream, _)) => {
                    tokio::spawn(connection(shared.clone(), stream));
                }
                Err(e) => {
                    // EMFILE repeats every 100 ms: one line per 10 s is enough.
                    if last_accept_log.is_none_or(|t| t.elapsed() >= ACCEPT_LOG_EVERY) {
                        last_accept_log = Some(std::time::Instant::now());
                        log(&home, &format!("accept failed: {e}"));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        }
    }

    drop(listener);
    let _ = std::fs::remove_file(&sock);
    drop(work_tx);
    // recv() yields None once every running request has dropped its sender.
    if tokio::time::timeout(ctx.config().ready_timeout, work_rx.recv())
        .await
        .is_err()
    {
        log(
            &home,
            "shutdown: requests still running after the wait; stopping anyway",
        );
    }
    // A seed or dump still running would be orphaned when this process exits.
    templates::kill_build_groups(&ctx);
    log(&home, "daemon stopped");
    drop(lock);
    Ok(())
}

async fn reaper(shared: Shared, mut shutdown: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(REAP_EVERY);
    tick.tick().await; // the first tick is immediate; startup gc just ran
    let mut reservations_dropped = now(); // wall clock: a monotonic one stops while the Mac sleeps
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tick.tick() => {}
        }
        let Some(_token) = shared.work.upgrade() else {
            return;
        };
        match lifecycle::reap_expired(&shared.ctx, now()).await {
            Ok(ids) if !ids.is_empty() => log(
                &shared.ctx.home,
                &format!("reaper destroyed: {}", ids.join(", ")),
            ),
            Ok(_) => {}
            Err(e) => log(&shared.ctx.home, &format!("reaper failed: {e}")),
        }
        let t = now();
        if t.saturating_sub(reservations_dropped) >= RESERVATIONS_EVERY_SECS {
            lifecycle::drop_unused_reservations(&shared.ctx, t);
            reservations_dropped = t;
        }
    }
}

async fn connection(shared: Shared, stream: UnixStream) {
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match (&mut rd)
            .take(MAX_LINE as u64 + 1)
            .read_until(b'\n', &mut buf)
            .await
        {
            Ok(0) => return,
            Err(e) => {
                // The loop returns, so this is at most one line per connection.
                log(&shared.ctx.home, &format!("connection read failed: {e}"));
                return;
            }
            Ok(_) => {}
        }
        let too_large = buf.len() > MAX_LINE && buf.last() != Some(&b'\n');
        let (result, shutdown) = if too_large {
            (
                Err(MitosisError::new(
                    ErrorCode::InvalidRequest,
                    "request too large",
                )),
                false,
            )
        } else {
            let line = String::from_utf8_lossy(&buf);
            if line.trim().is_empty() {
                continue;
            }
            match parse(&line) {
                Ok(req) => {
                    let shutdown = matches!(req, Request::Shutdown);
                    (dispatch_watching(&shared, req, &mut rd).await, shutdown)
                }
                Err(e) => (Err(e), false),
            }
        };
        let mut out = match serde_json::to_vec(&ResponseEnvelope {
            v: PROTOCOL_VERSION,
            result,
        }) {
            Ok(out) => out,
            Err(e) => {
                log(&shared.ctx.home, &format!("encode reply: {e}"));
                return;
            }
        };
        out.push(b'\n');
        let sent = wr.write_all(&out).await.is_ok() && wr.flush().await.is_ok();
        if shutdown {
            shared.shutdown.send_replace(true);
        }
        if !sent || too_large || shutdown {
            return;
        }
    }
}

/// Reads one request line; the version is checked before the request shape.
fn parse(line: &str) -> Result<Request> {
    let invalid = |e: serde_json::Error| {
        MitosisError::new(ErrorCode::InvalidRequest, format!("invalid request: {e}"))
    };
    let value: serde_json::Value = serde_json::from_str(line).map_err(invalid)?;
    let Some(v) = value.get("v").and_then(|v| v.as_u64()) else {
        return Err(MitosisError::new(
            ErrorCode::InvalidRequest,
            "invalid request: missing protocol version \"v\"",
        ));
    };
    if v != u64::from(PROTOCOL_VERSION) {
        return Err(MitosisError::new(
            ErrorCode::ProtocolMismatch,
            format!("client speaks protocol {v}, daemon speaks protocol {PROTOCOL_VERSION}"),
        )
        .with_hint("run the same mitosis version for the client and the daemon"));
    }
    Ok(serde_json::from_value::<RequestEnvelope>(value)
        .map_err(invalid)?
        .request)
}

/// `dispatch`, while watching the connection: if the client hangs up before the reply (EOF or a
/// read error), the request is told so and a queued create leaves the queue.
async fn dispatch_watching(
    shared: &Shared,
    req: Request,
    rd: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> Result<Response> {
    let (gone_tx, gone_rx) = watch::channel(false);
    let work = dispatch(shared, req, gone_rx);
    tokio::pin!(work);
    let mut watching = true;
    loop {
        tokio::select! {
            res = &mut work => return res,
            // Pipelined bytes mean the client is still there: stop watching, keep them unread.
            filled = rd.fill_buf(), if watching => {
                watching = false;
                let hung_up = match filled {
                    Ok(b) => b.is_empty(),
                    Err(_) => true,
                };
                if hung_up {
                    gone_tx.send_replace(true);
                }
            }
        }
    }
}

/// Runs `req` on its own task so a dropped connection never cancels a half-done create or destroy.
async fn dispatch(shared: &Shared, req: Request, gone: watch::Receiver<bool>) -> Result<Response> {
    let token = shared.work.upgrade();
    if token.is_none() && !matches!(req, Request::Status | Request::Shutdown) {
        return Err(MitosisError::new(
            ErrorCode::DaemonUnavailable,
            "the daemon is shutting down",
        ));
    }
    // Only the type tag: the full request can hold a source URL with a password.
    let kind = serde_json::to_value(&req)
        .ok()
        .and_then(|v| v["type"].as_str().map(String::from))
        .unwrap_or_default();
    let ctx = shared.ctx.clone();
    let res = tokio::spawn(async move {
        let _token = token;
        crate::budget::CLIENT_GONE
            .scope(gone, serve_one(ctx, req))
            .await
    })
    .await
    .unwrap_or_else(|e| Err(internal(format!("request task failed: {e}"))));
    if let Err(e) = &res {
        log(
            &shared.ctx.home,
            &format!("{kind} failed: {:?}: {}", e.code, e.message),
        );
    }
    res
}

async fn blocking<T: Send + 'static>(ctx: &Arc<Ctx>, f: fn(&Ctx) -> Result<T>) -> Result<T> {
    let ctx = ctx.clone();
    tokio::task::spawn_blocking(move || f(&ctx))
        .await
        .map_err(|e| internal(format!("background task failed: {e}")))?
}

/// Handles one request. `Shutdown` only answers `Ok` here; the connection loop stops the daemon.
pub async fn serve_one(ctx: Arc<Ctx>, req: Request) -> Result<Response> {
    Ok(match req {
        Request::TemplateInit(args) => Response::TemplateBuilt(templates::init(&ctx, args).await?),
        Request::TemplateRefresh {
            name,
            path,
            layered: true,
        } => Response::TemplateBuilt(templates::layered(&ctx, &name, path).await?),
        Request::TemplateRefresh { name, path, .. } => {
            Response::TemplateBuilt(templates::refresh(&ctx, &name, path).await?)
        }
        Request::TemplateSet(args) => Response::TemplateBuilt(templates::set(&ctx, args)?),
        Request::TemplateList => Response::Templates {
            templates: blocking(&ctx, templates::list).await?,
        },
        Request::TemplateHistory { name } => {
            let generations = templates::history(&ctx, &name)?;
            let paused = ctx.store().get_template(&name)?.is_some_and(|t| t.paused);
            Response::Generations {
                template: name,
                generations,
                paused,
            }
        }
        Request::TemplateRollback { name, to } => {
            Response::TemplateBuilt(templates::rollback(&ctx, &name, to).await?)
        }
        Request::TemplateUpgrade { name, to, as_name } => Response::TemplateBuilt(
            crate::upgrade::upgrade(&ctx, &name, to, as_name.as_deref()).await?,
        ),
        Request::CloneCreate(args) => {
            let info = clones::create(&ctx, args).await?;
            // The stored parent: none for a protected clone, the first creator's for a reused one.
            if let Some(pid) = info.parent_pid {
                lifecycle::watch_parent(ctx.clone(), info.id.clone(), pid);
            }
            Response::CloneCreated(info)
        }
        Request::CloneList => Response::Clones {
            clones: blocking(&ctx, clones::list).await?,
        },
        Request::CloneDestroy {
            id_or_name,
            force,
            forget,
        } => Response::Destroyed {
            id: clones::destroy_with(&ctx, &id_or_name, force, forget).await?,
        },
        Request::ClonePromote {
            id_or_name,
            template,
        } => {
            Response::TemplateBuilt(promote::promote(&ctx, &id_or_name, template.as_deref()).await?)
        }
        Request::PendingRestores => Response::PendingRestores {
            items: ctx.store().list_pending_restores()?,
        },
        Request::PendingRestoreDone { id } => {
            ctx.store().delete_pending_restore(id)?;
            Response::Ok
        }
        Request::Gc => Response::Gc(lifecycle::reconcile(ctx.clone(), now()).await?),
        Request::Status => {
            let (clones, templates) = blocking(&ctx, |ctx| {
                let store = ctx.store();
                Ok((store.list_clones()?.len(), store.list_templates()?.len()))
            })
            .await?;
            Response::Status(DaemonStatus {
                pid: std::process::id(),
                home: ctx.home.root.clone(),
                clones,
                templates,
                uptime_secs: ctx.started_at.elapsed().as_secs(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                service: std::env::var_os("MITOSIS_SERVICE").is_some_and(|v| v == "1"),
            })
        }
        Request::Usage => Response::Usage(crate::budget::usage(&ctx).await?),
        Request::ConfigReload => {
            let new = ctx.config().reloaded(&ctx.home)?;
            ctx.set_config(new);
            ctx.room.notify_waiters(); // a raised limit may let queued creates in
            Response::Ok
        }
        Request::Shutdown => Response::Ok,
    })
}
