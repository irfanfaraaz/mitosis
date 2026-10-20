use crate::config::Config;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use crate::protocol::{
    EventFilter, EventRow, Request, RequestEnvelope, Response, ResponseEnvelope, PROTOCOL_VERSION,
};
use crate::service;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Longest reply line accepted from the daemon.
const MAX_REPLY: u64 = 16 << 20;
/// The daemon runs crash reconcile before it binds the socket, so a live child gets a long wait.
const START_TIMEOUT: Duration = Duration::from_secs(30);
/// After the child exits, a racing daemon that won the lock may still be about to bind.
const EXIT_GRACE: Duration = Duration::from_millis(500);
/// Exit status of `mitosis daemon` when another daemon already holds the home's lock.
pub const EXIT_ALREADY_RUNNING: u8 = 3;
const POLL: Duration = Duration::from_millis(50);
/// Times `ensure_daemon` starts the daemon again after the lock holder went away without binding.
const RESPAWNS: u32 = 3;
/// How often `ensure_daemon` asks launchd whether a kickstarted service still runs.
const SERVICE_GRACE: Duration = Duration::from_secs(1);

fn unavailable(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::DaemonUnavailable, msg)
}

/// True when the error means nothing is listening on the socket.
fn not_listening(e: &std::io::Error) -> bool {
    matches!(e.kind(), ErrorKind::NotFound | ErrorKind::ConnectionRefused)
}

/// Sends `req` to the daemon, starting the daemon first if nothing is listening.
pub async fn request(home: &Home, req: Request) -> Result<Response> {
    let stream = match UnixStream::connect(home.daemon_sock()).await {
        Ok(s) => s,
        Err(e) if not_listening(&e) => {
            ensure_daemon(home).await?;
            connect_started(home).await?
        }
        Err(e) => {
            return Err(unavailable(format!(
                "connect to {}: {e}",
                home.daemon_sock().display()
            )))
        }
    };
    match exchange(stream, &req).await? {
        Reply::Done(r) => Ok(*r),
        Reply::Older(v) => {
            // A daemon from an older release: stop it (clones keep running), start ours, retry once.
            replace_old(home, v).await?;
            ensure_daemon(home).await?;
            match exchange(connect_started(home).await?, &req).await? {
                Reply::Done(r) => Ok(*r),
                Reply::Older(v) => Err(mismatch(v)),
            }
        }
    }
}

async fn connect_started(home: &Home) -> Result<UnixStream> {
    UnixStream::connect(home.daemon_sock())
        .await
        .map_err(|e| unavailable(format!("connect to {}: {e}", home.daemon_sock().display())))
}

fn mismatch(theirs: u64) -> MitosisError {
    mismatch_text(Some(theirs))
}

fn mismatch_text(theirs: Option<u64>) -> MitosisError {
    // A newer daemon would come back as the same newer one after a restart by this older client.
    let hint = match theirs {
        Some(v) if v > u64::from(PROTOCOL_VERSION) => {
            "the daemon is from a newer mitosis: run that newer binary (see `which -a mitosis`)"
        }
        _ => "restart the daemon: mitosis daemon stop",
    };
    let theirs = theirs.map_or("unknown".to_string(), |v| v.to_string());
    MitosisError::new(
        ErrorCode::ProtocolMismatch,
        format!("daemon speaks protocol {theirs}, this client speaks protocol {PROTOCOL_VERSION}"),
    )
    .with_hint(hint)
}

/// Sends a raw line to a daemon speaking protocol `v` and returns its parsed reply, if any.
async fn raw_exchange(home: &Home, line: &str) -> Option<serde_json::Value> {
    let stream = UnixStream::connect(home.daemon_sock()).await.ok()?;
    let (rd, mut wr) = stream.into_split();
    wr.write_all(format!("{line}\n").as_bytes()).await.ok()?;
    let mut buf = Vec::new();
    BufReader::new(rd)
        .take(MAX_REPLY)
        .read_until(b'\n', &mut buf)
        .await
        .ok()?;
    serde_json::from_slice(&buf).ok()
}

/// True when no daemon holds the home's lock. Taken and dropped at once (the drop unlocks).
fn lock_free(home: &Home) -> bool {
    match std::fs::File::open(home.daemon_lock()) {
        Ok(f) => nix::fcntl::Flock::lock(f, nix::fcntl::FlockArg::LockExclusiveNonblock).is_ok(),
        Err(_) => true,
    }
}

/// Shuts down a daemon that speaks the older protocol `v` and waits until its socket is gone.
async fn replace_old(home: &Home, v: u64) -> Result<()> {
    // The shutdown request is the one message every version understands.
    raw_exchange(
        home,
        &format!(r#"{{"v":{v},"request":{{"type":"shutdown"}}}}"#),
    )
    .await;
    let deadline = Instant::now() + START_TIMEOUT;
    let probe = format!(r#"{{"v":{PROTOCOL_VERSION},"request":{{"type":"status"}}}}"#);
    loop {
        // Done when nothing answers and the lock is free, or a daemon of our version answers (a
        // concurrent client replaced it). The old daemon removes its socket before it drains its
        // work and drops the lock; a daemon spawned before that would exit as already running.
        match raw_exchange(home, &probe).await {
            None if lock_free(home) => return Ok(()),
            None => {}
            Some(r) if r["v"].as_u64() >= Some(u64::from(PROTOCOL_VERSION)) => return Ok(()),
            Some(_) => {}
        }
        if Instant::now() >= deadline {
            let pid = raw_exchange(
                home,
                &format!(r#"{{"v":{v},"request":{{"type":"status"}}}}"#),
            )
            .await
            .and_then(|r| r["result"]["Ok"]["pid"].as_u64());
            let kill = pid.map_or("kill <pid>".to_string(), |p| format!("kill {p}"));
            return Err(unavailable(format!(
                "an older mitosis daemon (protocol {v}) is running and did not stop within 30 s"
            ))
            .with_hint(format!(
                "stop the old daemon: mitosis daemon stop (old binary) or {kill}"
            )));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Sends `req` only if a daemon is already listening; `None` when none is.
pub async fn request_if_running(home: &Home, req: Request) -> Result<Option<Response>> {
    match UnixStream::connect(home.daemon_sock()).await {
        Ok(s) => match exchange(s, &req).await? {
            Reply::Done(r) => Ok(Some(*r)),
            Reply::Older(v) => Err(mismatch(v)),
        },
        Err(e) if not_listening(&e) => Ok(None),
        Err(e) => Err(unavailable(format!(
            "connect to {}: {e}",
            home.daemon_sock().display()
        ))),
    }
}

/// A daemon's answer: the response, or the lower protocol version it speaks.
enum Reply {
    Done(Box<Response>),
    Older(u64),
}

async fn exchange(stream: UnixStream, req: &Request) -> Result<Reply> {
    let (rd, mut wr) = stream.into_split();
    let mut line = serde_json::to_vec(&RequestEnvelope {
        v: PROTOCOL_VERSION,
        request: req.clone(),
    })?;
    line.push(b'\n');
    wr.write_all(&line)
        .await
        .map_err(|e| unavailable(format!("send request: {e}")))?;
    wr.flush()
        .await
        .map_err(|e| unavailable(format!("send request: {e}")))?;

    let mut buf = Vec::new();
    BufReader::new(rd)
        .take(MAX_REPLY + 1)
        .read_until(b'\n', &mut buf)
        .await
        .map_err(|e| unavailable(format!("read reply: {e}")))?;
    if buf.last() != Some(&b'\n') {
        let msg = if buf.len() as u64 > MAX_REPLY {
            "daemon reply too large"
        } else {
            "daemon closed the connection"
        };
        return Err(unavailable(msg));
    }
    decode(&buf)
}

/// Reads one reply line. The version is checked before the shape: another version may not
/// decode at all.
fn decode(buf: &[u8]) -> Result<Reply> {
    let value: serde_json::Value = serde_json::from_slice(buf).map_err(|e| {
        MitosisError::new(
            ErrorCode::ProtocolMismatch,
            format!("unreadable daemon reply: {e}"),
        )
    })?;
    let v = value.get("v").and_then(|v| v.as_u64());
    if v != Some(u64::from(PROTOCOL_VERSION)) {
        return match v {
            Some(v) if v < u64::from(PROTOCOL_VERSION) => Ok(Reply::Older(v)),
            _ => Err(mismatch_text(v)),
        };
    }
    let env: ResponseEnvelope = serde_json::from_value(value).map_err(|e| {
        MitosisError::new(
            ErrorCode::ProtocolMismatch,
            format!("unreadable daemon reply: {e}"),
        )
    })?;
    env.result.map(|r| Reply::Done(Box::new(r)))
}

/// Streams the daemon's event log: calls `on_event` for the latest events that match `filter`
/// and then for each new one, until the daemon stops (`Ok`) or the call fails. Starts the daemon
/// if none runs.
pub async fn follow_events(
    home: &Home,
    filter: EventFilter,
    mut on_event: impl FnMut(EventRow),
) -> Result<()> {
    // An ordinary request first: it starts the daemon, or replaces one from an older release.
    request(home, Request::Status).await?;
    let (rd, mut wr) = connect_started(home).await?.into_split();
    let mut line = serde_json::to_vec(&RequestEnvelope {
        v: PROTOCOL_VERSION,
        request: Request::Events {
            filter,
            follow: true,
        },
    })?;
    line.push(b'\n');
    wr.write_all(&line)
        .await
        .map_err(|e| unavailable(format!("send request: {e}")))?;
    let mut rd = BufReader::new(rd);
    loop {
        let mut buf = Vec::new();
        (&mut rd)
            .take(MAX_REPLY + 1)
            .read_until(b'\n', &mut buf)
            .await
            .map_err(|e| unavailable(format!("read reply: {e}")))?;
        if buf.is_empty() {
            return Ok(());
        }
        if buf.last() != Some(&b'\n') {
            return Err(unavailable("daemon reply too large or cut short"));
        }
        match decode(&buf)? {
            Reply::Done(r) => match *r {
                Response::Events { events } => events.into_iter().for_each(&mut on_event),
                Response::Event(e) => on_event(e),
                _ => return Err(mismatch_text(None)),
            },
            Reply::Older(v) => return Err(mismatch(v)),
        }
    }
}

/// Spawns `current_exe() daemon` for `home` in its own session, logging to `log_path`.
fn spawn_daemon(home: &Home, log_path: &std::path::Path) -> Result<std::process::Child> {
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.arg("daemon")
        .current_dir("/")
        .env("MITOSIS_HOME", &home.root)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    // SAFETY: setsid is async-signal-safe and touches no memory of the parent.
    unsafe {
        cmd.pre_exec(|| {
            nix::unistd::setsid()
                .map(|_| ())
                .map_err(std::io::Error::from)
        });
    }
    Ok(cmd.spawn()?)
}

/// Starts `current_exe() daemon` in its own session and waits until its socket accepts connections.
pub async fn ensure_daemon(home: &Home) -> Result<()> {
    home.ensure_dirs()?;
    // A bad config value surfaces here as invalid_request, not as a daemon that fails to start.
    Config::load(home)?;
    let log_path = home.logs_dir().join("daemon.log");
    // With the login service loaded for this home, launchd starts the daemon, not us.
    let h = home.clone();
    let kicked = tokio::task::spawn_blocking(move || service::manages(&h) && service::kickstart())
        .await
        .unwrap_or(false);
    let mut child = if kicked {
        None
    } else {
        Some(spawn_daemon(home, &log_path)?)
    };

    let mut deadline = Instant::now() + START_TIMEOUT;
    let mut exited = false;
    // A service launchd started logs to service.log; when it is not running a moment after the
    // kickstart (its binary is gone, say), it is not coming up.
    let service_log = home.logs_dir().join("service.log");
    let mut next_service_check = Instant::now() + SERVICE_GRACE;
    // Our daemon found the lock taken; the holder has not bound the socket yet.
    let mut lost_lock = false;
    let mut respawns = 0;
    loop {
        if UnixStream::connect(home.daemon_sock()).await.is_ok() {
            return Ok(());
        }
        if !exited {
            if let Some(Ok(Some(status))) = child.as_mut().map(|c| c.try_wait()) {
                exited = true;
                // Lost the lock to another daemon, which may still be reconciling before it binds:
                // keep the long wait. Any other exit is a failed start: give a racer a moment only.
                if status.code() == Some(i32::from(EXIT_ALREADY_RUNNING)) {
                    lost_lock = true;
                } else {
                    deadline = deadline.min(Instant::now() + EXIT_GRACE);
                }
            }
        }
        // The holder let go without binding: an old daemon that removed its socket and drained,
        // or a daemon that saw a client's lock probe. Start ours again rather than wait it out.
        if kicked && !exited && Instant::now() >= next_service_check {
            next_service_check = Instant::now() + SERVICE_GRACE;
            let running =
                tokio::task::spawn_blocking(|| service::status().is_ok_and(|s| s.running))
                    .await
                    .unwrap_or(true);
            if !running {
                exited = true;
                deadline = deadline.min(Instant::now() + EXIT_GRACE);
            }
        }
        if lost_lock && respawns < RESPAWNS && lock_free(home) {
            child = Some(spawn_daemon(home, &log_path)?);
            (exited, lost_lock) = (false, false);
            respawns += 1;
        }
        if Instant::now() >= deadline {
            let shown = if kicked { &service_log } else { &log_path };
            let tail = crate::pg::cluster::log_tail(shown, 10);
            let why = if kicked && exited {
                "the mitosis login service stopped before the daemon was ready"
            } else if exited {
                "the mitosis daemon exited before it was ready"
            } else {
                "the mitosis daemon did not start within 30 s"
            };
            let tail = if tail.is_empty() {
                String::new()
            } else {
                format!("\n{tail}")
            };
            return Err(unavailable(why).with_hint(format!("see {}{tail}", shown.display())));
        }
        tokio::time::sleep(POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_free_sees_a_held_daemon_lock() {
        let tmp = tempfile::Builder::new()
            .prefix("mt")
            .tempdir_in("/tmp")
            .unwrap();
        let home = Home::at(tmp.path().to_path_buf());
        assert!(lock_free(&home), "no lock file yet");
        let f = std::fs::File::create(home.daemon_lock()).unwrap();
        let held = nix::fcntl::Flock::lock(f, nix::fcntl::FlockArg::LockExclusiveNonblock)
            .map_err(|(_, e)| e)
            .unwrap();
        assert!(!lock_free(&home), "a daemon holds the lock");
        drop(held);
        assert!(lock_free(&home), "the lock was let go");
    }

    #[test]
    fn mismatch_hint_is_not_circular_for_a_newer_daemon() {
        let newer = mismatch_text(Some(u64::from(PROTOCOL_VERSION) + 1));
        assert!(
            newer.hint.as_deref().unwrap().contains("newer"),
            "{newer:?}"
        );
        let older = mismatch_text(Some(1));
        assert_eq!(
            older.hint.as_deref(),
            Some("restart the daemon: mitosis daemon stop")
        );
    }
}
