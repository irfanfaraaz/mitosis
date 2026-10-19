use crate::config::Config;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use crate::protocol::{Request, RequestEnvelope, Response, ResponseEnvelope, PROTOCOL_VERSION};
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
        Reply::Done(r) => Ok(r),
        Reply::Older(v) => {
            // A daemon from an older release: stop it (clones keep running), start ours, retry once.
            replace_old(home, v).await?;
            ensure_daemon(home).await?;
            match exchange(connect_started(home).await?, &req).await? {
                Reply::Done(r) => Ok(r),
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
    let theirs = theirs.map_or("unknown".to_string(), |v| v.to_string());
    MitosisError::new(
        ErrorCode::ProtocolMismatch,
        format!("daemon speaks protocol {theirs}, this client speaks protocol {PROTOCOL_VERSION}"),
    )
    .with_hint("restart the daemon: mitosis daemon stop")
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

/// Shuts down a daemon that speaks the older protocol `v` and waits until its socket is gone.
async fn replace_old(home: &Home, v: u64) -> Result<()> {
    // The shutdown request is the one message every version understands.
    raw_exchange(
        home,
        &format!(r#"{{"v":{v},"request":{{"type":"shutdown"}}}}"#),
    )
    .await;
    let deadline = Instant::now() + START_TIMEOUT;
    while UnixStream::connect(home.daemon_sock()).await.is_ok() {
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
    Ok(())
}

/// Sends `req` only if a daemon is already listening; `None` when none is.
pub async fn request_if_running(home: &Home, req: Request) -> Result<Option<Response>> {
    match UnixStream::connect(home.daemon_sock()).await {
        Ok(s) => match exchange(s, &req).await? {
            Reply::Done(r) => Ok(Some(r)),
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
    Done(Response),
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
    // Check the version before the shape: another version may not decode at all.
    let value: serde_json::Value = serde_json::from_slice(&buf).map_err(|e| {
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
    env.result.map(Reply::Done)
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
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
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
        Some(cmd.spawn()?)
    };

    let mut deadline = Instant::now() + START_TIMEOUT;
    let mut exited = false;
    loop {
        if UnixStream::connect(home.daemon_sock()).await.is_ok() {
            return Ok(());
        }
        if !exited {
            if let Some(Ok(Some(status))) = child.as_mut().map(|c| c.try_wait()) {
                exited = true;
                // Lost the lock to another daemon, which may still be reconciling before it binds:
                // keep the long wait. Any other exit is a failed start: give a racer a moment only.
                if status.code() != Some(i32::from(EXIT_ALREADY_RUNNING)) {
                    deadline = deadline.min(Instant::now() + EXIT_GRACE);
                }
            }
        }
        if Instant::now() >= deadline {
            let tail = crate::pg::cluster::log_tail(&log_path, 10);
            let why = if exited {
                "the mitosis daemon exited before it was ready"
            } else {
                "the mitosis daemon did not start within 30 s"
            };
            let tail = if tail.is_empty() {
                String::new()
            } else {
                format!("\n{tail}")
            };
            return Err(unavailable(why).with_hint(format!("see {}{tail}", log_path.display())));
        }
        tokio::time::sleep(POLL).await;
    }
}
