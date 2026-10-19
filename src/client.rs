use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use crate::protocol::{Request, RequestEnvelope, Response, ResponseEnvelope, PROTOCOL_VERSION};
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
            UnixStream::connect(home.daemon_sock()).await.map_err(|e| {
                unavailable(format!("connect to {}: {e}", home.daemon_sock().display()))
            })?
        }
        Err(e) => {
            return Err(unavailable(format!(
                "connect to {}: {e}",
                home.daemon_sock().display()
            )))
        }
    };
    exchange(stream, req).await
}

/// Sends `req` only if a daemon is already listening; `None` when none is.
pub async fn request_if_running(home: &Home, req: Request) -> Result<Option<Response>> {
    match UnixStream::connect(home.daemon_sock()).await {
        Ok(s) => exchange(s, req).await.map(Some),
        Err(e) if not_listening(&e) => Ok(None),
        Err(e) => Err(unavailable(format!(
            "connect to {}: {e}",
            home.daemon_sock().display()
        ))),
    }
}

async fn exchange(stream: UnixStream, req: Request) -> Result<Response> {
    let (rd, mut wr) = stream.into_split();
    let mut line = serde_json::to_vec(&RequestEnvelope {
        v: PROTOCOL_VERSION,
        request: req,
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
        let theirs = v.map_or("unknown".to_string(), |v| v.to_string());
        return Err(MitosisError::new(
            ErrorCode::ProtocolMismatch,
            format!(
                "daemon speaks protocol {theirs}, this client speaks protocol {PROTOCOL_VERSION}"
            ),
        )
        .with_hint("restart the daemon: mitosis daemon stop"));
    }
    let env: ResponseEnvelope = serde_json::from_value(value).map_err(|e| {
        MitosisError::new(
            ErrorCode::ProtocolMismatch,
            format!("unreadable daemon reply: {e}"),
        )
    })?;
    env.result
}

/// Starts `current_exe() daemon` in its own session and waits until its socket accepts connections.
pub async fn ensure_daemon(home: &Home) -> Result<()> {
    home.ensure_dirs()?;
    let log_path = home.logs_dir().join("daemon.log");
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
    let mut child = cmd.spawn()?;

    let mut deadline = Instant::now() + START_TIMEOUT;
    let mut exited = false;
    loop {
        if UnixStream::connect(home.daemon_sock()).await.is_ok() {
            return Ok(());
        }
        // An early exit is usually AlreadyRunning (another client won the lock): give it a moment to bind.
        if !exited && matches!(child.try_wait(), Ok(Some(_))) {
            exited = true;
            deadline = deadline.min(Instant::now() + EXIT_GRACE);
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
