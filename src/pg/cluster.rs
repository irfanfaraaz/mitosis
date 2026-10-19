use crate::error::{ErrorCode, MitosisError, Result};
use crate::pg::binaries::PgInstall;
use crate::proc_util::{has_exited, is_alive, process_start_time};
use nix::errno::Errno;
use nix::sys::signal::{kill, killpg, Signal};
use nix::unistd::Pid;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct StartOptions {
    pub port: u16,
    pub socket_dir: PathBuf,
    pub listen_tcp: bool,
    pub durable: bool,
    pub log_file: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunningPg {
    pub pid: i32,
    pub start_time: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostmasterInfo {
    pub pid: i32,
    pub data_dir: PathBuf,
    pub port: u16,
    /// Postmaster start time (line 3), unix seconds.
    pub start_time: u64,
}

fn internal(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, msg)
}

pub fn initdb(install: &PgInstall, data_dir: &Path) -> Result<()> {
    let out = Command::new(install.bin("initdb"))
        .arg("-D")
        .arg(data_dir)
        .args(["-U", "postgres", "-E", "UTF8", "--auth-local=trust", "--auth-host=scram-sha-256"])
        .output()?;
    if !out.status.success() {
        return Err(internal(format!("initdb failed: {}", String::from_utf8_lossy(&out.stderr).trim())));
    }
    Ok(())
}

/// macOS limits a Unix socket path (`sun_path`) to 104 bytes including the trailing NUL.
const MAX_SOCKET_PATH: usize = 103;

/// Fails early when postgres could not create its socket file in `socket_dir`.
pub fn check_socket_path(socket_dir: &Path, port: u16) -> Result<()> {
    let path = socket_dir.join(format!(".s.PGSQL.{port}"));
    let len = path.as_os_str().len();
    if len > MAX_SOCKET_PATH {
        return Err(MitosisError::new(
            ErrorCode::PostgresStartFailed,
            format!("socket path too long ({len} bytes, max {MAX_SOCKET_PATH}): {}", path.display()),
        )
        .with_hint("set MITOSIS_HOME to a shorter path"));
    }
    Ok(())
}

/// Spawns postgres in its own process group and returns without waiting for it to be ready.
pub fn start(install: &PgInstall, data_dir: &Path, opts: &StartOptions) -> Result<RunningPg> {
    check_socket_path(&opts.socket_dir, opts.port)?;
    if let Some(parent) = opts.log_file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir_all(&opts.socket_dir)?;
    let log = std::fs::OpenOptions::new().create(true).append(true).open(&opts.log_file)?;
    let log_err = log.try_clone()?;

    let listen = if opts.listen_tcp { "listen_addresses=127.0.0.1" } else { "listen_addresses=" };
    let mut cmd = Command::new(install.bin("postgres"));
    cmd.arg("-D")
        .arg(data_dir)
        .args(["-p", &opts.port.to_string()])
        .arg("-k")
        .arg(&opts.socket_dir)
        .args(["-c", listen, "-c", "shared_buffers=32MB", "-c", "max_connections=20"]);
    if !opts.durable {
        cmd.args(["-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off"]);
    }
    let child = cmd
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(log_err)
        .spawn()
        .map_err(|e| MitosisError::new(ErrorCode::PostgresStartFailed, format!("could not spawn postgres: {e}")))?;
    let pid = child.id() as i32;
    drop(child); // dropping a std Child neither kills nor waits; has_exited reaps it later
    // 0 only if the process died before we could look; wait_ready reports that failure.
    Ok(RunningPg { pid, start_time: process_start_time(pid).unwrap_or(0) })
}

/// Last `n` lines of the last 64 KiB of the file; empty if unreadable.
fn log_tail(path: &Path, n: usize) -> String {
    let read = || -> std::io::Result<String> {
        let mut f = std::fs::File::open(path)?;
        let len = f.metadata()?.len();
        f.seek(SeekFrom::Start(len.saturating_sub(64 * 1024)))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    };
    let text = read().unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

fn start_failure(log_tail: &str, port: u16) -> MitosisError {
    if log_tail.contains("could not bind") || log_tail.contains("Address already in use") {
        MitosisError::new(ErrorCode::PortInUse, format!("port {port} is already in use"))
    } else {
        MitosisError::new(ErrorCode::PostgresStartFailed, format!("postgres exited during startup:\n{log_tail}"))
    }
}

/// On timeout the process is left running; the caller must `stop` it.
pub async fn wait_ready(
    install: &PgInstall,
    pg: &RunningPg,
    opts: &StartOptions,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if has_exited(pg.pid) {
            return Err(start_failure(&log_tail(&opts.log_file, 20), opts.port));
        }
        let ready = tokio::process::Command::new(install.bin("pg_isready"))
            .arg("-h")
            .arg(&opts.socket_dir)
            .args(["-p", &opts.port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?;
        if ready.success() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(MitosisError::new(
                ErrorCode::PostgresStartFailed,
                format!(
                    "postgres not ready after {}s:\n{}",
                    timeout.as_secs(),
                    log_tail(&opts.log_file, 20)
                ),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub fn psql(install: &PgInstall, socket_dir: &Path, port: u16, db: &str, sql: &str) -> Result<String> {
    let out = Command::new(install.bin("psql"))
        .arg("-h")
        .arg(socket_dir)
        .args(["-p", &port.to_string(), "-U", "postgres", "-d", db])
        .args(["-v", "ON_ERROR_STOP=1", "-X", "-Atc", sql])
        .env("PGCONNECT_TIMEOUT", "5")
        .output()?;
    if !out.status.success() {
        return Err(internal(format!("psql failed: {}", String::from_utf8_lossy(&out.stderr).trim())));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Polls `has_exited` (which also reaps our own child) for up to `wait`; true once gone.
fn wait_exited(pid: i32, wait: Duration) -> bool {
    let deadline = Instant::now() + wait;
    while !has_exited(pid) {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    true
}

/// Stops the cluster; a cluster with no `postmaster.pid` is already stopped.
/// Blocks (up to the timeout plus a few seconds): callers in async code use `spawn_blocking`.
pub fn stop(install: &PgInstall, data_dir: &Path, timeout: Duration) -> Result<()> {
    let Some(info) = read_postmaster_pid(data_dir)? else { return Ok(()) };
    let secs = timeout.as_secs().max(1);
    let out = Command::new(install.bin("pg_ctl"))
        .arg("stop")
        .arg("-D")
        .arg(data_dir)
        .args(["-m", "fast", "-t", &secs.to_string()])
        .output()?;
    if out.status.success() {
        wait_exited(info.pid, Duration::from_secs(5)); // reap our child; pg_ctl already confirmed shutdown
        return Ok(());
    }
    if !is_alive(info.pid) {
        return Ok(()); // stale pid file
    }
    // Never signal a pid that is not this cluster's postmaster (stale file + reused pid).
    let ours = process_start_time(info.pid).is_some_and(|t| t.abs_diff(info.start_time) <= 2);
    if !ours {
        return Err(internal(format!(
            "postmaster.pid at {} points to pid {} which is not this cluster's postgres",
            data_dir.display(),
            info.pid
        )));
    }
    let pid = Pid::from_raw(info.pid);
    // The postmaster leads its own group when we started it; otherwise hit just the pid.
    let mut res = killpg(pid, Signal::SIGKILL);
    if res == Err(Errno::ESRCH) {
        res = kill(pid, Signal::SIGKILL);
    }
    match res {
        Ok(()) | Err(Errno::ESRCH) => {}
        Err(e) => return Err(internal(format!("could not kill postgres pid {}: {e}", info.pid))),
    }
    if !wait_exited(info.pid, Duration::from_secs(5)) {
        return Err(internal(format!(
            "postgres pid {} for {} is still alive after SIGKILL",
            info.pid,
            data_dir.display()
        )));
    }
    Ok(())
}

/// Reads lines 1 (pid), 2 (data dir) and 4 (port) of `postmaster.pid`; `None` if the file is absent.
pub fn read_postmaster_pid(data_dir: &Path) -> Result<Option<PostmasterInfo>> {
    let text = match std::fs::read_to_string(data_dir.join("postmaster.pid")) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let lines: Vec<&str> = text.lines().collect();
    let bad = || internal(format!("malformed postmaster.pid in {}", data_dir.display()));
    let pid: i32 = lines.first().and_then(|l| l.trim().parse().ok()).filter(|p| *p > 1).ok_or_else(bad)?;
    let dir = lines.get(1).map(|l| l.trim()).filter(|l| !l.is_empty()).ok_or_else(bad)?;
    let start_time = lines.get(2).and_then(|l| l.trim().parse().ok()).ok_or_else(bad)?;
    let port = lines.get(3).and_then(|l| l.trim().parse().ok()).ok_or_else(bad)?;
    Ok(Some(PostmasterInfo { pid, data_dir: PathBuf::from(dir), port, start_time }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_postmaster_pid() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("postmaster.pid"),
            "4242\n/tmp/data\n1700000000\n54321\n/tmp/sock\nlocalhost\n  1234 5678\nready   \n",
        )
        .unwrap();
        let info = read_postmaster_pid(dir.path()).unwrap().unwrap();
        assert_eq!(
            info,
            PostmasterInfo {
                pid: 4242,
                data_dir: PathBuf::from("/tmp/data"),
                port: 54321,
                start_time: 1_700_000_000
            }
        );
    }

    #[test]
    fn missing_or_malformed_postmaster_pid() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_postmaster_pid(dir.path()).unwrap().is_none());
        std::fs::write(dir.path().join("postmaster.pid"), "oops\n").unwrap();
        assert!(read_postmaster_pid(dir.path()).is_err());
        for pid in ["0", "1", "-1"] {
            std::fs::write(dir.path().join("postmaster.pid"), format!("{pid}\n/tmp/data\n1700000000\n54321\n")).unwrap();
            assert!(read_postmaster_pid(dir.path()).is_err(), "pid {pid} must be rejected");
        }
        std::fs::write(dir.path().join("postmaster.pid"), "4242\n/tmp/data\nnotatime\n54321\n").unwrap();
        assert!(read_postmaster_pid(dir.path()).is_err());
    }

    #[test]
    fn log_tail_keeps_last_lines() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("pg.log");
        let text: String = (1..=30).map(|i| format!("line {i}\n")).collect();
        std::fs::write(&p, text).unwrap();
        let tail = log_tail(&p, 20);
        assert_eq!(tail.lines().count(), 20);
        assert!(tail.starts_with("line 11") && tail.ends_with("line 30"));
        assert_eq!(log_tail(&dir.path().join("none.log"), 20), "");
    }

    #[test]
    fn socket_path_length_is_checked() {
        // "/s/" + 82 chars + "/.s.PGSQL.54321" = 3 + 82 + 15 = 100 bytes
        let ok = PathBuf::from(format!("/s/{}", "a".repeat(82)));
        assert!(check_socket_path(&ok, 54321).is_ok());
        let edge = PathBuf::from(format!("/s/{}", "a".repeat(85))); // exactly 103
        assert!(check_socket_path(&edge, 54321).is_ok());
        let long = PathBuf::from(format!("/s/{}", "a".repeat(86))); // 104
        let e = check_socket_path(&long, 54321).unwrap_err();
        assert_eq!(e.code, ErrorCode::PostgresStartFailed);
        assert!(e.message.starts_with("socket path too long (104 bytes, max 103): /s/"), "{}", e.message);
        assert_eq!(e.hint.as_deref(), Some("set MITOSIS_HOME to a shorter path"));
    }

    #[test]
    fn start_failure_classifies_port_conflicts() {
        let e = start_failure("FATAL:  could not bind IPv4 address \"127.0.0.1\": Address already in use", 54321);
        assert_eq!(e.code, ErrorCode::PortInUse);
        let e = start_failure("FATAL:  data directory has wrong ownership", 54321);
        assert_eq!(e.code, ErrorCode::PostgresStartFailed);
        assert!(e.message.contains("wrong ownership"));
    }
}
