use crate::error::{ErrorCode, MitosisError, Result};
use crate::pg::binaries::PgInstall;
use crate::proc_util::{has_exited, is_alive, process_start_time};
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

/// Spawns postgres in its own process group and returns without waiting for it to be ready.
pub fn start(install: &PgInstall, data_dir: &Path, opts: &StartOptions) -> Result<RunningPg> {
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

/// Polls `has_exited` (which also reaps our own child) for up to `wait`.
fn wait_exited(pid: i32, wait: Duration) {
    let deadline = Instant::now() + wait;
    while !has_exited(pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Stops the cluster; a cluster with no `postmaster.pid` is already stopped.
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
        wait_exited(info.pid, Duration::from_secs(5));
        return Ok(());
    }
    if is_alive(info.pid) {
        let pid = Pid::from_raw(info.pid);
        // The postmaster leads its own group when we started it; otherwise hit just the pid.
        if killpg(pid, Signal::SIGKILL).is_err() {
            let _ = kill(pid, Signal::SIGKILL);
        }
        wait_exited(info.pid, Duration::from_secs(5));
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
    let pid = lines.first().and_then(|l| l.trim().parse().ok()).ok_or_else(bad)?;
    let dir = lines.get(1).map(|l| l.trim()).filter(|l| !l.is_empty()).ok_or_else(bad)?;
    let port = lines.get(3).and_then(|l| l.trim().parse().ok()).ok_or_else(bad)?;
    Ok(Some(PostmasterInfo { pid, data_dir: PathBuf::from(dir), port }))
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
        assert_eq!(info, PostmasterInfo { pid: 4242, data_dir: PathBuf::from("/tmp/data"), port: 54321 });
    }

    #[test]
    fn missing_or_malformed_postmaster_pid() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_postmaster_pid(dir.path()).unwrap().is_none());
        std::fs::write(dir.path().join("postmaster.pid"), "oops\n").unwrap();
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
    fn start_failure_classifies_port_conflicts() {
        let e = start_failure("FATAL:  could not bind IPv4 address \"127.0.0.1\": Address already in use", 54321);
        assert_eq!(e.code, ErrorCode::PortInUse);
        let e = start_failure("FATAL:  data directory has wrong ownership", 54321);
        assert_eq!(e.code, ErrorCode::PostgresStartFailed);
        assert!(e.message.contains("wrong ownership"));
    }
}
