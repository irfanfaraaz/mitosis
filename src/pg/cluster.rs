use crate::error::{ErrorCode, MitosisError, Result};
use crate::pg::binaries::PgInstall;
use crate::proc_util::{has_exited, is_alive, process_cwd, process_start_time};
use nix::errno::Errno;
use nix::sys::signal::{kill, killpg, Signal};
use nix::unistd::Pid;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
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
        .args([
            "-U",
            "postgres",
            "-E",
            "UTF8",
            "--auth-local=trust",
            "--auth-host=scram-sha-256",
        ])
        .output()?;
    if !out.status.success() {
        return Err(internal(format!(
            "initdb failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
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
            format!(
                "socket path too long ({len} bytes, max {MAX_SOCKET_PATH}): {}",
                path.display()
            ),
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
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&opts.log_file)?;
    // The log is appended across tries; wait_ready classifies only what follows the last marker.
    let tries = std::fs::read_to_string(&opts.log_file)
        .map(|t| t.matches(TRY_MARKER).count())
        .unwrap_or(0);
    writeln!(&log, "{TRY_MARKER} {}", tries + 1)?;
    let log_err = log.try_clone()?;

    let listen = if opts.listen_tcp {
        "listen_addresses=127.0.0.1"
    } else {
        "listen_addresses="
    };
    let mut cmd = Command::new(install.bin("postgres"));
    // macOS postgres aborts ("postmaster became multithreaded") when no locale is set in the
    // environment; database collation comes from initdb, so C here changes nothing else.
    cmd.env("LC_ALL", "C");
    cmd.arg("-D")
        .arg(data_dir)
        .args(["-p", &opts.port.to_string()])
        .arg("-k")
        .arg(&opts.socket_dir)
        .args([
            "-c",
            listen,
            "-c",
            "shared_buffers=32MB",
            "-c",
            "max_connections=20",
        ]);
    if !opts.durable {
        cmd.args([
            "-c",
            "fsync=off",
            "-c",
            "synchronous_commit=off",
            "-c",
            "full_page_writes=off",
        ]);
    }
    let child = cmd
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(log_err)
        .spawn()
        .map_err(|e| {
            MitosisError::new(
                ErrorCode::PostgresStartFailed,
                format!("could not spawn postgres: {e}"),
            )
        })?;
    let pid = child.id() as i32;
    drop(child); // dropping a std Child neither kills nor waits; has_exited reaps it later
                 // 0 only if the process died before we could look; wait_ready reports that failure.
    Ok(RunningPg {
        pid,
        start_time: process_start_time(pid).unwrap_or(0),
    })
}

const TRY_MARKER: &str = "mitosis: start try";

/// Like `log_tail`, but only the lines after the last start-try marker (the current try).
fn current_try_tail(path: &Path, n: usize) -> String {
    let tail = log_tail(path, usize::MAX);
    let after = match tail.rfind(TRY_MARKER) {
        Some(i) => tail[i..].split_once('\n').map_or("", |(_, rest)| rest),
        None => &tail,
    };
    let lines: Vec<&str> = after.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

/// Last `n` lines of the last 64 KiB of the file; empty if unreadable.
pub(crate) fn log_tail(path: &Path, n: usize) -> String {
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
        MitosisError::new(
            ErrorCode::PortInUse,
            format!("port {port} is already in use"),
        )
    } else {
        MitosisError::new(
            ErrorCode::PostgresStartFailed,
            format!("postgres exited during startup:\n{log_tail}"),
        )
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
            return Err(start_failure(
                &current_try_tail(&opts.log_file, 20),
                opts.port,
            ));
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
                    current_try_tail(&opts.log_file, 20)
                ),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Runs `sql` with psql over the socket. The SQL goes on stdin, never in argv, so `ps` cannot show
/// secrets in it (e.g. `ALTER ROLE ... PASSWORD`).
pub fn psql(
    install: &PgInstall,
    socket_dir: &Path,
    port: u16,
    db: &str,
    sql: &str,
) -> Result<String> {
    let mut child = Command::new(install.bin("psql"))
        .arg("-h")
        .arg(socket_dir)
        .args(["-p", &port.to_string(), "-U", "postgres", "-d", db])
        .args(["-v", "ON_ERROR_STOP=1", "-X", "-A", "-t", "-q", "-f", "-"])
        .env("PGCONNECT_TIMEOUT", "5")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().expect("psql stdin is piped");
    let input = sql.to_owned();
    // Written from a thread so a large output cannot deadlock against a large input.
    let writer = std::thread::spawn(move || stdin.write_all(input.as_bytes())); // stdin closes on drop
    let out = child.wait_with_output()?;
    let written = writer
        .join()
        .unwrap_or_else(|_| Err(std::io::Error::other("stdin writer panicked")));
    if !out.status.success() {
        return Err(internal(format!(
            "psql failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    written.map_err(|e| internal(format!("could not send SQL to psql: {e}")))?;
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

/// True while `pg` still runs as the same process: same pid and exactly the same kernel start
/// time. A dead child (zombie) is reaped and counts as gone.
fn is_same_process(pg: RunningPg) -> bool {
    pg.start_time != 0 && !has_exited(pg.pid) && process_start_time(pg.pid) == Some(pg.start_time)
}

/// Whether `pid` serves `data_dir`. Postgres chdirs into its data dir, so its cwd names it exactly.
/// `Some(false)` when it provably does not (gone, another user's process, another folder);
/// `None` when that cannot be told.
fn serves_dir(pid: i32, data_dir: &Path) -> Option<bool> {
    match process_cwd(pid) {
        Ok((dev, ino, path)) => {
            let same_inode =
                std::fs::metadata(data_dir).is_ok_and(|m| (m.dev(), m.ino()) == (dev, ino));
            let same_path = data_dir.canonicalize().is_ok_and(|d| d == path);
            Some(same_inode || same_path)
        }
        Err(e) if matches!(e.raw_os_error(), Some(libc::ESRCH) | Some(libc::EPERM)) => Some(false),
        Err(_) => None,
    }
}

/// The postmaster now serving `data_dir`, identified exactly: `expected` (the server we started,
/// as recorded) while it still runs, else the pid in `postmaster.pid` when that process runs in
/// `data_dir`. `None` when no server of this folder runs; an error when a live pid cannot be told apart.
pub fn running_server(data_dir: &Path, expected: Option<RunningPg>) -> Result<Option<RunningPg>> {
    if let Some(pg) = expected.filter(|pg| is_same_process(*pg)) {
        return Ok(Some(pg));
    }
    let Some(info) = read_postmaster_pid(data_dir)? else {
        return Ok(None);
    };
    if has_exited(info.pid) {
        return Ok(None);
    }
    // Start time read on both sides of the cwd check, so a pid reused in between is caught.
    let before = process_start_time(info.pid);
    match serves_dir(info.pid, data_dir) {
        Some(true) => {}
        Some(false) => return Ok(None),
        None => {
            return Err(internal(format!(
                "cannot tell whether pid {} (from postmaster.pid) serves {}",
                info.pid,
                data_dir.display()
            )))
        }
    }
    match (before, process_start_time(info.pid)) {
        (Some(a), Some(b)) if a == b => Ok(Some(RunningPg {
            pid: info.pid,
            start_time: a,
        })),
        _ => Ok(None), // exited (or replaced) during the check
    }
}

/// Removes `postmaster.pid` when the pid it names is no longer alive (a crash or reboot).
fn remove_dead_pid_file(data_dir: &Path) -> Result<()> {
    let Some(info) = read_postmaster_pid(data_dir).ok().flatten() else {
        return Ok(());
    };
    if is_alive(info.pid) {
        return Ok(()); // a live process that is not this cluster's server: leave its file alone
    }
    match std::fs::remove_file(data_dir.join("postmaster.pid")) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

/// Stops the cluster's server, identified by `running_server` (pass the recorded identity when
/// there is one). Nothing running is success; a pid that is not this cluster's server is never
/// signalled. Blocks (up to the timeout plus a few seconds): async callers use `spawn_blocking`.
pub fn stop(
    install: &PgInstall,
    data_dir: &Path,
    expected: Option<RunningPg>,
    timeout: Duration,
) -> Result<()> {
    let Some(pg) = running_server(data_dir, expected)? else {
        return remove_dead_pid_file(data_dir);
    };
    // pg_ctl signals whatever pid the file names, so it is used only when that is our server.
    let file_pid = read_postmaster_pid(data_dir).ok().flatten().map(|i| i.pid);
    if file_pid == Some(pg.pid) {
        let secs = timeout.as_secs().max(1);
        let out = Command::new(install.bin("pg_ctl"))
            .arg("stop")
            .arg("-D")
            .arg(data_dir)
            .args(["-m", "fast", "-t", &secs.to_string()])
            .output()?;
        if out.status.success() {
            wait_exited(pg.pid, Duration::from_secs(5)); // reap our child; pg_ctl already confirmed shutdown
            return Ok(());
        }
    }
    // Re-checked right before the kill: the pid may have exited and been reused meanwhile.
    if !is_same_process(pg) {
        return remove_dead_pid_file(data_dir);
    }
    let pid = Pid::from_raw(pg.pid);
    // The postmaster leads its own group when we started it; otherwise hit just the pid.
    let mut res = killpg(pid, Signal::SIGKILL);
    if res == Err(Errno::ESRCH) {
        res = kill(pid, Signal::SIGKILL);
    }
    match res {
        Ok(()) | Err(Errno::ESRCH) => {}
        Err(e) => {
            return Err(internal(format!(
                "could not kill postgres pid {}: {e}",
                pg.pid
            )))
        }
    }
    if !wait_exited(pg.pid, Duration::from_secs(5)) {
        return Err(internal(format!(
            "postgres pid {} for {} is still alive after SIGKILL",
            pg.pid,
            data_dir.display()
        )));
    }
    remove_dead_pid_file(data_dir)
}

/// `Database cluster state` from `pg_control`, as `pg_controldata` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlState {
    /// Shut down cleanly (`shut down` or `shut down in recovery`): every write reached disk.
    ShutDown,
    InProduction,
    Other(String),
}

/// The cluster state from `pg_controldata` output; `None` when the line is missing.
pub fn parse_control_state(text: &str) -> Option<ControlState> {
    let state = text
        .lines()
        .find_map(|l| l.strip_prefix("Database cluster state:"))?
        .trim();
    Some(match state {
        "shut down" | "shut down in recovery" => ControlState::ShutDown,
        "in production" => ControlState::InProduction,
        other => ControlState::Other(other.to_string()),
    })
}

/// Runs `pg_controldata` on a stopped (or crashed) cluster and reads its state.
pub fn control_state(install: &PgInstall, data_dir: &Path) -> Result<ControlState> {
    let out = Command::new(install.bin("pg_controldata"))
        .arg("-D")
        .arg(data_dir)
        .env("LC_ALL", "C") // the labels are translated otherwise
        .output()?;
    let text = String::from_utf8_lossy(&out.stdout);
    match parse_control_state(&text) {
        Some(s) if out.status.success() => Ok(s),
        _ => Err(internal(format!(
            "pg_controldata failed for {}: {}",
            data_dir.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        ))),
    }
}

/// Reads lines 1-4 of `postmaster.pid` (pid, data dir, start time, port);
/// `None` if the file is absent.
pub fn read_postmaster_pid(data_dir: &Path) -> Result<Option<PostmasterInfo>> {
    let text = match std::fs::read_to_string(data_dir.join("postmaster.pid")) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let lines: Vec<&str> = text.lines().collect();
    let bad = || {
        internal(format!(
            "malformed postmaster.pid in {}",
            data_dir.display()
        ))
    };
    let pid: i32 = lines
        .first()
        .and_then(|l| l.trim().parse().ok())
        .filter(|p| *p > 1)
        .ok_or_else(bad)?;
    let dir = lines
        .get(1)
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .ok_or_else(bad)?;
    let start_time = lines
        .get(2)
        .and_then(|l| l.trim().parse().ok())
        .ok_or_else(bad)?;
    let port = lines
        .get(3)
        .and_then(|l| l.trim().parse().ok())
        .ok_or_else(bad)?;
    Ok(Some(PostmasterInfo {
        pid,
        data_dir: PathBuf::from(dir),
        port,
        start_time,
    }))
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
    fn stop_never_runs_pg_ctl_for_a_foreign_or_dead_pid() {
        // A bin dir with no pg_ctl: reaching pg_ctl would fail with an io error instead.
        let install = PgInstall {
            bin_dir: PathBuf::from("/nonexistent"),
            major: 16,
            version: "16.0".into(),
        };
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("postmaster.pid");

        // A live process that does not run in the data dir (this test) is not its server:
        // nothing is signalled and its pid file stays.
        let me = std::process::id();
        std::fs::write(&pid_file, format!("{me}\n/tmp/data\n1000\n54321\n")).unwrap();
        assert_eq!(running_server(dir.path(), None).unwrap(), None);
        stop(&install, dir.path(), None, Duration::from_secs(1)).unwrap();
        assert!(pid_file.exists() && is_alive(me as i32));

        // A recorded identity with the wrong start time is not trusted either.
        let wrong = RunningPg {
            pid: me as i32,
            start_time: process_start_time(me as i32).unwrap() - 100,
        };
        stop(&install, dir.path(), Some(wrong), Duration::from_secs(1)).unwrap();
        assert!(is_alive(me as i32));

        let mut child = Command::new("true").spawn().unwrap();
        let dead = child.id();
        child.wait().unwrap();
        std::fs::write(&pid_file, format!("{dead}\n/tmp/data\n1000\n54321\n")).unwrap();
        stop(&install, dir.path(), None, Duration::from_secs(1)).unwrap();
        assert!(
            !pid_file.exists(),
            "a pid file whose pid is dead is removed"
        );
    }

    #[test]
    fn a_process_running_in_the_data_dir_is_its_server() {
        let dir = tempfile::tempdir().unwrap();
        let mut child = Command::new("sleep")
            .arg("30")
            .current_dir(dir.path())
            .spawn()
            .unwrap();
        let pid = child.id() as i32;
        std::fs::write(
            dir.path().join("postmaster.pid"),
            format!("{pid}\n/tmp/data\n1\n54321\n"),
        )
        .unwrap();
        let found = running_server(dir.path(), None).unwrap();
        let start = process_start_time(pid);
        let other = tempfile::tempdir().unwrap();
        std::fs::copy(
            dir.path().join("postmaster.pid"),
            other.path().join("postmaster.pid"),
        )
        .unwrap();
        let elsewhere = running_server(other.path(), None).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        assert_eq!(
            found,
            Some(RunningPg {
                pid,
                start_time: start.unwrap()
            }),
            "the recorded start time is the kernel's, not line 3 of the pid file"
        );
        assert_eq!(
            elsewhere, None,
            "same pid file, other folder: not its server"
        );
        assert_eq!(running_server(dir.path(), None).unwrap(), None, "exited");
    }

    #[test]
    fn missing_or_malformed_postmaster_pid() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_postmaster_pid(dir.path()).unwrap().is_none());
        std::fs::write(dir.path().join("postmaster.pid"), "oops\n").unwrap();
        assert!(read_postmaster_pid(dir.path()).is_err());
        for pid in ["0", "1", "-1"] {
            std::fs::write(
                dir.path().join("postmaster.pid"),
                format!("{pid}\n/tmp/data\n1700000000\n54321\n"),
            )
            .unwrap();
            assert!(
                read_postmaster_pid(dir.path()).is_err(),
                "pid {pid} must be rejected"
            );
        }
        std::fs::write(
            dir.path().join("postmaster.pid"),
            "4242\n/tmp/data\nnotatime\n54321\n",
        )
        .unwrap();
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
        assert!(
            e.message
                .starts_with("socket path too long (104 bytes, max 103): /s/"),
            "{}",
            e.message
        );
        assert_eq!(
            e.hint.as_deref(),
            Some("set MITOSIS_HOME to a shorter path")
        );
    }

    #[test]
    fn start_failure_classifies_port_conflicts() {
        let e = start_failure(
            "FATAL:  could not bind IPv4 address \"127.0.0.1\": Address already in use",
            54321,
        );
        assert_eq!(e.code, ErrorCode::PortInUse);
        let e = start_failure("FATAL:  data directory has wrong ownership", 54321);
        assert_eq!(e.code, ErrorCode::PostgresStartFailed);
        assert!(e.message.contains("wrong ownership"));
    }

    #[test]
    fn current_try_tail_ignores_earlier_tries() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("pg.log");
        std::fs::write(
            &p,
            "mitosis: start try 1\nFATAL: Address already in use\nmitosis: start try 2\nFATAL: other\n",
        )
        .unwrap();
        assert_eq!(current_try_tail(&p, 20), "FATAL: other");
        std::fs::write(&p, "no marker\n").unwrap();
        assert_eq!(current_try_tail(&p, 20), "no marker");
    }

    #[test]
    fn parse_control_state_cases() {
        let sample = |state: &str| {
            format!(
                "pg_control version number:            1300\n\
                 Catalog version number:               202307071\n\
                 Database system identifier:           7300000000000000000\n\
                 Database cluster state:               {state}\n\
                 pg_control last modified:             Mon Oct 19 10:00:00 2026\n"
            )
        };
        assert_eq!(
            parse_control_state(&sample("shut down")),
            Some(ControlState::ShutDown)
        );
        assert_eq!(
            parse_control_state(&sample("shut down in recovery")),
            Some(ControlState::ShutDown)
        );
        assert_eq!(
            parse_control_state(&sample("in production")),
            Some(ControlState::InProduction)
        );
        assert_eq!(
            parse_control_state(&sample("in crash recovery")),
            Some(ControlState::Other("in crash recovery".into()))
        );
        assert_eq!(
            parse_control_state("pg_control version number: 1300\n"),
            None
        );
    }
}
