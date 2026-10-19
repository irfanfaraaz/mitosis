use nix::errno::Errno;
use nix::sys::signal::kill;
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;
use std::process::Command;

/// True when a process with this pid exists (zombies count as alive).
pub fn is_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false; // 0 and negatives would signal whole groups
    }
    match kill(Pid::from_raw(pid), None) {
        Ok(()) => true,
        Err(e) => e == Errno::EPERM, // exists, but owned by someone else
    }
}

/// True once the process is gone. If it is our own child, this reaps it (so no zombie
/// stays behind); for a process that is not our child it falls back to `!is_alive`.
pub fn has_exited(pid: i32) -> bool {
    if pid <= 0 {
        return true; // waitpid(0/-1) would reap unrelated children
    }
    match waitpid(Pid::from_raw(pid), Some(WaitPidFlag::WNOHANG)) {
        Ok(WaitStatus::StillAlive) => false,
        Ok(_) => true,
        Err(_) => !is_alive(pid), // ECHILD: not our child
    }
}

/// Process start time in seconds since the Unix epoch (macOS `proc_pidinfo`).
pub fn process_start_time(pid: i32) -> Option<u64> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    let n = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size,
        )
    };
    (n == size).then_some(info.pbi_start_tvsec)
}

/// Resident memory in bytes of `pid` plus its direct children. Postgres backends call
/// `setsid()`, so the process group holds only the postmaster; its children are found by ppid.
pub fn rss_bytes(pid: i32) -> Option<u64> {
    let out = Command::new("ps").args(["-axo", "pid=,ppid=,rss="]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    sum_rss(&String::from_utf8_lossy(&out.stdout), pid)
}

/// Sums the `rss` (KiB) of `pid` and every process whose ppid is `pid`, from `pid ppid rss` lines.
/// `None` when `pid` itself is not listed.
fn sum_rss(ps: &str, pid: i32) -> Option<u64> {
    let mut found = false;
    let mut kb = 0u64;
    for line in ps.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        let [p, pp, rss] = f[..] else { continue };
        let (Ok(p), Ok(pp), Ok(rss)) = (p.parse::<i32>(), pp.parse::<i32>(), rss.parse::<u64>()) else { continue };
        if p == pid {
            found = true;
        }
        if p == pid || pp == pid {
            kb += rss;
        }
    }
    found.then_some(kb * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn own_process_is_alive_with_start_time() {
        let me = std::process::id() as i32;
        assert!(is_alive(me));
        assert!(process_start_time(me).unwrap() > 1_600_000_000);
    }

    #[test]
    fn non_positive_pids_are_never_alive() {
        assert!(!is_alive(0) && !is_alive(-1));
        assert!(has_exited(0) && has_exited(-1));
    }

    #[test]
    fn rss_sums_pid_and_children() {
        let ps = "    1     0   100\n  500     1  2000\n  501   500   300\n  502   500    40\n  600   501     7\n  bad\n";
        assert_eq!(sum_rss(ps, 500), Some((2000 + 300 + 40) * 1024)); // grandchild 600 not counted
        assert_eq!(sum_rss(ps, 502), Some(40 * 1024));
        assert_eq!(sum_rss(ps, 999), None);
        assert!(rss_bytes(std::process::id() as i32).unwrap() > 0);
    }

    #[test]
    fn dead_process_is_not_alive() {
        assert!(!is_alive(i32::MAX));
        assert!(process_start_time(i32::MAX).is_none());
    }

    #[test]
    fn has_exited_reaps_our_child() {
        let child = Command::new("true").spawn().unwrap();
        let pid = child.id() as i32;
        drop(child);
        let mut exited = false;
        for _ in 0..100 {
            if has_exited(pid) {
                exited = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(exited);
        assert!(!is_alive(pid)); // reaped, no zombie left
    }
}
