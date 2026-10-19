use nix::errno::Errno;
use nix::sys::signal::kill;
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;
use std::process::Command;

/// True when a process with this pid exists (zombies count as alive).
pub fn is_alive(pid: i32) -> bool {
    match kill(Pid::from_raw(pid), None) {
        Ok(()) => true,
        Err(e) => e == Errno::EPERM, // exists, but owned by someone else
    }
}

/// True once the process is gone. If it is our own child, this reaps it (so no zombie
/// stays behind); for a process that is not our child it falls back to `!is_alive`.
pub fn has_exited(pid: i32) -> bool {
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

/// Total resident memory in bytes of every process in process group `pgid`.
pub fn rss_bytes(pgid: i32) -> Option<u64> {
    let out = Command::new("ps").args(["-o", "rss=", "-g", &pgid.to_string()]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let kb: Vec<u64> =
        String::from_utf8_lossy(&out.stdout).lines().filter_map(|l| l.trim().parse().ok()).collect();
    (!kb.is_empty()).then(|| kb.iter().sum::<u64>() * 1024)
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
