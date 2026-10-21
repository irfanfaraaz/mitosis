//! systemd user unit (`~/.config/systemd/user/mitosis.service`) that keeps `mitosis daemon` running.
use super::{same_folder, ServiceStatus};
use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const UNIT: &str = "mitosis.service";

/// `s` as one systemd word: double-quoted, with `\`, `"`, `%` and `$` escaped.
fn quote(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '%' => out.push_str("%%"),
            '$' => out.push_str("$$"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Reads the text of a word written by `quote` (up to its closing quote); none when unquoted.
fn unquote(s: &str) -> Option<String> {
    let mut chars = s.strip_prefix('"')?.chars().peekable();
    let mut out = String::new();
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some(out),
            '\\' => out.push(chars.next()?),
            '%' | '$' if chars.peek() == Some(&c) => {
                chars.next();
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    None
}

/// The unit file text. `home` is `$MITOSIS_HOME` when set; `path_env` is the caller's `PATH`.
pub fn unit_text(binary: &Path, home: Option<&Path>, path_env: &str) -> String {
    let home_env = home.map_or(String::new(), |h| {
        format!(
            "Environment={}\n",
            quote(&format!("MITOSIS_HOME={}", h.to_string_lossy()))
        )
    });
    format!(
        "[Unit]\n\
         Description=Mitosis daemon\n\
         \n\
         [Service]\n\
         ExecStart={bin} daemon\n\
         Environment=\"MITOSIS_SERVICE=1\"\n\
         {home_env}Environment={path}\n\
         Restart=on-failure\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        bin = quote(&binary.to_string_lossy()),
        path = quote(&format!("PATH={path_env}")),
    )
}

pub fn unit_path() -> PathBuf {
    let home = std::env::var_os("HOME").map_or(PathBuf::from("/"), PathBuf::from);
    home.join(".config/systemd/user").join(UNIT)
}

/// The binary named by the `ExecStart=` line.
fn binary_from_unit(text: &str) -> Option<PathBuf> {
    let rest = text
        .lines()
        .find_map(|l| l.strip_prefix("ExecStart="))?
        .trim();
    Some(PathBuf::from(unquote(rest)?))
}

/// The value of `MITOSIS_HOME` in the unit's `Environment=` lines.
fn home_from_unit(text: &str) -> Option<String> {
    text.lines()
        .filter_map(|l| l.strip_prefix("Environment="))
        .find_map(|v| unquote(v.trim())?.strip_prefix("MITOSIS_HOME=").map(String::from))
}

/// What `systemctl --user show mitosis.service -p ActiveState,MainPID,FragmentPath` says.
#[derive(Debug, PartialEq)]
struct Shown {
    active_state: String,
    main_pid: u32,
    fragment_path: String,
}

fn parse_show(text: &str) -> Shown {
    let mut shown = Shown {
        active_state: String::new(),
        main_pid: 0,
        fragment_path: String::new(),
    };
    for line in text.lines() {
        match line.split_once('=') {
            Some(("ActiveState", v)) => shown.active_state = v.trim().to_string(),
            Some(("MainPID", v)) => shown.main_pid = v.trim().parse().unwrap_or(0),
            Some(("FragmentPath", v)) => shown.fragment_path = v.trim().to_string(),
            _ => {}
        }
    }
    shown
}

fn systemctl(args: &[&str]) -> Result<Output> {
    Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()
        .map_err(|e| MitosisError::new(ErrorCode::Internal, format!("run systemctl: {e}")))
}

fn systemctl_failed(what: &str, o: &Output) -> MitosisError {
    MitosisError::new(
        ErrorCode::Internal,
        format!(
            "systemctl --user {what} failed: {}",
            String::from_utf8_lossy(&o.stderr).trim()
        ),
    )
}

fn show() -> Option<Shown> {
    let o = systemctl(&["show", UNIT, "-p", "ActiveState,MainPID,FragmentPath"]).ok()?;
    o.status
        .success()
        .then(|| parse_show(&String::from_utf8_lossy(&o.stdout)))
}

pub fn install() -> Result<()> {
    let exe = std::env::current_exe()?;
    let home = std::env::var_os("MITOSIS_HOME").filter(|h| !h.is_empty());
    let home = home.map(std::path::absolute).transpose()?;
    Home::from_env()?.ensure_dirs()?;
    let path_env = std::env::var("PATH").unwrap_or_default();
    let file = unit_path();
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&file, unit_text(&exe, home.as_deref(), &path_env))?;
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644))?;
    // `restart` so a reinstall runs the new binary, as launchd's bootout + bootstrap does.
    for args in [&["daemon-reload"][..], &["enable", UNIT][..], &["restart", UNIT][..]] {
        let o = systemctl(args)?;
        if !o.status.success() {
            return Err(systemctl_failed(args[0], &o));
        }
    }
    Ok(())
}

pub fn uninstall() -> Result<()> {
    // Not installed or not loaded is fine here.
    let _ = systemctl(&["disable", "--now", UNIT])?;
    match std::fs::remove_file(unit_path()) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
        _ => {}
    }
    let _ = systemctl(&["daemon-reload"])?;
    Ok(())
}

pub fn status() -> Result<ServiceStatus> {
    let text = std::fs::read_to_string(unit_path()).ok();
    let binary = text.as_deref().and_then(binary_from_unit);
    // A machine without a user systemd reports "not loaded" rather than an error.
    let shown = show();
    let loaded = shown.as_ref().is_some_and(|s| !s.fragment_path.is_empty());
    Ok(ServiceStatus {
        installed: text.is_some(),
        loaded,
        running: loaded
            && shown
                .as_ref()
                .is_some_and(|s| s.active_state == "active" && s.main_pid != 0),
        binary_exists: binary.as_deref().is_some_and(Path::exists),
        binary,
    })
}

/// True when the installed, loaded unit runs the daemon for `home`.
pub fn manages(home: &Home) -> bool {
    let Ok(text) = std::fs::read_to_string(unit_path()) else {
        return false;
    };
    // A unit without MITOSIS_HOME serves the default home, i.e. a caller with the variable unset.
    let same = match home_from_unit(&text) {
        Some(h) => same_folder(Path::new(&h), &home.root),
        None => std::env::var_os("MITOSIS_HOME").is_none_or(|h| h.is_empty()),
    };
    same && show().is_some_and(|s| !s.fragment_path.is_empty())
}

/// Starts the loaded unit now; false when systemd refused.
pub fn kickstart() -> bool {
    systemctl(&["start", UNIT]).is_ok_and(|o| o.status.success())
}

/// `Linger=no` in `loginctl show-user` output.
fn lingering_off(text: &str) -> bool {
    text.lines().any(|l| l.trim() == "Linger=no")
}

/// A warning when the daemon would stop at logout because lingering is off; none otherwise or
/// when it cannot be told.
pub fn linger_hint() -> Option<String> {
    let user = std::env::var("USER").ok().filter(|u| !u.is_empty())?;
    let o = Command::new("loginctl")
        .args(["show-user", &user, "-p", "Linger"])
        .output()
        .ok()?;
    (o.status.success() && lingering_off(&String::from_utf8_lossy(&o.stdout))).then(|| {
        "run \"loginctl enable-linger $USER\" so the daemon keeps running after you log out"
            .to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_text_quotes_paths_with_spaces() {
        let t = unit_text(
            Path::new("/opt/my dir/mitosis"),
            Some(Path::new("/data/my home")),
            "/usr/bin:/opt/x",
        );
        for want in [
            "ExecStart=\"/opt/my dir/mitosis\" daemon\n",
            "Environment=\"MITOSIS_HOME=/data/my home\"\n",
            "Environment=\"PATH=/usr/bin:/opt/x\"\n",
            "Restart=on-failure\n",
            "WantedBy=default.target\n",
        ] {
            assert!(t.contains(want), "missing {want}:\n{t}");
        }
        // Special characters are escaped and read back.
        let odd = unit_text(Path::new("/a\"b/m%m$"), Some(Path::new("/h\\1")), "/p");
        assert!(odd.contains("ExecStart=\"/a\\\"b/m%%m$$\" daemon"), "{odd}");
        assert_eq!(binary_from_unit(&odd), Some(PathBuf::from("/a\"b/m%m$")));
        assert_eq!(home_from_unit(&odd).as_deref(), Some("/h\\1"));
        assert_eq!(
            binary_from_unit(&t),
            Some(PathBuf::from("/opt/my dir/mitosis"))
        );
    }

    #[test]
    fn unit_text_sets_home_only_when_given() {
        let with = unit_text(Path::new("/b"), Some(Path::new("/h")), "/p");
        assert!(with.contains("MITOSIS_HOME=/h"));
        let without = unit_text(Path::new("/b"), None, "/p");
        assert!(!without.contains("MITOSIS_HOME"));
        assert_eq!(home_from_unit(&without), None);
    }

    #[test]
    fn status_parses_systemctl_show_output() {
        let s = parse_show(
            "ActiveState=active\nMainPID=4242\nFragmentPath=/home/u/.config/systemd/user/mitosis.service\n",
        );
        assert_eq!(
            s,
            Shown {
                active_state: "active".into(),
                main_pid: 4242,
                fragment_path: "/home/u/.config/systemd/user/mitosis.service".into(),
            }
        );
        let gone = parse_show("ActiveState=inactive\nMainPID=0\nFragmentPath=\n");
        assert_eq!(gone.active_state, "inactive");
        assert!(gone.fragment_path.is_empty());
        assert!(lingering_off("Linger=no\n"));
        assert!(!lingering_off("Linger=yes\n"));
    }
}
