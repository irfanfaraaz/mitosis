//! Login service: a launchd user agent (macOS) or systemd user unit (Linux) that keeps
//! `mitosis daemon` running.
#[cfg(target_os = "macos")]
use crate::error::{ErrorCode, MitosisError, Result};
#[cfg(target_os = "macos")]
use crate::paths::Home;
use serde::Serialize;
#[cfg(target_os = "macos")]
use std::os::unix::fs::PermissionsExt;
#[cfg(target_os = "macos")]
use std::path::Path;
use std::path::PathBuf;
#[cfg(target_os = "macos")]
use std::process::Command;
#[cfg(target_os = "macos")]
use std::time::Duration;

#[cfg(target_os = "linux")]
mod systemd;
#[cfg(target_os = "linux")]
pub use systemd::{install, kickstart, linger_hint, manages, status, uninstall};

#[cfg(target_os = "macos")]
pub const SERVICE_LABEL: &str = "io.github.irfanfaraaz.mitosis";

#[derive(Serialize, Clone, Debug)]
pub struct ServiceStatus {
    pub installed: bool,
    pub loaded: bool,
    pub running: bool,
    pub binary: Option<PathBuf>,
    pub binary_exists: bool,
}

#[cfg(target_os = "macos")]
fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(target_os = "macos")]
fn unesc(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

/// The launchd plist text. `home` is `$MITOSIS_HOME` when set; `path_env` is the caller's `PATH`.
#[cfg(target_os = "macos")]
pub fn plist(binary: &Path, home: Option<&Path>, path_env: &str) -> String {
    let log_home = home.map(Path::to_path_buf).unwrap_or_else(|| {
        let user = std::env::var_os("HOME").map_or(PathBuf::from("/tmp"), PathBuf::from);
        user.join("Library/Application Support/mitosis")
    });
    let log = esc(&log_home.join("logs/service.log").to_string_lossy());
    let home_env = home.map_or(String::new(), |h| {
        format!(
            "    <key>MITOSIS_HOME</key>\n    <string>{}</string>\n",
            esc(&h.to_string_lossy())
        )
    });
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{SERVICE_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{bin}</string>
    <string>daemon</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>EnvironmentVariables</key>
  <dict>
    <key>MITOSIS_SERVICE</key>
    <string>1</string>
{home_env}    <key>PATH</key>
    <string>{path}</string>
  </dict>
  <key>StandardOutPath</key>
  <string>{log}</string>
  <key>StandardErrorPath</key>
  <string>{log}</string>
  <key>ThrottleInterval</key>
  <integer>30</integer>
  <key>ProcessType</key>
  <string>Background</string>
</dict>
</plist>
"#,
        bin = esc(&binary.to_string_lossy()),
        path = esc(path_env),
    )
}

#[cfg(target_os = "macos")]
pub fn plist_path() -> PathBuf {
    let home = std::env::var_os("HOME").map_or(PathBuf::from("/"), PathBuf::from);
    home.join("Library/LaunchAgents")
        .join(format!("{SERVICE_LABEL}.plist"))
}

#[cfg(target_os = "macos")]
fn target() -> String {
    format!("gui/{}/{SERVICE_LABEL}", unsafe { libc::getuid() })
}

#[cfg(target_os = "macos")]
fn domain() -> String {
    format!("gui/{}", unsafe { libc::getuid() })
}

#[cfg(target_os = "macos")]
fn launchctl(args: &[&str]) -> Result<std::process::Output> {
    Command::new("launchctl")
        .args(args)
        .output()
        .map_err(|e| MitosisError::new(ErrorCode::Internal, format!("run launchctl: {e}")))
}

#[cfg(target_os = "macos")]
fn launchctl_failed(what: &str, o: &std::process::Output) -> MitosisError {
    MitosisError::new(
        ErrorCode::Internal,
        format!(
            "launchctl {what} failed: {}",
            String::from_utf8_lossy(&o.stderr).trim()
        ),
    )
}

/// Calls `cond` up to `tries` times, `pause` apart; true as soon as it holds.
#[cfg(target_os = "macos")]
fn wait_until(tries: u32, pause: Duration, mut cond: impl FnMut() -> bool) -> bool {
    for i in 0..tries {
        if cond() {
            return true;
        }
        if i + 1 < tries {
            std::thread::sleep(pause);
        }
    }
    false
}

/// True when `a` and `b` are the same folder, also through symlinks (`/tmp` and `/private/tmp`).
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn same_folder(a: &Path, b: &Path) -> bool {
    let canon = |p: &Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
    a == b || canon(a) == canon(b)
}

/// The text of the first `<string>` after `<key>key</key>`.
#[cfg(target_os = "macos")]
fn string_after(text: &str, key: &str) -> Option<String> {
    let rest = &text[text.find(&format!("<key>{key}</key>"))?..];
    let rest = &rest[rest.find("<string>")? + "<string>".len()..];
    Some(unesc(&rest[..rest.find("</string>")?]))
}

#[cfg(target_os = "macos")]
pub fn install() -> Result<()> {
    let exe = std::env::current_exe()?;
    let home = std::env::var_os("MITOSIS_HOME").filter(|h| !h.is_empty());
    let home = home.map(std::path::absolute).transpose()?;
    Home::from_env()?.ensure_dirs()?;
    let path_env = std::env::var("PATH").unwrap_or_default();
    let file = plist_path();
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&file, plist(&exe, home.as_deref(), &path_env))?;
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644))?;
    // Reloading an already-loaded service would fail; "not loaded" on this first call is fine.
    let _ = launchctl(&["bootout", &target()])?;
    // bootout returns before launchd has dropped the job; a bootstrap in that gap fails with an
    // I/O error, so wait (bounded) until it is gone.
    wait_until(50, Duration::from_millis(100), || {
        !launchctl(&["print", &target()]).is_ok_and(|o| o.status.success())
    });
    let o = launchctl(&["bootstrap", &domain(), &file.to_string_lossy()])?;
    if !o.status.success() {
        return Err(launchctl_failed("bootstrap", &o));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn uninstall() -> Result<()> {
    let _ = launchctl(&["bootout", &target()])?;
    match std::fs::remove_file(plist_path()) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

#[cfg(target_os = "macos")]
pub fn status() -> Result<ServiceStatus> {
    let text = std::fs::read_to_string(plist_path()).ok();
    let binary = text
        .as_deref()
        .and_then(|t| string_after(t, "ProgramArguments"))
        .map(PathBuf::from);
    let printed = launchctl(&["print", &target()])?;
    let out = String::from_utf8_lossy(&printed.stdout);
    Ok(ServiceStatus {
        installed: text.is_some(),
        loaded: printed.status.success(),
        running: printed.status.success() && out.contains("state = running"),
        binary_exists: binary.as_deref().is_some_and(Path::exists),
        binary,
    })
}

/// True when the installed, loaded service runs the daemon for `home`.
#[cfg(target_os = "macos")]
pub fn manages(home: &Home) -> bool {
    let Ok(text) = std::fs::read_to_string(plist_path()) else {
        return false;
    };
    // A service without MITOSIS_HOME serves the default home, i.e. a caller with the variable unset.
    let theirs = string_after(&text, "MITOSIS_HOME");
    let same = match theirs {
        Some(h) => same_folder(Path::new(&h), &home.root),
        None => std::env::var_os("MITOSIS_HOME").is_none_or(|h| h.is_empty()),
    };
    same && launchctl(&["print", &target()]).is_ok_and(|o| o.status.success())
}

/// Starts the loaded service now; false when launchd refused.
#[cfg(target_os = "macos")]
pub fn kickstart() -> bool {
    launchctl(&["kickstart", &target()]).is_ok_and(|o| o.status.success())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod other {
    use super::ServiceStatus;
    use crate::error::{ErrorCode, MitosisError, Result};
    use crate::paths::Home;

    fn not_built() -> MitosisError {
        MitosisError::new(ErrorCode::Internal, "no login service on this platform")
    }

    pub fn install() -> Result<()> {
        Err(not_built())
    }

    pub fn uninstall() -> Result<()> {
        Err(not_built())
    }

    pub fn status() -> Result<ServiceStatus> {
        Err(not_built())
    }

    pub fn manages(_home: &Home) -> bool {
        false
    }

    pub fn kickstart() -> bool {
        false
    }
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub use other::{install, kickstart, manages, status, uninstall};

/// A warning to show when the daemon would stop at logout (Linux without lingering); none elsewhere.
#[cfg(not(target_os = "linux"))]
pub fn linger_hint() -> Option<String> {
    None
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn plist_has_required_keys() {
        let p = plist(
            Path::new("/opt/bin/mitosis"),
            Some(Path::new("/tmp/m&h")),
            "/usr/bin:/opt/x",
        );
        for want in [
            SERVICE_LABEL,
            "<string>/opt/bin/mitosis</string>",
            "<string>daemon</string>",
            "RunAtLoad",
            "SuccessfulExit",
            "MITOSIS_SERVICE",
            "<string>/usr/bin:/opt/x</string>",
            "<string>/tmp/m&amp;h</string>",
            "/tmp/m&amp;h/logs/service.log",
            "Background",
            "ThrottleInterval",
        ] {
            assert!(p.contains(want), "missing {want}:\n{p}");
        }
        assert!(!plist(Path::new("/b"), None, "/p").contains("MITOSIS_HOME"));
        // Markup characters in the binary and PATH are escaped and read back.
        let odd = plist(Path::new("/a<b>/m&m"), None, "/x<y>:/z");
        assert!(
            odd.contains("<string>/a&lt;b&gt;/m&amp;m</string>"),
            "{odd}"
        );
        assert!(odd.contains("<string>/x&lt;y&gt;:/z</string>"), "{odd}");
        assert_eq!(
            string_after(&odd, "ProgramArguments").as_deref(),
            Some("/a<b>/m&m")
        );
        assert_eq!(
            string_after(&p, "ProgramArguments").as_deref(),
            Some("/opt/bin/mitosis")
        );
        assert_eq!(
            string_after(&p, "MITOSIS_HOME").as_deref(),
            Some("/tmp/m&h")
        );

        // Valid plist, when plutil exists (macOS).
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t.plist");
        std::fs::write(&file, &p).unwrap();
        if let Ok(o) = Command::new("plutil").arg("-lint").arg(&file).output() {
            assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stdout));
        }
    }

    #[test]
    fn wait_until_stops_early_and_gives_up() {
        let mut calls = 0;
        let ok = wait_until(5, Duration::ZERO, || {
            calls += 1;
            calls == 3
        });
        assert!(ok);
        assert_eq!(calls, 3);
        let mut calls = 0;
        assert!(!wait_until(4, Duration::ZERO, || {
            calls += 1;
            false
        }));
        assert_eq!(calls, 4);
    }

    #[test]
    fn homes_match_through_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(same_folder(&real, &link));
        assert!(same_folder(&real, &real));
        assert!(!same_folder(&real, &tmp.path().join("other")));
    }
}
