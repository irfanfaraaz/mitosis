//! Login service: a launchd user agent that keeps `mitosis daemon` running.
use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use serde::Serialize;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

pub const SERVICE_LABEL: &str = "io.github.irfanfaraaz.mitosis";

#[derive(Serialize, Clone, Debug)]
pub struct ServiceStatus {
    pub installed: bool,
    pub loaded: bool,
    pub running: bool,
    pub binary: Option<PathBuf>,
    pub binary_exists: bool,
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn unesc(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

/// The launchd plist text. `home` is `$MITOSIS_HOME` when set; `path_env` is the caller's `PATH`.
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
  <key>ProcessType</key>
  <string>Background</string>
</dict>
</plist>
"#,
        bin = esc(&binary.to_string_lossy()),
        path = esc(path_env),
    )
}

pub fn plist_path() -> PathBuf {
    let home = std::env::var_os("HOME").map_or(PathBuf::from("/"), PathBuf::from);
    home.join("Library/LaunchAgents")
        .join(format!("{SERVICE_LABEL}.plist"))
}

fn target() -> String {
    format!("gui/{}/{SERVICE_LABEL}", unsafe { libc::getuid() })
}

fn domain() -> String {
    format!("gui/{}", unsafe { libc::getuid() })
}

fn launchctl(args: &[&str]) -> Result<std::process::Output> {
    Command::new("launchctl")
        .args(args)
        .output()
        .map_err(|e| MitosisError::new(ErrorCode::Internal, format!("run launchctl: {e}")))
}

fn launchctl_failed(what: &str, o: &std::process::Output) -> MitosisError {
    MitosisError::new(
        ErrorCode::Internal,
        format!(
            "launchctl {what} failed: {}",
            String::from_utf8_lossy(&o.stderr).trim()
        ),
    )
}

/// The text of the first `<string>` after `<key>key</key>`.
fn string_after(text: &str, key: &str) -> Option<String> {
    let rest = &text[text.find(&format!("<key>{key}</key>"))?..];
    let rest = &rest[rest.find("<string>")? + "<string>".len()..];
    Some(unesc(&rest[..rest.find("</string>")?]))
}

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
    let o = launchctl(&["bootstrap", &domain(), &file.to_string_lossy()])?;
    if !o.status.success() {
        return Err(launchctl_failed("bootstrap", &o));
    }
    Ok(())
}

pub fn uninstall() -> Result<()> {
    let _ = launchctl(&["bootout", &target()])?;
    match std::fs::remove_file(plist_path()) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

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
pub fn manages(home: &Home) -> bool {
    let Ok(text) = std::fs::read_to_string(plist_path()) else {
        return false;
    };
    // A service without MITOSIS_HOME serves the default home, i.e. a caller with the variable unset.
    let theirs = string_after(&text, "MITOSIS_HOME");
    let same = match theirs {
        Some(h) => Path::new(&h) == home.root,
        None => std::env::var_os("MITOSIS_HOME").is_none_or(|h| h.is_empty()),
    };
    same && launchctl(&["print", &target()]).is_ok_and(|o| o.status.success())
}

/// Starts the loaded service now; false when launchd refused.
pub fn kickstart() -> bool {
    launchctl(&["kickstart", &target()]).is_ok_and(|o| o.status.success())
}

#[cfg(test)]
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
        ] {
            assert!(p.contains(want), "missing {want}:\n{p}");
        }
        assert!(!plist(Path::new("/b"), None, "/p").contains("MITOSIS_HOME"));
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
}
