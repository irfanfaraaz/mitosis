//! `.env` injection: put a clone's URL in a project's `.env` file and put the old value back when
//! the clone is destroyed.
//!
//! The editing functions are pure and touch only the one line they are asked about: everything
//! else in the file (comments, order, line endings, a missing final newline) stays as it was.

use crate::client;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::names::random_hex;
use crate::paths::Home;
use crate::protocol::{
    BranchArgs, CloneCreateArgs, CloneInfo, EnvTarget, PendingRestore, Request, Response,
};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

pub const DEFAULT_VAR: &str = "DATABASE_URL";

fn invalid(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::InvalidRequest, msg)
}

/// Where a variable's line sits in the text (all byte offsets).
struct Found {
    /// Start of the line.
    start: usize,
    /// Just after the `=`.
    value_start: usize,
    /// End of the line's text, before `\r\n` / `\n`.
    content_end: usize,
    /// After the line break (or the end of the text).
    end: usize,
}

/// `^\s*(export\s+)?VAR\s*=` on one line; returns the offset just after the `=`.
fn match_line(text: &str, start: usize, content_end: usize, var: &str) -> Option<usize> {
    let mut rest = text[start..content_end].trim_start();
    if let Some(r) = rest.strip_prefix("export") {
        if r.starts_with(char::is_whitespace) {
            rest = r.trim_start();
        }
    }
    let after = rest.strip_prefix(var)?.trim_start().strip_prefix('=')?;
    Some(content_end - after.len())
}

fn find(text: &str, var: &str) -> Option<Found> {
    let mut start = 0;
    while start < text.len() {
        let nl = text[start..].find('\n');
        let end = nl.map_or(text.len(), |i| start + i + 1);
        let mut content_end = if nl.is_some() { end - 1 } else { end };
        if text[start..content_end].ends_with('\r') {
            content_end -= 1;
        }
        if let Some(value_start) = match_line(text, start, content_end, var) {
            return Some(Found {
                start,
                value_start,
                content_end,
                end,
            });
        }
        start = end;
    }
    None
}

/// The value of the first `VAR=` line: bare, `"…"` or `'…'`.
pub fn get_var(text: &str, var: &str) -> Option<String> {
    let f = find(text, var)?;
    let raw = text[f.value_start..f.content_end].trim();
    for q in ['"', '\''] {
        if let Some(inner) = raw.strip_prefix(q).and_then(|r| r.split_once(q)) {
            return Some(inner.0.to_string());
        }
    }
    // A bare value ends at a ` #` comment.
    Some(raw.split(" #").next().unwrap_or(raw).trim().to_string())
}

/// Everything after the `=` of the first `VAR=` line, exactly as written (quotes, spaces and a
/// trailing comment included). What `restore` puts back.
pub fn get_raw(text: &str, var: &str) -> Option<String> {
    let f = find(text, var)?;
    Some(text[f.value_start..f.content_end].to_string())
}

/// Replaces everything after the `=` of the first `VAR=` line with `raw`, verbatim.
fn set_raw(text: &str, var: &str, raw: &str) -> String {
    match find(text, var) {
        Some(f) => format!("{}{raw}{}", &text[..f.value_start], &text[f.content_end..]),
        None => set_var(text, var, raw),
    }
}

/// Sets the first `VAR=` line (keeping everything up to and including the `=`, so a restore of
/// the raw value gives back the identical line), or appends one. The value is written as it is: a
/// clone URL needs no quoting.
pub fn set_var(text: &str, var: &str, value: &str) -> String {
    if let Some(f) = find(text, var) {
        return format!(
            "{}{value}{}",
            &text[..f.value_start],
            &text[f.content_end..]
        );
    }
    let eol = if text.contains("\r\n") { "\r\n" } else { "\n" };
    let mut out = text.to_string();
    if !out.is_empty() && !out.ends_with('\n') {
        out.push_str(eol);
    }
    out.push_str(&format!("{var}={value}{eol}"));
    out
}

/// Removes the first `VAR=` line, line break included.
pub fn remove_var(text: &str, var: &str) -> String {
    match find(text, var) {
        Some(f) => format!("{}{}", &text[..f.start], &text[f.end..]),
        None => text.to_string(),
    }
}

/// The file's text, or `None` when it does not exist.
pub fn read_file(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(t) => Ok(Some(t)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(MitosisError::new(
            ErrorCode::Internal,
            format!("read {}: {e}", path.display()),
        )),
    }
}

/// Replaces the file atomically (temp file in the same folder, then rename). An existing file keeps
/// its mode, a new one gets 0600. A file the owner cannot write is refused, as a plain write would.
pub fn write_file(path: &Path, text: &str) -> Result<()> {
    let io = |e: std::io::Error| {
        MitosisError::new(
            ErrorCode::Internal,
            format!("write {}: {e}", path.display()),
        )
    };
    // A symlinked .env is replaced through the link, not turned into a plain file.
    let target = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mode = match std::fs::metadata(&target) {
        Ok(m) if m.mode() & 0o200 == 0 => {
            return Err(io(std::io::Error::from(ErrorKind::PermissionDenied)))
        }
        Ok(m) => m.mode() & 0o7777,
        Err(e) if e.kind() == ErrorKind::NotFound => 0o600,
        Err(e) => return Err(io(e)),
    };
    let name = target
        .file_name()
        .map_or_else(|| ".env".into(), |n| n.to_string_lossy().into_owned());
    let tmp = target.with_file_name(format!(".{name}.mitosis-{}", random_hex(4)));
    let result = (|| {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(text.as_bytes())?;
        f.sync_all()?;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode))?;
        std::fs::rename(&tmp, &target)
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_file(&tmp);
        return Err(io(e));
    }
    Ok(())
}

/// The file name must start with `.env` (`.env`, `.env.local`, …): other files are not ours to edit.
pub fn check_name(path: &Path) -> Result<()> {
    let ok = path
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with(".env"));
    if ok {
        Ok(())
    } else {
        Err(invalid(format!(
            "{} is not an .env file: its name must start with .env",
            path.display()
        ))
        .with_hint("mitosis clone --env-file .env.local"))
    }
}

/// True when the file is in a git work tree and git does not ignore it (so a commit could pick up
/// the password). False when git or a repository is missing.
pub fn git_unignored(path: &Path) -> bool {
    let Some(dir) = path.parent() else {
        return false;
    };
    std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["check-ignore", "-q"])
        .arg(path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.code() == Some(1))
}

/// Puts the previous value back, but only while the file still holds `rec.url`: a value the user
/// changed since is theirs. `false` (nothing touched) when the value differs or the file is gone.
pub fn restore(rec: &PendingRestore) -> Result<bool> {
    let Some(text) = read_file(&rec.file)? else {
        return Ok(false);
    };
    if get_var(&text, &rec.var).as_deref() != Some(rec.url.as_str()) {
        return Ok(false);
    }
    let new = match &rec.previous {
        Some(prev) => set_raw(&text, &rec.var, prev),
        None => remove_var(&text, &rec.var),
    };
    write_file(&rec.file, &new)?;
    Ok(true)
}

/// Points `var` at `new_url` when the file still holds `old_url` (the clone's URL changed).
/// `false` (nothing touched) when the value differs or the file is gone.
pub fn rewrite(path: &Path, var: &str, old_url: &str, new_url: &str) -> Result<bool> {
    let Some(text) = read_file(path)? else {
        return Ok(false);
    };
    if get_var(&text, var).as_deref() != Some(old_url) {
        return Ok(false);
    }
    write_file(path, &set_var(&text, var, new_url))?;
    Ok(true)
}

/// A variable name a `.env` line can hold: a letter or `_`, then letters, digits or `_`.
pub fn check_var(var: &str) -> Result<()> {
    let mut chars = var.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(invalid(format!("{var} is not a valid variable name")))
    }
}

fn env_target(file: PathBuf, var: Option<String>) -> Result<EnvTarget> {
    let var = var.unwrap_or_else(|| DEFAULT_VAR.to_string());
    check_var(&var)?;
    let file = std::env::current_dir()?.join(file);
    check_name(&file)?;
    let previous = read_file(&file)?.and_then(|t| get_raw(&t, &var));
    Ok(EnvTarget {
        file,
        var,
        previous,
    })
}

/// Completes a `clone --branch` request. The caller sets `name` to the branch it asked for (empty:
/// the branch checked out) and `repo_root` to its working folder; this resolves both with git and
/// adds the caller's `PATH`.
async fn fill_branch(b: &mut BranchArgs) -> Result<()> {
    let cwd = b.repo_root.clone();
    let git = move |args: &[&str]| {
        std::process::Command::new("git")
            .arg("-C")
            .arg(&cwd)
            .args(args)
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    };
    let (root, current) = tokio::task::spawn_blocking(move || {
        (
            git(&["rev-parse", "--show-toplevel"]),
            git(&["branch", "--show-current"]),
        )
    })
    .await
    .map_err(|e| MitosisError::new(ErrorCode::Internal, format!("git: {e}")))?;
    let root = root.filter(|r| !r.is_empty()).ok_or_else(|| {
        invalid(format!(
            "{} is not in a git work tree",
            b.repo_root.display()
        ))
        .with_hint("run mitosis clone --branch inside your git checkout")
    })?;
    let current = current.unwrap_or_default();
    if b.name.is_empty() {
        if current.is_empty() {
            return Err(invalid("the checkout is not on a branch (detached HEAD)")
                .with_hint("name the branch: mitosis clone --branch <branch>"));
        }
        b.name = current;
    } else if !current.is_empty() && current != b.name {
        // The build runs this checkout's migrations, which belong to the branch checked out.
        return Err(invalid(format!(
            "the checkout in {root} is on branch {current}, not {}",
            b.name
        ))
        .with_hint(format!(
            "git switch {}, or run it in a worktree of that branch",
            b.name
        )));
    }
    b.repo_root = PathBuf::from(root);
    if b.path.is_none() {
        b.path = std::env::var("PATH").ok();
    }
    Ok(())
}

/// Creates a clone through the daemon and, with `env_file`, writes its URL into that file
/// (`env_var`, default `DATABASE_URL`). A file that cannot be written is a warning on the returned
/// info: the clone exists and its URL is in the result. With `args.branch` (see `fill_branch`),
/// the clone comes from the template built for that git branch.
pub async fn create_clone(
    home: &Home,
    mut args: CloneCreateArgs,
    env_file: Option<PathBuf>,
    env_var: Option<String>,
) -> Result<CloneInfo> {
    if env_file.is_none() && env_var.is_some() {
        return Err(invalid("an env variable needs an env file"));
    }
    // Before `env_target` reads `previous`: a restore left by a dead clone must put the user's
    // value back first, or the dead clone's URL is recorded as `previous` and the value is lost.
    if let Some(b) = &mut args.branch {
        fill_branch(b).await?;
    }
    let mut pending = finish_pending(home).await;
    args.env = match env_file {
        Some(f) => Some(env_target(f, env_var)?),
        None => None,
    };
    let env = args.env.clone();
    let mut info = match client::request(home, Request::CloneCreate(args)).await? {
        Response::CloneCreated(info) => info,
        _ => return Err(unexpected_reply()),
    };
    if let Some(t) = env {
        let written = read_file(&t.file).and_then(|text| {
            write_file(
                &t.file,
                &set_var(&text.unwrap_or_default(), &t.var, &info.url),
            )
        });
        match written {
            Ok(()) => {
                if git_unignored(&t.file) {
                    info.warnings
                        .push(format!("{} is not ignored by git", t.file.display()));
                }
            }
            Err(e) => info.warnings.push(format!(
                "could not write {} to {}: {}",
                t.var,
                t.file.display(),
                e.message
            )),
        }
    }
    pending.append(&mut info.warnings);
    info.warnings = pending;
    Ok(info)
}

// Not the reply itself: it can hold a password-bearing URL.
fn unexpected_reply() -> MitosisError {
    MitosisError::new(ErrorCode::ProtocolMismatch, "unexpected daemon reply")
        .with_hint("restart the daemon: mitosis daemon stop")
}

/// The URL `mitosis env` prints for clone `id_or_name`: its `url`, or with `direct` its
/// `direct_url`. A direct URL refuses connections while the clone sleeps, so when the printed URL
/// is the direct one (`direct`, or the router off) a sleeping clone is woken first; its port may
/// move on the wake. Returns the URL and the wake's warnings.
pub async fn env_url(home: &Home, id_or_name: &str, direct: bool) -> Result<(String, Vec<String>)> {
    let clones = match client::request(home, Request::CloneList).await? {
        Response::Clones { clones } => clones,
        _ => return Err(unexpected_reply()),
    };
    let c = clones
        .into_iter()
        .find(|c| c.id == id_or_name || c.name.as_deref() == Some(id_or_name))
        .ok_or_else(|| {
            MitosisError::new(
                ErrorCode::CloneNotFound,
                format!("clone {id_or_name} not found"),
            )
            .with_hint("see running clones with `mitosis list`")
        })?;
    let not_usable = match c.status.as_str() {
        "starting" => Some("is still starting; try again in a moment"),
        "stopping" => Some("is being destroyed"),
        _ => None,
    };
    if let Some(why) = not_usable {
        return Err(invalid(format!("clone {id_or_name} {why}")));
    }
    let direct = direct || c.url == c.direct_url;
    if !direct {
        // The router URL works while the clone sleeps: the router wakes it.
        return Ok((c.url, Vec::new()));
    }
    if c.status != "sleeping" {
        return Ok((c.direct_url, Vec::new()));
    }
    // By id, so a name reused meanwhile cannot redirect the wake.
    match client::request(home, Request::CloneWake { id_or_name: c.id }).await? {
        Response::Clone(info) => Ok((info.direct_url, info.warnings)),
        _ => Err(unexpected_reply()),
    }
}

/// Finishes the `.env` restores the daemon could not do, starting the daemon if none runs.
/// Returns warnings, never fails.
pub async fn finish_pending(home: &Home) -> Vec<String> {
    finish_pending_counted(home).await.1
}

/// `finish_pending`, with how many restores were finished: a warning is not always one failed
/// restore (a failed request is one warning for all of them).
pub async fn finish_pending_counted(home: &Home) -> (usize, Vec<String>) {
    match client::request(home, Request::PendingRestores).await {
        Ok(r) => finish_items(home, r).await,
        Err(e) => (0, vec![format!("pending .env restores: {}", e.message)]),
    }
}

/// `finish_pending` when a daemon already runs; nothing (and no daemon start) when none does.
/// For eager callers such as the MCP server start: a later command finishes them instead.
pub async fn finish_pending_if_running(home: &Home) -> Vec<String> {
    match client::request_if_running(home, Request::PendingRestores).await {
        Ok(Some(r)) => finish_items(home, r).await.1,
        Ok(None) => Vec::new(),
        Err(e) => vec![format!("pending .env restores: {}", e.message)],
    }
}

/// Applies the restores in a `PendingRestores` reply. The warnings say they come from an earlier
/// clone, so they are not mistaken for the current command's.
async fn finish_items(home: &Home, reply: Response) -> (usize, Vec<String>) {
    let items = match reply {
        Response::PendingRestores { items } => items,
        _ => {
            return (
                0,
                vec!["unexpected daemon reply to a pending .env restore request".into()],
            )
        }
    };
    let (mut done, mut warnings) = (0, Vec::new());
    for item in items {
        match restore(&item) {
            // Restored, or the user changed the value or removed the file: either way it is done
            // (a row left by a failed `done` request is finished again, harmlessly, next time).
            Ok(_) => {
                done += 1;
                if let Err(e) =
                    client::request(home, Request::PendingRestoreDone { id: item.id }).await
                {
                    warnings.push(format!("pending .env restore: {}", e.message));
                }
            }
            Err(e) => warnings.push(format!(
                "could not restore {} in {} for an earlier clone: {}",
                item.var,
                item.file.display(),
                e.message
            )),
        }
    }
    (done, warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_edit_replaces_first_only() {
        let t = "A=1\nDATABASE_URL=old\nB=2\nDATABASE_URL=again\n";
        assert_eq!(
            set_var(t, "DATABASE_URL", "new"),
            "A=1\nDATABASE_URL=new\nB=2\nDATABASE_URL=again\n"
        );
        assert_eq!(get_var(t, "DATABASE_URL").as_deref(), Some("old"));
        // A longer name is not the variable.
        assert_eq!(get_var("DATABASE_URL_X=1\n", "DATABASE_URL"), None);
    }

    #[test]
    fn env_edit_keeps_crlf() {
        let t = "A=1\r\nDATABASE_URL=old\r\nB=2\r\n";
        assert_eq!(
            set_var(t, "DATABASE_URL", "new"),
            "A=1\r\nDATABASE_URL=new\r\nB=2\r\n"
        );
        assert_eq!(get_var(t, "DATABASE_URL").as_deref(), Some("old"));
        assert_eq!(
            set_var("A=1\r\n", "X", "y"),
            "A=1\r\nX=y\r\n",
            "an appended line uses the file's line ending"
        );
    }

    #[test]
    fn env_edit_no_trailing_newline() {
        assert_eq!(
            set_var("A=1\nDATABASE_URL=old", "DATABASE_URL", "new"),
            "A=1\nDATABASE_URL=new"
        );
        assert_eq!(set_var("A=1", "X", "y"), "A=1\nX=y\n");
    }

    #[test]
    fn env_edit_export_prefix() {
        let t = "  export DATABASE_URL = old\n";
        assert_eq!(get_var(t, "DATABASE_URL").as_deref(), Some("old"));
        assert_eq!(
            set_var(t, "DATABASE_URL", "new"),
            "  export DATABASE_URL =new\n"
        );
    }

    #[test]
    fn env_restore_keeps_spaces_around_equals() {
        let original = "A=1\n  export DATABASE_URL = old # c\nB=2\n";
        let previous = get_raw(original, "DATABASE_URL");
        let url = "postgres://postgres:pw@127.0.0.1:54321/app";
        let written = set_var(original, "DATABASE_URL", url);
        assert_eq!(get_var(&written, "DATABASE_URL").as_deref(), Some(url));
        let restored = set_raw(&written, "DATABASE_URL", previous.as_deref().unwrap());
        assert_eq!(restored, original);
    }

    #[test]
    fn env_edit_quoted_values_read() {
        assert_eq!(get_var("V=\"a b\"\n", "V").as_deref(), Some("a b"));
        assert_eq!(get_var("V='a#b' # note\n", "V").as_deref(), Some("a#b"));
        assert_eq!(get_var("V=bare # note\n", "V").as_deref(), Some("bare"));
        assert_eq!(get_var("V=\n", "V").as_deref(), Some(""));
        assert_eq!(get_var("# V=1\n", "V"), None);
    }

    #[test]
    fn env_edit_appends_when_absent() {
        assert_eq!(set_var("A=1\n", "V", "x"), "A=1\nV=x\n");
    }

    #[test]
    fn env_edit_empty_file() {
        assert_eq!(set_var("", "V", "x"), "V=x\n");
        assert_eq!(remove_var("", "V"), "");
        assert_eq!(get_var("", "V"), None);
    }

    #[test]
    fn env_restore_gives_back_the_identical_line() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join(".env");
        for (line, eol) in [
            ("DATABASE_URL=\"a b # c\"", "\n"),
            ("DATABASE_URL=x # comment", "\n"),
            ("export DATABASE_URL='q' # c", "\r\n"),
            ("export DATABASE_URL = old", "\n"),
            ("DATABASE_URL=", "\r\n"),
        ] {
            let original = format!("A=1{eol}{line}{eol}B=2{eol}");
            let previous = get_raw(&original, "DATABASE_URL");
            let url = "postgres://postgres:pw@127.0.0.1:54321/app".to_string();
            std::fs::write(&p, set_var(&original, "DATABASE_URL", &url)).unwrap();
            let rec = PendingRestore {
                id: 0,
                file: p.clone(),
                var: "DATABASE_URL".into(),
                previous,
                url,
            };
            assert!(restore(&rec).unwrap());
            assert_eq!(std::fs::read_to_string(&p).unwrap(), original);
        }
    }

    #[test]
    fn env_remove_keeps_rest() {
        assert_eq!(remove_var("A=1\nV=x\nB=2\n", "V"), "A=1\nB=2\n");
        assert_eq!(remove_var("A=1\r\nV=x\r\n", "V"), "A=1\r\n");
        assert_eq!(remove_var("A=1\n", "V"), "A=1\n");
    }

    #[test]
    fn check_name_rejects_other_files() {
        assert!(check_name(Path::new("/p/.env")).is_ok());
        assert!(check_name(Path::new("/p/.env.local")).is_ok());
        for bad in ["/p/config.toml", "/p/env", "/p/my.env", "/p/"] {
            let e = check_name(Path::new(bad)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidRequest, "{bad}");
        }
    }

    #[test]
    fn write_file_keeps_mode_and_creates_0600() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join(".env");
        write_file(&p, "A=1\n").unwrap();
        assert_eq!(std::fs::metadata(&p).unwrap().mode() & 0o777, 0o600);
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o640)).unwrap();
        write_file(&p, "A=2\n").unwrap();
        assert_eq!(std::fs::metadata(&p).unwrap().mode() & 0o777, 0o640);
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "A=2\n");
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o400)).unwrap();
        assert!(write_file(&p, "A=3\n").is_err());
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "A=2\n");
    }
}
