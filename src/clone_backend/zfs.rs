//! ZFS back end (Linux): every generation and clone directory is a dataset under `zfs_dataset`,
//! mounted at exactly the path the store records. A clone is a `zfs clone` of the one snapshot
//! `<generation>@mitosis`, taken at the first clone and reused by the rest.
//!
//! Linux cannot delegate mount, unmount, mountpoint, canmount or rename through `zfs allow`, so
//! those steps run as `sudo -n zfs ...` under the sudoers rule `mitosis zfs setup` prints. `-n`
//! means sudo never waits for a password: a refusal is `backend_unsupported` with the fix.

use super::{fs_remove_dir, fs_rename_dir, unsupported, CloneBackend};
use crate::error::{ErrorCode, MitosisError, Result};
use std::collections::HashSet;
use std::fs::Permissions;
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Longest any one `zfs` or `sudo` run may take.
const TIMEOUT: Duration = Duration::from_secs(60);
/// The one snapshot of a generation that its clones are made from.
const SNAPSHOT: &str = "mitosis";
/// What `zfs allow` must give the user. `mount` is listed because zfs-allow(8) requires it for
/// create, clone and destroy, even though only root can actually mount on Linux.
pub const DELEGATED: &str = "create,destroy,mount,snapshot,clone,send,receive,canmount,mountpoint";

pub fn setup_hint(dataset: &str) -> String {
    format!("run \"mitosis zfs setup {dataset}\" and give the printed lines to an admin")
}

fn internal(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, msg)
}

pub struct ZfsBackend {
    /// `zfs_dataset`: the parent dataset Mitosis owns.
    root: String,
    /// Where `root` is mounted; a path under it maps to the dataset of the same relative name.
    mount_root: PathBuf,
    zfs: PathBuf,
    sudo: PathBuf,
    /// Folders `remove_dir` left in place because clones depend on them; see `take_kept`.
    kept: Mutex<Vec<PathBuf>>,
}

impl ZfsBackend {
    /// The back end for dataset `root`, mounted where `zfs get mountpoint` says.
    pub fn new(root: &str) -> Result<ZfsBackend> {
        check_dataset_name(root)?;
        let zfs = which_zfs().unwrap_or_else(|| PathBuf::from("zfs"));
        let mut b = ZfsBackend::with_programs(root, PathBuf::new(), zfs, PathBuf::from("sudo"));
        let out = b.run_zfs(&["get", "-H", "-o", "value", "mountpoint", root], false)?;
        let mp = out.trim();
        if !mp.starts_with('/') {
            return Err(unsupported(
                format!("ZFS dataset {root} has mountpoint '{mp}', not a folder"),
                format!("as root: zfs set mountpoint=/path/to/folder {root}, then put MITOSIS_HOME under it"),
            ));
        }
        b.mount_root = PathBuf::from(mp);
        Ok(b)
    }

    fn with_programs(root: &str, mount_root: PathBuf, zfs: PathBuf, sudo: PathBuf) -> ZfsBackend {
        ZfsBackend {
            root: root.to_string(),
            mount_root,
            zfs,
            sudo,
            kept: Mutex::new(Vec::new()),
        }
    }

    /// True when `path` is under the dataset's mountpoint.
    pub fn contains(&self, path: &Path) -> bool {
        path.starts_with(&self.mount_root)
    }

    /// The dataset for `path`: `root` plus the path relative to `mount_root`, `/`-joined. Bytes
    /// ZFS does not allow in a name (`@` of `main@3`, `~` of a branch folder) become `:` and two
    /// hex digits, so `templates/main@3` is `<root>/templates/main:403`.
    fn dataset(&self, path: &Path) -> Result<String> {
        let rel = path.strip_prefix(&self.mount_root).map_err(|_| {
            unsupported(
                format!(
                    "{} is not under {}, the mountpoint of ZFS dataset {}",
                    path.display(),
                    self.mount_root.display(),
                    self.root
                ),
                "set MITOSIS_HOME to a folder under the mountpoint, or zfs_dataset to the dataset that holds it",
            )
        })?;
        let mut ds = self.root.clone();
        for c in rel.components() {
            let Component::Normal(part) = c else {
                return Err(internal(format!("unexpected path {}", path.display())));
            };
            ds.push('/');
            for &b in part.as_bytes() {
                if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.') {
                    ds.push(b as char);
                } else {
                    ds.push_str(&format!(":{b:02x}"));
                }
            }
        }
        if ds == self.root {
            return Err(internal(format!(
                "{} is the mountpoint of {} itself, not a folder in it",
                path.display(),
                self.root
            )));
        }
        Ok(ds)
    }

    /// Runs `zfs <args>`, or `sudo -n zfs <args>` when `sudo`; returns stdout. A failure names
    /// the command and carries zfs's stderr; sudo refusing (no rule, or it wants a password) is
    /// `backend_unsupported` with the `zfs setup` fix.
    fn run_zfs(&self, args: &[&str], sudo: bool) -> Result<String> {
        let mut cmd = if sudo {
            let mut c = Command::new(&self.sudo);
            c.arg("-n").arg(&self.zfs);
            c
        } else {
            Command::new(&self.zfs)
        };
        cmd.args(args);
        let shown = format!("{}zfs {}", if sudo { "sudo -n " } else { "" }, args.join(" "));
        let out = match output_within(cmd, TIMEOUT) {
            Ok(Some(out)) => out,
            Ok(None) => {
                return Err(internal(format!(
                    "{shown}: no answer after {} s",
                    TIMEOUT.as_secs()
                )))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(if sudo {
                    unsupported(format!("{shown}: sudo is not installed"), setup_hint(&self.root))
                } else {
                    unsupported(
                        format!("{shown}: zfs is not installed"),
                        "install the ZFS tools (zfsutils-linux)",
                    )
                })
            }
            Err(e) => return Err(internal(format!("{shown}: {e}"))),
        };
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        if out.status.success() {
            return Ok(String::from_utf8_lossy(&out.stdout).into_owned());
        }
        if sudo && sudo_refused(&stderr, out.status.code()) {
            return Err(unsupported(
                format!("{shown} was refused: {stderr}"),
                setup_hint(&self.root),
            ));
        }
        Err(internal(format!("{shown} failed: {stderr}")))
    }

    /// True when the dataset or snapshot `name` exists.
    fn exists(&self, name: &str) -> Result<bool> {
        match self.run_zfs(&["list", "-H", "-o", "name", "-t", "all", name], false) {
            Ok(_) => Ok(true),
            Err(e) if e.message.contains("does not exist") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Creates the missing datasets between `root` and `ds` (for example `<root>/clones`) with
    /// `canmount=off`: they only hold children, and the home's plain folders stay visible.
    fn ensure_parents(&self, ds: &str) -> Result<()> {
        let mut missing = Vec::new();
        let mut cur = ds;
        while let Some((parent, _)) = cur.rsplit_once('/') {
            if parent.len() <= self.root.len() || self.exists(parent)? {
                break;
            }
            missing.push(parent);
            cur = parent;
        }
        for p in missing.into_iter().rev() {
            match self.run_zfs(&["create", "-o", "canmount=off", p], false) {
                Err(e) if e.message.contains("already exists") => {} // made by a parallel call
                r => {
                    r?;
                }
            }
        }
        Ok(())
    }

    /// Mounts `ds` at `path` (both steps need root on Linux).
    fn mount_at(&self, ds: &str, path: &Path) -> Result<()> {
        let mp = format!("mountpoint={}", path.display());
        self.run_zfs(&["set", &mp, ds], true)?;
        self.run_zfs(&["mount", ds], true)?;
        Ok(())
    }

    /// Destroys a dataset this call just made and could not mount, so a retry starts clean.
    fn discard(&self, ds: &str) {
        let _ = self.run_zfs(&["destroy", ds], false);
    }

    fn keep(&self, path: &Path) {
        self.kept
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(path.to_path_buf());
    }
}

impl CloneBackend for ZfsBackend {
    fn name(&self) -> &'static str {
        "zfs"
    }

    fn check_supported(&self, path: &Path) -> Result<()> {
        if self.contains(path) {
            return Ok(());
        }
        self.dataset(path).map(|_| ())
    }

    fn clone_dir(&self, src: &Path, dst: &Path) -> Result<()> {
        let (src_ds, dst_ds) = (self.dataset(src)?, self.dataset(dst)?);
        if std::fs::symlink_metadata(dst).is_ok() {
            return Err(internal(format!(
                "clone {} -> {}: destination exists",
                src.display(),
                dst.display()
            )));
        }
        let snap = format!("{src_ds}@{SNAPSHOT}");
        if !self.exists(&snap)? {
            match self.run_zfs(&["snapshot", &snap], false) {
                Err(e) if e.message.contains("already exists") => {} // a parallel clone took it
                r => {
                    r?;
                }
            }
        }
        self.ensure_parents(&dst_ds)?;
        self.run_zfs(&["clone", "-o", "canmount=noauto", &snap, &dst_ds], false)?;
        self.mount_at(&dst_ds, dst).inspect_err(|_| self.discard(&dst_ds))
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        let ds = self.dataset(path)?;
        self.ensure_parents(&ds)?;
        self.run_zfs(&["create", "-o", "canmount=noauto", &ds], false)?;
        self.mount_at(&ds, path).inspect_err(|_| self.discard(&ds))?;
        // Postgres refuses a data folder others can read; pg_basebackup keeps an existing
        // folder's mode, so it is set here.
        let _ = std::fs::set_permissions(path, Permissions::from_mode(0o700));
        Ok(())
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        let ds = self.dataset(path)?;
        if !self.exists(&ds)? {
            // Not a dataset: a leftover mountpoint folder or a stray plain folder.
            return fs_remove_dir(path);
        }
        // The snapshot goes first: if clones still depend on it, ZFS refuses and nothing has
        // been unmounted yet. gc tries again on its next run; Mitosis never promotes.
        let snap = format!("{ds}@{SNAPSHOT}");
        if self.exists(&snap)? {
            match self.run_zfs(&["destroy", &snap], false) {
                Err(e) if e.message.contains("dependent clones") => {
                    self.keep(path);
                    return Ok(());
                }
                r => {
                    r?;
                }
            }
        }
        match self.run_zfs(&["unmount", &ds], true) {
            Err(e) if e.message.contains("not currently mounted") => {}
            r => {
                r?;
            }
        }
        match self.run_zfs(&["destroy", &ds], false) {
            Err(e) if e.message.contains("dependent clones") => {
                self.keep(path);
                return Ok(());
            }
            r => {
                r?;
            }
        }
        let _ = std::fs::remove_dir(path); // the empty mountpoint folder, if one is left
        Ok(())
    }

    fn rename_dir(&self, from: &Path, to: &Path) -> Result<()> {
        let (a, b) = (self.dataset(from)?, self.dataset(to)?);
        if !self.exists(&a)? {
            return fs_rename_dir(from, to);
        }
        self.ensure_parents(&b)?;
        self.run_zfs(&["rename", &a, &b], true)?;
        let mp = format!("mountpoint={}", to.display());
        self.run_zfs(&["set", &mp, &b], true)?;
        let _ = std::fs::remove_dir(from); // the old, now empty mountpoint folder
        Ok(())
    }

    fn private_size(&self, path: &Path) -> Option<u64> {
        let ds = self.dataset(path).ok()?;
        let out = self
            .run_zfs(&["get", "-Hp", "-o", "value", "written", &ds], false)
            .ok()?;
        out.trim().parse().ok()
    }

    fn take_kept(&self) -> Vec<PathBuf> {
        std::mem::take(&mut *self.kept.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

/// True when sudo itself said no (as opposed to zfs failing under sudo): it wants a password, the
/// rule does not allow the command, or another `sudo:` error with sudo's exit status 1.
fn sudo_refused(stderr: &str, code: Option<i32>) -> bool {
    stderr.contains("a password is required")
        || (code == Some(1)
            && (stderr.starts_with("sudo:") || stderr.contains("is not allowed to execute")))
}

/// Runs `cmd` with stdin closed; `None` when it did not finish within `timeout` (it is killed).
fn output_within(mut cmd: Command, timeout: Duration) -> std::io::Result<Option<Output>> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // ETXTBSY: another thread of this process was forking while the program file was being
    // written (seen with scripts written just before they run); it clears at once.
    let mut tries = 0;
    let mut child = loop {
        match cmd.spawn() {
            Err(e) if e.raw_os_error() == Some(libc::ETXTBSY) && tries < 5 => {
                tries += 1;
                std::thread::sleep(Duration::from_millis(10));
            }
            r => break r?,
        }
    };
    let mut out = child.stdout.take().expect("stdout is piped");
    let mut err = child.stderr.take().expect("stderr is piped");
    let read_out = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = out.read_to_end(&mut b);
        b
    });
    let read_err = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = err.read_to_end(&mut b);
        b
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break Some(s);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let stdout = read_out.join().unwrap_or_default();
    let stderr = read_err.join().unwrap_or_default();
    Ok(status.map(|status| Output {
        status,
        stdout,
        stderr,
    }))
}

/// A dataset name safe to put in commands and the sudoers text: ZFS name characters only.
pub fn check_dataset_name(ds: &str) -> Result<()> {
    let ok = !ds.is_empty()
        && !ds.starts_with('/')
        && !ds.ends_with('/')
        && !ds.contains("//")
        && ds
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':' | b'/'));
    if ok {
        return Ok(());
    }
    Err(MitosisError::new(
        ErrorCode::InvalidRequest,
        format!("invalid ZFS dataset name {ds:?}"),
    )
    .with_hint("use a name like tank/mitosis: letters, digits, '_', '-', '.', ':' and '/'"))
}

/// The real path of `zfs`: the first one on `PATH`, else in `/usr/sbin` or `/sbin`.
pub fn which_zfs() -> Option<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path)
        .chain([PathBuf::from("/usr/sbin"), PathBuf::from("/sbin")])
        .map(|d| d.join("zfs"))
        .find(|p| {
            p.metadata()
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        })
        .map(|p| std::fs::canonicalize(&p).unwrap_or(p))
}

/// The login name of this process's user.
pub fn current_user() -> String {
    if let Some(u) = std::env::var("USER").ok().filter(|u| !u.is_empty()) {
        return u;
    }
    Command::new("id")
        .arg("-un")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| "<user>".into())
}

/// What `mitosis zfs setup` prints: the `zfs allow` line and the sudoers rule an admin installs
/// once for `user` and `dataset`, with `zfs` at its real path.
pub fn setup_text(user: &str, dataset: &str, zfs: &Path) -> String {
    let z = zfs.display();
    let d = dataset;
    [
        "# Run these once as root. Mitosis never runs them itself.".to_string(),
        String::new(),
        format!("# 1. Delegate the dataset work to {user}:"),
        format!("zfs allow -u {user} {DELEGATED} {d}"),
        String::new(),
        format!("# 2. Let {user} mount, unmount and rename under {d} without a password."),
        "#    Save this line as /etc/sudoers.d/mitosis (visudo -f /etc/sudoers.d/mitosis):"
            .to_string(),
        format!(
            "{user} ALL=(root) NOPASSWD: {z} mount {d}/*, {z} unmount {d}/*, \
             {z} set mountpoint=* {d}/*, {z} set canmount=* {d}/*, {z} rename {d}/* {d}/*"
        ),
        String::new(),
    ]
    .join("\n")
}

/// The rights in `DELEGATED` that `zfs allow <dataset>` output does not give `user`.
fn missing_rights(allow_output: &str, user: &str) -> Vec<&'static str> {
    let mut have = HashSet::new();
    for line in allow_output.lines() {
        let mut words = line.split_whitespace();
        if words.next() == Some("user") && words.next() == Some(user) {
            if let Some(perms) = words.next() {
                have.extend(perms.split(','));
            }
        }
    }
    DELEGATED.split(',').filter(|p| !have.contains(p)).collect()
}

/// One result of `probes`, for `doctor` and `zfs setup`.
pub struct Probe {
    pub name: &'static str,
    pub ok: bool,
    pub detail: String,
    pub fix: Option<String>,
}

impl Probe {
    fn ok(name: &'static str, detail: impl Into<String>) -> Probe {
        Probe {
            name,
            ok: true,
            detail: detail.into(),
            fix: None,
        }
    }

    fn fail(name: &'static str, detail: impl Into<String>, fix: impl Into<String>) -> Probe {
        Probe {
            name,
            ok: false,
            detail: detail.into(),
            fix: Some(fix.into()),
        }
    }
}

/// The ZFS checks: `zfs_tool`, `zfs_dataset`, `zfs_permissions`, `zfs_sudo`. A failed check
/// stops the ones after it that depend on it. Nothing is changed: `sudo -n -l` only asks whether
/// the rule allows a command.
pub fn probes(dataset: &str) -> Vec<Probe> {
    let hint = setup_hint(dataset);
    let mut out = Vec::new();
    let Some(zfs) = which_zfs() else {
        out.push(Probe::fail(
            "zfs_tool",
            "zfs is not on PATH",
            "install the ZFS tools (zfsutils-linux)",
        ));
        return out;
    };
    out.push(Probe::ok("zfs_tool", format!("zfs at {}", zfs.display())));
    let b = ZfsBackend::with_programs(dataset, PathBuf::new(), zfs.clone(), "sudo".into());
    if let Err(e) = b.run_zfs(&["list", "-H", "-o", "name", dataset], false) {
        out.push(Probe::fail(
            "zfs_dataset",
            e.message,
            format!("as root: zfs create {dataset}"),
        ));
        return out;
    }
    out.push(Probe::ok("zfs_dataset", format!("{dataset} exists")));

    let user = current_user();
    out.push(match b.run_zfs(&["allow", dataset], false) {
        Ok(text) => match missing_rights(&text, &user) {
            m if m.is_empty() => Probe::ok(
                "zfs_permissions",
                format!("{user} has {DELEGATED} on {dataset}"),
            ),
            m => Probe::fail(
                "zfs_permissions",
                format!("{user} lacks {} on {dataset}", m.join(",")),
                hint.clone(),
            ),
        },
        Err(e) => Probe::fail("zfs_permissions", e.message, hint.clone()),
    });

    let (x, y) = (format!("{dataset}/mitosis-check"), format!("{dataset}/mitosis-check-2"));
    let (x, y) = (x.as_str(), y.as_str());
    let forms: [Vec<&str>; 5] = [
        vec!["mount", x],
        vec!["unmount", x],
        vec!["set", "mountpoint=/mitosis-check", x],
        vec!["set", "canmount=noauto", x],
        vec!["rename", x, y],
    ];
    let refused: Vec<String> = forms
        .iter()
        .filter(|args| {
            let mut cmd = Command::new("sudo");
            cmd.args(["-n", "-l"]).arg(&zfs).args(args.iter());
            !matches!(output_within(cmd, TIMEOUT), Ok(Some(o)) if o.status.success())
        })
        .map(|args| format!("zfs {}", args.join(" ")))
        .collect();
    out.push(if refused.is_empty() {
        Probe::ok("zfs_sudo", "sudo -n zfs works for mount, unmount, set and rename")
    } else {
        Probe::fail(
            "zfs_sudo",
            format!("sudo -n does not allow: {}", refused.join("; ")),
            hint,
        )
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Logs each run and keeps one file per existing dataset or snapshot in the state folder.
    /// `clone` marks its origin snapshot, whose `destroy` then reports dependent clones.
    const FAKE_ZFS: &str = r#"#!/bin/sh
echo "zfs $*" >> "@LOG@"
eval last=\${$#}
f="@STATE@/$(printf %s "$last" | tr '/@' '%+')"
case "$1" in
  list) if [ -e "$f" ]; then echo "$last"; exit 0; fi
        echo "cannot open '$last': dataset does not exist" >&2; exit 1 ;;
  snapshot|create) touch "$f" ;;
  clone) touch "$f"; touch "@STATE@/$(printf %s "$4" | tr '/@' '%+').deps" ;;
  destroy) if [ -e "$f.deps" ]; then
             echo "cannot destroy '$last': snapshot has dependent clones" >&2; exit 1
           fi
           rm -f "$f" ;;
esac
exit 0
"#;

    const FAKE_SUDO: &str = r#"#!/bin/sh
echo "sudo $*" >> "@LOG@"
[ "$1" = "-n" ] && shift
exec "$@"
"#;

    const PASSWORD_SUDO: &str = r#"#!/bin/sh
echo "sudo $*" >> "@LOG@"
echo "sudo: a password is required" >&2
exit 1
"#;

    struct Fake {
        _tmp: tempfile::TempDir,
        log: PathBuf,
        state: PathBuf,
        mount: PathBuf,
        backend: Arc<ZfsBackend>,
    }

    impl Fake {
        fn new(sudo_script: &str) -> Fake {
            let tmp = tempfile::tempdir().unwrap();
            let p = tmp.path();
            let (log, state, bin, mount) =
                (p.join("log"), p.join("state"), p.join("bin"), p.join("mnt"));
            for d in [&state, &bin, &mount] {
                std::fs::create_dir(d).unwrap();
            }
            let fill = |s: &str| {
                s.replace("@LOG@", log.to_str().unwrap())
                    .replace("@STATE@", state.to_str().unwrap())
            };
            let (zfs, sudo) = (bin.join("zfs"), bin.join("sudo"));
            for (path, text) in [(&zfs, fill(FAKE_ZFS)), (&sudo, fill(sudo_script))] {
                std::fs::write(path, text).unwrap();
                std::fs::set_permissions(path, Permissions::from_mode(0o755)).unwrap();
            }
            let backend = Arc::new(ZfsBackend::with_programs("tank/m", mount.clone(), zfs, sudo));
            Fake {
                _tmp: tmp,
                log,
                state,
                mount,
                backend,
            }
        }

        /// Makes dataset or snapshot `name` exist.
        fn has(&self, name: &str) {
            let key = name.replace('/', "%").replace('@', "+");
            std::fs::write(self.state.join(key), "").unwrap();
        }

        fn log(&self) -> Vec<String> {
            std::fs::read_to_string(&self.log)
                .unwrap_or_default()
                .lines()
                .map(String::from)
                .collect()
        }

        /// Index of the zfs run `line` in the log.
        fn pos(&self, line: &str) -> usize {
            let log = self.log();
            log.iter()
                .position(|l| l == line)
                .unwrap_or_else(|| panic!("{line:?} not run: {log:#?}"))
        }
    }

    #[test]
    fn clone_runs_snapshot_clone_mount_in_order() {
        let f = Fake::new(FAKE_SUDO);
        f.has("tank/m/templates");
        f.has("tank/m/templates/main:403");
        let (src, dst) = (f.mount.join("templates/main@3"), f.mount.join("clones/c0ffee1"));
        f.backend.clone_dir(&src, &dst).unwrap();

        let snap = f.pos("zfs snapshot tank/m/templates/main:403@mitosis");
        let parent = f.pos("zfs create -o canmount=off tank/m/clones");
        let clone = f.pos(
            "zfs clone -o canmount=noauto tank/m/templates/main:403@mitosis tank/m/clones/c0ffee1",
        );
        let mp = f.pos(&format!(
            "zfs set mountpoint={} tank/m/clones/c0ffee1",
            dst.display()
        ));
        let mount = f.pos("zfs mount tank/m/clones/c0ffee1");
        assert!(snap < parent && parent < clone && clone < mp && mp < mount);
        // Only the mountpoint and mount steps went through sudo.
        let sudo: Vec<_> = f.log().into_iter().filter(|l| l.starts_with("sudo ")).collect();
        assert_eq!(sudo.len(), 2, "{sudo:?}");
        assert!(sudo.iter().all(|l| l.starts_with("sudo -n ")), "{sudo:?}");
    }

    #[test]
    fn snapshot_reused_across_clones() {
        let f = Fake::new(FAKE_SUDO);
        f.has("tank/m/templates");
        f.has("tank/m/templates/main:403");
        f.has("tank/m/clones");
        let src = f.mount.join("templates/main@3");
        for id in ["c0ffee1", "c0ffee2"] {
            f.backend.clone_dir(&src, &f.mount.join("clones").join(id)).unwrap();
        }
        let snaps = f.log().iter().filter(|l| l.starts_with("zfs snapshot ")).count();
        assert_eq!(snaps, 1, "{:#?}", f.log());
        f.pos("zfs clone -o canmount=noauto tank/m/templates/main:403@mitosis tank/m/clones/c0ffee2");
    }

    #[test]
    fn remove_with_dependents_keeps_generation() {
        let f = Fake::new(FAKE_SUDO);
        f.has("tank/m/templates");
        f.has("tank/m/templates/main:403");
        f.has("tank/m/clones");
        let gen = f.mount.join("templates/main@3");
        std::fs::create_dir_all(&gen).unwrap();
        f.backend.clone_dir(&gen, &f.mount.join("clones/c0ffee1")).unwrap();

        f.backend.remove_dir(&gen).unwrap();
        f.pos("zfs destroy tank/m/templates/main:403@mitosis");
        let log = f.log();
        assert!(
            !log.iter().any(|l| l == "zfs destroy tank/m/templates/main:403"
                || l == "zfs unmount tank/m/templates/main:403"),
            "{log:#?}"
        );
        assert!(!log.iter().any(|l| l.contains(" -R ") || l.contains("promote")));
        assert!(gen.is_dir());
        assert_eq!(f.backend.take_kept(), vec![gen]);
        assert!(f.backend.take_kept().is_empty());
    }

    #[test]
    fn sudo_password_prompt_is_backend_unsupported() {
        let f = Fake::new(PASSWORD_SUDO);
        f.has("tank/m/templates");
        let dir = f.mount.join("templates/.main.building-1700000000");
        let t = Instant::now();
        let e = f.backend.create_dir(&dir).unwrap_err();
        assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
        assert_eq!(e.code, ErrorCode::BackendUnsupported, "{e}");
        assert_eq!(e.hint.as_deref(), Some(setup_hint("tank/m").as_str()));
        // The dataset it made but could not mount is not left behind.
        f.pos("zfs destroy tank/m/templates/.main.building-1700000000");
    }

    #[tokio::test]
    async fn orphan_sweep_destroys_datasets() {
        let f = Fake::new(FAKE_SUDO);
        let backend: Arc<dyn CloneBackend> = f.backend.clone();
        let home = crate::paths::Home::at(f.mount.clone());
        let ctx = crate::ctx::Ctx::open_with_backend(home, crate::ctx::Config::default(), backend)
            .unwrap();
        // A clone dataset the daemon made before it crashed, with no row in the store.
        let orphan = ctx.home.clones_dir().join("c0ffee1");
        std::fs::create_dir(&orphan).unwrap();
        f.has("tank/m/clones");
        f.has("tank/m/clones/c0ffee1");

        let report = crate::lifecycle::reconcile(Arc::new(ctx), crate::templates::now())
            .await
            .unwrap();
        assert!(report.orphan_folders_removed.contains(&orphan), "{report:?}");
        let unmount = f.pos("zfs unmount tank/m/clones/c0ffee1");
        let destroy = f.pos("zfs destroy tank/m/clones/c0ffee1");
        assert!(unmount < destroy);
        let log = f.log();
        assert!(!log.iter().any(|l| l.contains(" rename ")), "{log:#?}");
        assert!(!orphan.exists());
    }

    #[test]
    fn setup_prints_allow_and_sudoers_lines() {
        let text = setup_text("ci", "tank/mitosis", Path::new("/usr/sbin/zfs"));
        let lines: Vec<&str> = text.lines().collect();
        assert!(lines.contains(
            &"zfs allow -u ci create,destroy,mount,snapshot,clone,send,receive,canmount,mountpoint tank/mitosis"
        ), "{text}");
        assert!(lines.contains(
            &"ci ALL=(root) NOPASSWD: /usr/sbin/zfs mount tank/mitosis/*, /usr/sbin/zfs unmount tank/mitosis/*, /usr/sbin/zfs set mountpoint=* tank/mitosis/*, /usr/sbin/zfs set canmount=* tank/mitosis/*, /usr/sbin/zfs rename tank/mitosis/* tank/mitosis/*"
        ), "{text}");
        assert!(text.contains("/etc/sudoers.d/mitosis"));
        // Every non-comment line is one an admin runs or saves.
        let commands = lines.iter().filter(|l| !l.is_empty() && !l.starts_with('#')).count();
        assert_eq!(commands, 2, "{text}");
    }

    #[test]
    fn allow_output_and_dataset_names() {
        let out = "---- Permissions on tank/mitosis ----\nLocal+Descendent permissions:\n\
                   \tuser ci canmount,clone,create,destroy,mount,mountpoint,snapshot\n\
                   \tuser other send,receive\n";
        assert_eq!(missing_rights(out, "ci"), vec!["send", "receive"]);
        assert_eq!(missing_rights(out, "nobody").len(), 9);
        assert!(check_dataset_name("tank/mitosis").is_ok());
        for bad in ["", "/tank", "tank/", "tank//m", "tank m", "tank/m@s", "t,ank"] {
            assert!(check_dataset_name(bad).is_err(), "{bad:?}");
        }
    }
}
