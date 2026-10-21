use crate::config::Config;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use std::ffi::CString;
use std::fs::{Metadata, OpenOptions, Permissions};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod copy;
pub use copy::{CopyBackend, SLOW_WARNING};
#[cfg(target_os = "linux")]
mod reflink;
#[cfg(target_os = "linux")]
pub use reflink::ReflinkBackend;
#[cfg(target_os = "linux")]
pub mod zfs;
#[cfg(target_os = "linux")]
pub use zfs::ZfsBackend;

// Not exported by libc; value from <sys/clonefile.h>.
#[cfg(target_os = "macos")]
const CLONE_NOFOLLOW: u32 = 0x0001;

pub trait CloneBackend: Send + Sync {
    /// `"apfs"`, `"reflink"`, `"zfs"` or `"copy"`.
    fn name(&self) -> &'static str;
    fn check_supported(&self, path: &Path) -> Result<()>;
    /// Makes `dst` a clone of `src`; `dst` must not exist.
    fn clone_dir(&self, src: &Path, dst: &Path) -> Result<()>;
    /// Makes `dst` (which must not exist) a template generation copied from `src`, a generation
    /// or a clone. Unlike `clone_dir`, the result must not depend on `src` staying around.
    fn copy_dir(&self, src: &Path, dst: &Path) -> Result<()> {
        self.clone_dir(src, dst)
    }
    /// Makes an empty directory a build writes into and later clones are made from.
    fn create_dir(&self, path: &Path) -> Result<()>;
    /// Removes a clone or generation directory and everything in it; a missing path is `Ok(())`.
    fn remove_dir(&self, path: &Path) -> Result<()>;
    /// Renames a clone or generation directory within the same parent tree.
    fn rename_dir(&self, from: &Path, to: &Path) -> Result<()>;
    /// Bytes under `path` not shared with any clone, or `None` when it cannot be told.
    fn private_size(&self, path: &Path) -> Option<u64>;
    /// Folders `remove_dir` left in place (and returned `Ok`) because clones still depend on
    /// them, since the last call; only ZFS keeps any. gc reports them and tries again later.
    fn take_kept(&self) -> Vec<PathBuf> {
        Vec::new()
    }
    /// ZFS only: where `dataset`, a dataset Mitosis does not own (a running source), is mounted.
    fn external_mountpoint(&self, _dataset: &str) -> Result<PathBuf> {
        Err(no_running_source(self.name()))
    }
    /// ZFS only: takes the snapshot `<dataset>@<name>` and returns the folder it is read through.
    fn snapshot_external(&self, _dataset: &str, _name: &str) -> Result<PathBuf> {
        Err(no_running_source(self.name()))
    }
    /// ZFS only: destroys the snapshot `snapshot_external` took.
    fn destroy_external(&self, _dataset: &str, _name: &str) -> Result<()> {
        Err(no_running_source(self.name()))
    }
}

fn no_running_source(backend: &str) -> MitosisError {
    unsupported(
        format!("--from-running needs the zfs back end; this home uses {backend}"),
        "set backend = zfs and zfs_dataset, with MITOSIS_HOME on that dataset",
    )
}

/// Fills `dst`, an empty folder that exists, with a full copy of the tree at `src` (owner, modes
/// and times kept, symlinks recreated, never followed).
pub(crate) fn fill_copy(src: &Path, dst: &Path) -> Result<()> {
    fill_tree(src, dst, &copy::copy_file)
}

/// `create_dir`, `remove_dir` and `rename_dir` as plain file system calls (APFS, reflink, copy).
/// Mode 0700: Postgres refuses a data folder others can read, and `pg_basebackup` keeps the
/// mode of a folder that already exists (`initdb` sets 0700 itself).
fn fs_create_dir(path: &Path) -> Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
        .map_err(|e| {
            MitosisError::new(ErrorCode::Internal, format!("create {}: {e}", path.display()))
        })
}

fn fs_remove_dir(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(MitosisError::new(
            ErrorCode::Internal,
            format!("remove {}: {e}", path.display()),
        )),
        _ => Ok(()),
    }
}

/// The error message is the bare OS error, so callers add which move failed.
fn fs_rename_dir(from: &Path, to: &Path) -> Result<()> {
    Ok(std::fs::rename(from, to)?)
}

#[cfg(target_os = "macos")]
pub struct ApfsBackend;

/// The `backend` setting: which way clones are made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Auto,
    Apfs,
    Reflink,
    Zfs,
    Copy,
}

impl BackendKind {
    pub fn as_str(self) -> &'static str {
        match self {
            BackendKind::Auto => "auto",
            BackendKind::Apfs => "apfs",
            BackendKind::Reflink => "reflink",
            BackendKind::Zfs => "zfs",
            BackendKind::Copy => "copy",
        }
    }
}

impl std::str::FromStr for BackendKind {
    type Err = MitosisError;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "auto" => BackendKind::Auto,
            "apfs" => BackendKind::Apfs,
            "reflink" => BackendKind::Reflink,
            "zfs" => BackendKind::Zfs,
            "copy" => BackendKind::Copy,
            _ => {
                return Err(MitosisError::new(
                    ErrorCode::InvalidRequest,
                    format!("unknown back end {s:?}"),
                )
                .with_hint("backends are: auto, apfs, reflink, zfs, copy"))
            }
        })
    }
}

fn unsupported(msg: impl Into<String>, hint: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::BackendUnsupported, msg).with_hint(hint)
}

/// The file system type of `path`, for error messages (`btrfs`, `ext4`, or the hex magic).
#[cfg(not(target_os = "macos"))]
fn fs_type_name(path: &Path) -> String {
    let Ok(c) = cpath(path) else {
        return "unknown".into();
    };
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c.as_ptr(), &mut st) } != 0 {
        return "unknown".into();
    }
    let magic = st.f_type as u64 & 0xFFFF_FFFF;
    match magic {
        0x9123683E => "btrfs".into(),
        0x5846_5342 => "xfs".into(),
        0xEF53 => "ext4".into(),
        0x2FC1_2FC1 => "zfs".into(),
        0x0102_1994 => "tmpfs".into(),
        0x794C_7630 => "overlayfs".into(),
        m => format!("filesystem 0x{m:x}"),
    }
}

/// Picks the clone back end for `home` from the `backend` setting. `Copy` is never picked by
/// `Auto`; ZFS is Linux only.
pub fn select_backend(home: &Home, cfg: &Config) -> Result<Arc<dyn CloneBackend>> {
    match cfg.backend {
        #[cfg(target_os = "macos")]
        BackendKind::Auto | BackendKind::Apfs => Ok(Arc::new(ApfsBackend)),
        #[cfg(not(target_os = "macos"))]
        BackendKind::Apfs => Err(unsupported(
            "the apfs back end only works on macOS",
            "set backend = \"auto\", or put MITOSIS_HOME on Btrfs, XFS (mkfs.xfs -m reflink=1) or ZFS",
        )),
        #[cfg(not(target_os = "macos"))]
        BackendKind::Auto => {
            // Order: a configured ZFS dataset whose mountpoint holds the home, then a FICLONE
            // probe (OpenZFS block cloning answers it too).
            #[cfg(target_os = "linux")]
            if let Some(ds) = &cfg.zfs_dataset {
                let zfs = ZfsBackend::new(ds)?;
                if zfs.contains(&home.root) {
                    return Ok(Arc::new(zfs));
                }
            }
            let fs = fs_type_name(&home.root);
            let no_backend = || {
                unsupported(
                    format!(
                        "no clone back end works on the '{fs}' file system at {}",
                        home.root.display()
                    ),
                    "put MITOSIS_HOME on Btrfs, XFS (mkfs.xfs -m reflink=1) or ZFS, or set backend = \"copy\"",
                )
            };
            #[cfg(target_os = "linux")]
            match ReflinkBackend.check_supported(&home.root) {
                Ok(()) => return Ok(Arc::new(ReflinkBackend)),
                Err(e) if e.code != ErrorCode::BackendUnsupported => return Err(e),
                Err(_) => {}
            }
            if fs == "zfs" {
                return Err(unsupported(
                    format!(
                        "{} is on ZFS without block cloning, and no zfs_dataset holds it",
                        home.root.display()
                    ),
                    "set zfs_dataset to the dataset whose mountpoint holds MITOSIS_HOME, for example: mitosis config set zfs_dataset tank/mitosis",
                ));
            }
            Err(no_backend())
        }
        #[cfg(target_os = "linux")]
        BackendKind::Reflink => Ok(Arc::new(ReflinkBackend)),
        #[cfg(not(target_os = "linux"))]
        BackendKind::Reflink => Err(unsupported(
            "the reflink back end only works on Linux",
            "set backend = \"auto\"",
        )),
        BackendKind::Copy => Ok(Arc::new(CopyBackend)),
        #[cfg(target_os = "linux")]
        BackendKind::Zfs => {
            let ds = cfg.zfs_dataset.as_deref().ok_or_else(|| {
                unsupported(
                    "backend = \"zfs\" needs zfs_dataset",
                    "set zfs_dataset to the dataset Mitosis owns, for example: mitosis config set zfs_dataset tank/mitosis",
                )
            })?;
            Ok(Arc::new(ZfsBackend::new(ds)?))
        }
        #[cfg(not(target_os = "linux"))]
        BackendKind::Zfs => {
            let _ = home;
            Err(unsupported("the zfs back end only works on Linux", "set backend = \"auto\""))
        }
    }
}

fn cpath(p: &Path) -> Result<CString> {
    CString::new(p.as_os_str().as_bytes()).map_err(|_| {
        MitosisError::new(
            ErrorCode::Internal,
            format!("path contains a NUL byte: {}", p.display()),
        )
    })
}

#[cfg(target_os = "macos")]
impl CloneBackend for ApfsBackend {
    fn name(&self) -> &'static str {
        "apfs"
    }

    fn check_supported(&self, path: &Path) -> Result<()> {
        let c = cpath(path)?;
        let mut st: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(c.as_ptr(), &mut st) } != 0 {
            let e = std::io::Error::last_os_error();
            return Err(MitosisError::new(
                ErrorCode::Internal,
                format!("statfs {}: {e}", path.display()),
            ));
        }
        let name = unsafe { std::ffi::CStr::from_ptr(st.f_fstypename.as_ptr()) }.to_string_lossy();
        if name != "apfs" {
            return Err(MitosisError::new(
                ErrorCode::NotApfs,
                format!("{} is on a '{name}' file system, not APFS", path.display()),
            )
            .with_hint("set MITOSIS_HOME to a folder on an APFS volume"));
        }
        Ok(())
    }

    fn clone_dir(&self, src: &Path, dst: &Path) -> Result<()> {
        let (s, d) = (cpath(src)?, cpath(dst)?);
        if unsafe { libc::clonefile(s.as_ptr(), d.as_ptr(), CLONE_NOFOLLOW) } == 0 {
            return Ok(());
        }
        let e = std::io::Error::last_os_error();
        let msg = format!("clonefile {} -> {}: {e}", src.display(), dst.display());
        Err(match e.raw_os_error() {
            Some(libc::EXDEV) => MitosisError::new(ErrorCode::CrossVolume, msg)
                .with_hint("source and destination must be on the same volume"),
            _ => MitosisError::new(ErrorCode::Internal, msg),
        })
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        fs_create_dir(path)
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        fs_remove_dir(path)
    }

    fn rename_dir(&self, from: &Path, to: &Path) -> Result<()> {
        fs_rename_dir(from, to)
    }

    fn private_size(&self, path: &Path) -> Option<u64> {
        apfs_private_size(path)
    }
}

fn stat(path: &Path) -> Result<Metadata> {
    std::fs::metadata(path).map_err(|e| {
        MitosisError::new(ErrorCode::Internal, format!("stat {}: {e}", path.display()))
    })
}

pub fn same_volume(a: &Path, b: &Path) -> Result<bool> {
    Ok(stat(a)?.dev() == stat(b)?.dev())
}

pub fn free_space(path: &Path) -> Result<u64> {
    let c = cpath(path)?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        let e = std::io::Error::last_os_error();
        return Err(MitosisError::new(
            ErrorCode::Internal,
            format!("statvfs {}: {e}", path.display()),
        ));
    }
    Ok(st.f_bavail as u64 * st.f_frsize as u64)
}

/// Calls `f` for every regular file under `path` (or `path` itself), never following symlinks.
fn walk_files(
    path: &Path,
    f: &mut dyn FnMut(&Path, &Metadata) -> Option<()>,
) -> Result<Option<()>> {
    let md = std::fs::symlink_metadata(path).map_err(|e| {
        MitosisError::new(
            ErrorCode::Internal,
            format!("lstat {}: {e}", path.display()),
        )
    })?;
    if md.is_dir() {
        let rd = std::fs::read_dir(path).map_err(|e| {
            MitosisError::new(
                ErrorCode::Internal,
                format!("read_dir {}: {e}", path.display()),
            )
        })?;
        for entry in rd {
            let entry = entry?;
            if walk_files(&entry.path(), f)?.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(()))
    } else if md.is_file() {
        Ok(f(path, &md))
    } else {
        Ok(Some(())) // symlinks and special files are skipped
    }
}

fn io_error(what: &str, path: &Path, e: std::io::Error) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, format!("{what} {}: {e}", path.display()))
}

/// Makes `dst` (which must not exist) a copy of the tree at `src`; `file` copies or clones one
/// regular file. A half-made tree is removed so a retry starts clean.
fn clone_tree(
    src: &Path,
    dst: &Path,
    file: &dyn Fn(&Path, &Path, &Metadata) -> Result<()>,
) -> Result<()> {
    if std::fs::symlink_metadata(dst).is_ok() {
        return Err(MitosisError::new(
            ErrorCode::Internal,
            format!("clone {} -> {}: destination exists", src.display(), dst.display()),
        ));
    }
    clone_entry(src, dst, file).inspect_err(|_| {
        let _ = std::fs::remove_dir_all(dst).or_else(|_| std::fs::remove_file(dst));
    })
}

/// `clone_tree` into `dst`, an empty folder that already exists (a fresh ZFS dataset): the
/// entries of `src` are recreated in it, and it takes `src`'s owner, mode and times.
fn fill_tree(
    src: &Path,
    dst: &Path,
    file: &dyn Fn(&Path, &Path, &Metadata) -> Result<()>,
) -> Result<()> {
    let md = std::fs::symlink_metadata(src).map_err(|e| io_error("lstat", src, e))?;
    for entry in std::fs::read_dir(src).map_err(|e| io_error("read_dir", src, e))? {
        let entry = entry.map_err(|e| io_error("read_dir", src, e))?;
        clone_entry(&entry.path(), &dst.join(entry.file_name()), file)?;
    }
    keep_owner(dst, &md)?;
    std::fs::set_permissions(dst, Permissions::from_mode(md.mode() & 0o7777))
        .map_err(|e| io_error("chmod", dst, e))?;
    keep_times(dst, &md)
}

/// Creates `dst` for a regular file with the source's permission bits (O_CREAT | O_EXCL).
fn create_dst(dst: &Path, md: &Metadata) -> Result<std::fs::File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(md.mode() & 0o777)
        .open(dst)
        .map_err(|e| io_error("create", dst, e))
}

/// Recreates `src` at `dst`: directories made, symlinks recreated (never followed), regular
/// files handed to `file`; owner, mode and times kept. Sockets, fifos and devices are skipped.
fn clone_entry(
    src: &Path,
    dst: &Path,
    file: &dyn Fn(&Path, &Path, &Metadata) -> Result<()>,
) -> Result<()> {
    let md = std::fs::symlink_metadata(src).map_err(|e| io_error("lstat", src, e))?;
    let ft = md.file_type();
    if ft.is_symlink() {
        let target = std::fs::read_link(src).map_err(|e| io_error("readlink", src, e))?;
        std::os::unix::fs::symlink(&target, dst).map_err(|e| io_error("symlink", dst, e))?;
    } else if ft.is_dir() {
        // Owner-writable while it is filled; the source mode is set after the children.
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(dst)
            .map_err(|e| io_error("mkdir", dst, e))?;
        for entry in std::fs::read_dir(src).map_err(|e| io_error("read_dir", src, e))? {
            let entry = entry.map_err(|e| io_error("read_dir", src, e))?;
            clone_entry(&entry.path(), &dst.join(entry.file_name()), file)?;
        }
    } else if ft.is_file() {
        file(src, dst, &md)?;
    } else {
        return Ok(());
    }
    keep_owner(dst, &md)?;
    if !ft.is_symlink() {
        // After the chown, which clears setuid/setgid bits.
        std::fs::set_permissions(dst, Permissions::from_mode(md.mode() & 0o7777))
            .map_err(|e| io_error("chmod", dst, e))?;
    }
    keep_times(dst, &md)
}

/// Gives `path` the source's owner when it differs from ours and we are allowed to (root).
fn keep_owner(path: &Path, md: &Metadata) -> Result<()> {
    let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
    if md.uid() == uid && md.gid() == gid {
        return Ok(());
    }
    let c = cpath(path)?;
    if unsafe { libc::lchown(c.as_ptr(), md.uid(), md.gid()) } == 0 {
        return Ok(());
    }
    let e = std::io::Error::last_os_error();
    match e.raw_os_error() {
        Some(libc::EPERM) => Ok(()), // not allowed: the clone stays ours
        _ => Err(io_error("chown", path, e)),
    }
}

/// Copies access and modify times onto `path`, without following a symlink.
fn keep_times(path: &Path, md: &Metadata) -> Result<()> {
    let c = cpath(path)?;
    let mut times: [libc::timespec; 2] = unsafe { std::mem::zeroed() };
    times[0].tv_sec = md.atime() as libc::time_t;
    times[0].tv_nsec = md.atime_nsec() as _;
    times[1].tv_sec = md.mtime() as libc::time_t;
    times[1].tv_nsec = md.mtime_nsec() as _;
    let rc = unsafe {
        libc::utimensat(
            libc::AT_FDCWD,
            c.as_ptr(),
            times.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc != 0 {
        return Err(io_error("utimensat", path, std::io::Error::last_os_error()));
    }
    Ok(())
}

pub fn apparent_size(path: &Path) -> Result<u64> {
    let mut total = 0;
    walk_files(path, &mut |_, md| {
        total += md.len();
        Some(())
    })?;
    Ok(total)
}

/// Bytes of one file not shared with any clone, via ATTR_CMNEXT_PRIVATESIZE.
#[cfg(target_os = "macos")]
fn file_private_size(path: &Path) -> Option<u64> {
    #[repr(C, packed)]
    struct Reply {
        length: u32,
        private_size: i64, // off_t
    }
    let c = cpath(path).ok()?;
    let mut al: libc::attrlist = unsafe { std::mem::zeroed() };
    al.bitmapcount = libc::ATTR_BIT_MAP_COUNT;
    al.forkattr = libc::ATTR_CMNEXT_PRIVATESIZE; // extended common attrs travel in forkattr
    let mut reply = Reply {
        length: 0,
        private_size: 0,
    };
    let rc = unsafe {
        libc::getattrlist(
            c.as_ptr(),
            &mut al as *mut _ as *mut libc::c_void,
            &mut reply as *mut _ as *mut libc::c_void,
            std::mem::size_of::<Reply>(),
            libc::FSOPT_NOFOLLOW | libc::FSOPT_ATTR_CMN_EXTENDED,
        )
    };
    let size = reply.private_size;
    (rc == 0 && size >= 0).then_some(size as u64)
}

// shortcut: any failure on any file makes the whole sum None rather than a partial number;
// upgrade if callers need a best-effort partial total.
#[cfg(target_os = "macos")]
fn apfs_private_size(path: &Path) -> Option<u64> {
    let mut total = 0u64;
    let done = walk_files(path, &mut |p, _| {
        total += file_private_size(p)?;
        Some(())
    });
    matches!(done, Ok(Some(()))).then_some(total)
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use std::fs::{create_dir, read_to_string, write};

    #[test]
    fn auto_picks_apfs_on_apfs() {
        let tmp = tempfile::tempdir().unwrap();
        let home = Home::at(tmp.path().to_path_buf());
        let b = select_backend(&home, &Config::default()).unwrap();
        assert_eq!(b.name(), "apfs");
        b.check_supported(tmp.path()).unwrap();
    }

    #[test]
    fn unbuilt_backends_are_unsupported() {
        let tmp = tempfile::tempdir().unwrap();
        let home = Home::at(tmp.path().to_path_buf());
        for kind in [BackendKind::Reflink, BackendKind::Zfs] {
            let cfg = Config {
                backend: kind,
                ..Config::default()
            };
            let e = select_backend(&home, &cfg).err().unwrap();
            assert_eq!(e.code, ErrorCode::BackendUnsupported);
        }
    }

    #[test]
    fn clone_is_isolated_and_shares_blocks() {
        let tmp = tempfile::tempdir().unwrap();
        ApfsBackend.check_supported(tmp.path()).unwrap();
        let (src, dst) = (tmp.path().join("src"), tmp.path().join("dst"));
        create_dir(&src).unwrap();
        write(src.join("a.txt"), "hello").unwrap();
        create_dir(src.join("sub")).unwrap();
        write(src.join("sub/b.txt"), "x".repeat(1_000_000)).unwrap();

        ApfsBackend.clone_dir(&src, &dst).unwrap();
        assert_eq!(read_to_string(dst.join("a.txt")).unwrap(), "hello");
        write(dst.join("a.txt"), "changed").unwrap();
        assert_eq!(read_to_string(src.join("a.txt")).unwrap(), "hello");

        assert!(same_volume(&src, &dst).unwrap());
        assert!(free_space(tmp.path()).unwrap() > 0);
        assert!(ApfsBackend.clone_dir(&src, &dst).is_err());
        assert_eq!(apparent_size(&src).unwrap(), 1_000_005);
        if let Some(p) = ApfsBackend.private_size(&dst) {
            assert!(p < 1_000_000);
        }
    }

    #[test]
    fn symlinks_are_not_followed() {
        let tmp = tempfile::tempdir().unwrap();
        let big = tmp.path().join("big");
        write(&big, vec![0u8; 4096]).unwrap();
        let dir = tmp.path().join("d");
        create_dir(&dir).unwrap();
        std::os::unix::fs::symlink(&big, dir.join("link")).unwrap();
        assert_eq!(apparent_size(&dir).unwrap(), 0);
    }
}

#[cfg(all(test, not(target_os = "macos")))]
mod auto_tests {
    use super::*;

    #[test]
    fn auto_never_picks_copy() {
        let tmp = tempfile::tempdir_in(std::env::var("MITOSIS_TEST_ROOT").unwrap_or("/tmp".into()))
            .unwrap();
        let home = Home::at(tmp.path().to_path_buf());
        // Skipped on a reflink file system, where auto picks reflink.
        if let Ok(b) = select_backend(&home, &Config::default()) {
            assert_ne!(b.name(), "copy");
            return;
        }
        let e = select_backend(&home, &Config::default()).err().unwrap();
        assert_eq!(e.code, ErrorCode::BackendUnsupported);
    }
}
