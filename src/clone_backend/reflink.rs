//! Reflink back end for Btrfs and XFS: the tree is walked and each regular file is cloned with
//! the `FICLONE` ioctl, so a clone shares every block with its source until one side writes.

use super::{
    cpath, fs_create_dir, fs_remove_dir, fs_rename_dir, fs_type_name, unsupported, walk_files,
    CloneBackend,
};
use crate::error::{ErrorCode, MitosisError, Result};
use std::fs::{File, Metadata, OpenOptions, Permissions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const REFLINK_HINT: &str =
    "put MITOSIS_HOME on Btrfs or XFS (mkfs.xfs -m reflink=1), or set backend = \"copy\"";

// Not exported by libc; values from <linux/fs.h> and <linux/fiemap.h>.
const FS_IOC_FIEMAP: u32 = 0xC020_660B;
const FIEMAP_EXTENT_LAST: u32 = 0x0001;
const FIEMAP_EXTENT_SHARED: u32 = 0x2000;
const FIEMAP_BATCH: usize = 64;

#[repr(C)]
#[derive(Clone, Copy)]
#[allow(dead_code)] // the kernel fills fields we never read
struct FiemapExtent {
    fe_logical: u64,
    fe_physical: u64,
    fe_length: u64,
    fe_reserved64: [u64; 2],
    fe_flags: u32,
    fe_reserved: [u32; 3],
}

/// `struct fiemap` with room for `FIEMAP_BATCH` extents after the 32-byte header.
#[repr(C)]
#[allow(dead_code)] // the kernel reads fields we only write
struct Fiemap {
    fm_start: u64,
    fm_length: u64,
    fm_flags: u32,
    fm_mapped_extents: u32,
    fm_extent_count: u32,
    fm_reserved: u32,
    fm_extents: [FiemapExtent; FIEMAP_BATCH],
}

pub struct ReflinkBackend;

impl CloneBackend for ReflinkBackend {
    fn name(&self) -> &'static str {
        "reflink"
    }

    fn check_supported(&self, path: &Path) -> Result<()> {
        // Probes from parallel threads of one process must not share file names.
        static PROBE: AtomicU64 = AtomicU64::new(0);
        let tag = format!(
            ".mitosis-reflink-probe-{}-{}",
            std::process::id(),
            PROBE.fetch_add(1, Ordering::Relaxed)
        );
        let (a, b) = (path.join(format!("{tag}-a")), path.join(format!("{tag}-b")));
        let probed = probe(&a, &b);
        let _ = std::fs::remove_file(&a);
        let _ = std::fs::remove_file(&b);
        probed.map_err(|e| {
            unsupported(
                format!(
                    "reflink (FICLONE) does not work on the '{}' file system at {}: {e}",
                    fs_type_name(path),
                    path.display()
                ),
                REFLINK_HINT,
            )
        })
    }

    fn clone_dir(&self, src: &Path, dst: &Path) -> Result<()> {
        if std::fs::symlink_metadata(dst).is_ok() {
            return Err(MitosisError::new(
                ErrorCode::Internal,
                format!("clone {} -> {}: destination exists", src.display(), dst.display()),
            ));
        }
        // A half-made clone is removed so a retry starts clean; `dst` did not exist before.
        clone_entry(src, dst).inspect_err(|_| {
            let _ = std::fs::remove_dir_all(dst).or_else(|_| std::fs::remove_file(dst));
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

    // shortcut: any failure on any file makes the whole sum None rather than a partial number,
    // as on APFS.
    fn private_size(&self, path: &Path) -> Option<u64> {
        let mut total = 0u64;
        let done = walk_files(path, &mut |p, _| {
            total += file_private_size(p)?;
            Some(())
        });
        matches!(done, Ok(Some(()))).then_some(total)
    }
}

fn io_error(what: &str, path: &Path, e: std::io::Error) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, format!("{what} {}: {e}", path.display()))
}

fn ficlone(src: &File, dst: &File) -> std::io::Result<()> {
    if unsafe { libc::ioctl(dst.as_raw_fd(), libc::FICLONE, src.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Clones a small temp file `a` into `b`: the answer holds for this exact mount.
fn probe(a: &Path, b: &Path) -> std::io::Result<()> {
    let mut fa = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(a)?;
    fa.write_all(&[0u8; 4096])?;
    let fb = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(b)?;
    ficlone(&fa, &fb)
}

/// Recreates `src` at `dst`: directories made, symlinks recreated (never followed), regular
/// files cloned; owner, mode and times kept. Sockets, fifos and devices are skipped.
fn clone_entry(src: &Path, dst: &Path) -> Result<()> {
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
            clone_entry(&entry.path(), &dst.join(entry.file_name()))?;
        }
    } else if ft.is_file() {
        clone_file(src, dst, &md)?;
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

fn clone_file(src: &Path, dst: &Path, md: &Metadata) -> Result<()> {
    let s = File::open(src).map_err(|e| io_error("open", src, e))?;
    let d = OpenOptions::new()
        .write(true)
        .create_new(true) // O_CREAT | O_EXCL
        .mode(md.mode() & 0o777)
        .open(dst)
        .map_err(|e| io_error("create", dst, e))?;
    ficlone(&s, &d).map_err(|e| {
        let msg = format!("FICLONE {} -> {}: {e}", src.display(), dst.display());
        match e.raw_os_error() {
            Some(libc::EXDEV) => MitosisError::new(ErrorCode::CrossVolume, msg)
                .with_hint("templates/ and clones/ must be on the same filesystem"),
            Some(libc::EOPNOTSUPP | libc::EINVAL) => unsupported(msg, REFLINK_HINT),
            _ => MitosisError::new(ErrorCode::Internal, msg),
        }
    })
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

/// Bytes of one file in extents not marked shared, via `FS_IOC_FIEMAP`; any ioctl error is `None`.
fn file_private_size(path: &Path) -> Option<u64> {
    let f = File::open(path).ok()?;
    let mut fm: Fiemap = unsafe { std::mem::zeroed() };
    let (mut start, mut total) = (0u64, 0u64);
    loop {
        fm.fm_start = start;
        fm.fm_length = u64::MAX;
        fm.fm_flags = 0;
        fm.fm_mapped_extents = 0;
        fm.fm_extent_count = FIEMAP_BATCH as u32;
        if unsafe { libc::ioctl(f.as_raw_fd(), FS_IOC_FIEMAP as _, &mut fm as *mut Fiemap) } != 0 {
            return None;
        }
        let n = (fm.fm_mapped_extents as usize).min(FIEMAP_BATCH);
        if n == 0 {
            return Some(total);
        }
        let extents = &fm.fm_extents[..n];
        for e in extents {
            if e.fe_flags & FIEMAP_EXTENT_SHARED == 0 {
                total += e.fe_length;
            }
        }
        let last = extents[n - 1];
        let next = last.fe_logical.saturating_add(last.fe_length);
        if last.fe_flags & FIEMAP_EXTENT_LAST != 0 || next <= start {
            return Some(total);
        }
        start = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clone_backend::same_volume;
    use std::fs::{create_dir, metadata, read, read_link, read_to_string, symlink_metadata, write};
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;

    /// The mounted Btrfs/XFS test file system, only when `scripts/linux-test.sh` runs reflink.
    fn reflink_root() -> Option<PathBuf> {
        if std::env::var("MITOSIS_BACKEND").as_deref() != Ok("reflink") {
            eprintln!("skipped: MITOSIS_BACKEND is not reflink");
            return None;
        }
        Some(std::env::var("MITOSIS_TEST_ROOT").unwrap_or_else(|_| "/tmp".into()).into())
    }

    #[test]
    fn reflink_clone_is_isolated_and_shares_blocks() {
        let Some(root) = reflink_root() else { return };
        let tmp = tempfile::tempdir_in(root).unwrap();
        ReflinkBackend.check_supported(tmp.path()).unwrap();
        let (src, dst) = (tmp.path().join("src"), tmp.path().join("dst"));
        create_dir(&src).unwrap();
        let big = src.join("big");
        write(&big, vec![7u8; 8 << 20]).unwrap();
        std::fs::set_permissions(&big, Permissions::from_mode(0o640)).unwrap();
        create_dir(src.join("sub")).unwrap();
        write(src.join("sub/a.txt"), "hello").unwrap();

        ReflinkBackend.clone_dir(&src, &dst).unwrap();
        let private = ReflinkBackend.private_size(&dst).unwrap();
        assert!(private < 64 * 1024, "clone has {private} private bytes");
        let (smd, dmd) = (metadata(&big).unwrap(), metadata(dst.join("big")).unwrap());
        assert_eq!(dmd.mode() & 0o7777, 0o640);
        assert_eq!(dmd.modified().unwrap(), smd.modified().unwrap());

        write(dst.join("sub/a.txt"), "changed").unwrap();
        let mut f = OpenOptions::new().write(true).open(dst.join("big")).unwrap();
        f.write_all(b"xxxx").unwrap();
        drop(f);
        assert_eq!(read_to_string(src.join("sub/a.txt")).unwrap(), "hello");
        assert_eq!(read(&big).unwrap()[..4], [7u8; 4]);
        assert!(ReflinkBackend.clone_dir(&src, &dst).is_err());
    }

    #[test]
    fn reflink_keeps_symlinks_unfollowed() {
        let Some(root) = reflink_root() else { return };
        let tmp = tempfile::tempdir_in(root).unwrap();
        let (src, dst) = (tmp.path().join("src"), tmp.path().join("dst"));
        let wal_real = tmp.path().join("wal_real");
        create_dir(&wal_real).unwrap();
        write(wal_real.join("000000010000000000000001"), vec![1u8; 4096]).unwrap();
        create_dir(&src).unwrap();
        create_dir(src.join("pg_tblspc")).unwrap();
        symlink("/elsewhere", src.join("pg_tblspc/16384")).unwrap();
        symlink(&wal_real, src.join("pg_wal")).unwrap();

        ReflinkBackend.clone_dir(&src, &dst).unwrap();
        for link in ["pg_tblspc/16384", "pg_wal"] {
            assert!(symlink_metadata(dst.join(link)).unwrap().file_type().is_symlink());
            assert_eq!(read_link(dst.join(link)).unwrap(), read_link(src.join(link)).unwrap());
        }
        assert_eq!(std::fs::read_dir(&wal_real).unwrap().count(), 1);
    }

    #[test]
    fn reflink_cross_filesystem_is_cross_volume() {
        let Some(root) = reflink_root() else { return };
        let tmp = tempfile::tempdir_in(root).unwrap();
        let other = tempfile::tempdir_in("/tmp").unwrap();
        if same_volume(tmp.path(), other.path()).unwrap() {
            eprintln!("skipped: /tmp is on the test file system");
            return;
        }
        let src = tmp.path().join("src");
        create_dir(&src).unwrap();
        write(src.join("a"), "hello").unwrap();
        let dst = other.path().join("dst");

        let e = ReflinkBackend.clone_dir(&src, &dst).err().unwrap();
        assert_eq!(e.code, ErrorCode::CrossVolume, "{e}");
        assert!(!dst.exists());
    }

    #[test]
    fn probe_rejects_non_reflink_fs() {
        if std::env::var("MITOSIS_TEST_ROOT").is_err() {
            eprintln!("skipped: not in the Linux test container");
            return;
        }
        let tmp = tempfile::tempdir_in("/tmp").unwrap();
        let e = ReflinkBackend.check_supported(tmp.path()).err().unwrap();
        assert_eq!(e.code, ErrorCode::BackendUnsupported, "{e}");
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
    }
}
