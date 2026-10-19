use crate::error::{ErrorCode, MitosisError, Result};
use std::ffi::{CStr, CString};
use std::fs::Metadata;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

// Not exported by libc; value from <sys/clonefile.h>.
const CLONE_NOFOLLOW: u32 = 0x0001;

pub trait CloneBackend: Send + Sync {
    fn check_supported(&self, path: &Path) -> Result<()>;
    fn clone_dir(&self, src: &Path, dst: &Path) -> Result<()>;
}

pub struct ApfsBackend;

fn cpath(p: &Path) -> Result<CString> {
    CString::new(p.as_os_str().as_bytes()).map_err(|_| {
        MitosisError::new(
            ErrorCode::Internal,
            format!("path contains a NUL byte: {}", p.display()),
        )
    })
}

impl CloneBackend for ApfsBackend {
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
        let name = unsafe { CStr::from_ptr(st.f_fstypename.as_ptr()) }.to_string_lossy();
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
                .with_hint("source and destination must be on the same APFS volume"),
            _ => MitosisError::new(ErrorCode::Internal, msg),
        })
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

pub fn apparent_size(path: &Path) -> Result<u64> {
    let mut total = 0;
    walk_files(path, &mut |_, md| {
        total += md.len();
        Some(())
    })?;
    Ok(total)
}

/// Bytes of one file not shared with any clone, via ATTR_CMNEXT_PRIVATESIZE.
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
pub fn private_size(path: &Path) -> Option<u64> {
    let mut total = 0u64;
    let done = walk_files(path, &mut |p, _| {
        total += file_private_size(p)?;
        Some(())
    });
    matches!(done, Ok(Some(()))).then_some(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir, read_to_string, write};

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
        if let Some(p) = private_size(&dst) {
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
