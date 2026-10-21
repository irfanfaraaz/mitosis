//! Copy back end for disks without reflink: the same tree walk as the reflink back end, but each
//! regular file is copied in full. Chosen only by `backend = "copy"`, never by `auto`.

use super::{
    apparent_size, clone_tree, create_dst, fs_create_dir, fs_remove_dir, fs_rename_dir, io_error,
    CloneBackend,
};
use crate::error::Result;
use std::fs::{File, Metadata};
use std::path::Path;

/// Printed on every clone create.
pub const SLOW_WARNING: &str =
    "backend \"copy\" makes full copies; clones are slow and use full disk space";

pub struct CopyBackend;

impl CloneBackend for CopyBackend {
    fn name(&self) -> &'static str {
        "copy"
    }

    fn check_supported(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    fn clone_dir(&self, src: &Path, dst: &Path) -> Result<()> {
        clone_tree(src, dst, &copy_file)
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

    /// Nothing is shared, so every byte is private.
    fn private_size(&self, path: &Path) -> Option<u64> {
        apparent_size(path).ok()
    }
}

/// Also the file copy of the ZFS back end's `copy_dir`.
pub(super) fn copy_file(src: &Path, dst: &Path, md: &Metadata) -> Result<()> {
    let mut s = File::open(src).map_err(|e| io_error("open", src, e))?;
    let mut d = create_dst(dst, md)?;
    std::io::copy(&mut s, &mut d).map_err(|e| io_error("copy", src, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir, read_link, read_to_string, symlink_metadata, write};

    #[test]
    fn copy_backend_clone_is_isolated() {
        let tmp = tempfile::tempdir().unwrap();
        let (src, dst) = (tmp.path().join("src"), tmp.path().join("dst"));
        create_dir(&src).unwrap();
        write(src.join("a.txt"), "hello").unwrap();
        create_dir(src.join("sub")).unwrap();
        write(src.join("sub/b.txt"), "x".repeat(1_000_000)).unwrap();
        std::os::unix::fs::symlink("/elsewhere", src.join("link")).unwrap();

        CopyBackend.check_supported(tmp.path()).unwrap();
        CopyBackend.clone_dir(&src, &dst).unwrap();
        write(dst.join("a.txt"), "changed").unwrap();
        assert_eq!(read_to_string(src.join("a.txt")).unwrap(), "hello");
        assert!(symlink_metadata(dst.join("link"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            read_link(dst.join("link")).unwrap(),
            Path::new("/elsewhere")
        );
        assert_eq!(CopyBackend.private_size(&dst), Some(1_000_005 + 2));
        assert!(CopyBackend.clone_dir(&src, &dst).is_err());
    }
}
