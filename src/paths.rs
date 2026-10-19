use crate::error::{ErrorCode, MitosisError, Result};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Home {
    pub root: PathBuf,
}

impl Home {
    /// `$MITOSIS_HOME`, else `~/Library/Application Support/mitosis`.
    pub fn from_env() -> Result<Home> {
        if let Some(p) = std::env::var_os("MITOSIS_HOME") {
            return Ok(Home::at(PathBuf::from(p)));
        }
        let home = std::env::var_os("HOME").ok_or_else(|| {
            MitosisError::new(ErrorCode::Internal, "HOME is not set")
                .with_hint("set MITOSIS_HOME to choose where mitosis keeps its data")
        })?;
        Ok(Home::at(PathBuf::from(home).join("Library/Application Support/mitosis")))
    }

    pub fn at(root: PathBuf) -> Home {
        Home { root }
    }

    pub fn templates_dir(&self) -> PathBuf {
        self.root.join("templates")
    }

    pub fn clones_dir(&self) -> PathBuf {
        self.root.join("clones")
    }

    pub fn sockets_dir(&self) -> PathBuf {
        self.root.join("sockets")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    pub fn state_db(&self) -> PathBuf {
        self.root.join("state.db")
    }

    pub fn daemon_sock(&self) -> PathBuf {
        self.root.join("daemon.sock")
    }

    pub fn daemon_lock(&self) -> PathBuf {
        self.root.join("daemon.lock")
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        std::fs::create_dir_all(&self.root)?;
        // Clone sockets under sockets/ use trust auth: keep other local users out.
        std::fs::set_permissions(&self.root, std::fs::Permissions::from_mode(0o700))?;
        for d in [self.templates_dir(), self.clones_dir(), self.sockets_dir(), self.logs_dir()] {
            std::fs::create_dir_all(d)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_and_ensure_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let h = Home::at(tmp.path().join("m"));
        h.ensure_dirs().unwrap();
        for d in [h.templates_dir(), h.clones_dir(), h.sockets_dir(), h.logs_dir()] {
            assert!(d.is_dir());
        }
        assert_eq!(h.state_db(), h.root.join("state.db"));
        assert_eq!(h.daemon_sock(), h.root.join("daemon.sock"));
        assert_eq!(h.daemon_lock(), h.root.join("daemon.lock"));
    }

    #[test]
    fn home_root_is_private() {
        let tmp = tempfile::tempdir().unwrap();
        let h = Home::at(tmp.path().join("m"));
        std::fs::create_dir(&h.root).unwrap();
        std::fs::set_permissions(&h.root, std::fs::Permissions::from_mode(0o755)).unwrap();
        h.ensure_dirs().unwrap();
        assert_eq!(std::fs::metadata(&h.root).unwrap().permissions().mode() & 0o777, 0o700);
    }
}
