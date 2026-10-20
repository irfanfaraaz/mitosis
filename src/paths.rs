use crate::error::{ErrorCode, MitosisError, Result};
use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

/// Empty `value` counts as unset; a relative one is made absolute so the daemon never depends on its cwd.
fn resolve(value: Option<OsString>, home_dir: Option<PathBuf>) -> Result<PathBuf> {
    match value.filter(|v| !v.is_empty()) {
        Some(p) => Ok(std::path::absolute(p)?),
        None => {
            let home = home_dir.ok_or_else(|| {
                MitosisError::new(ErrorCode::Internal, "HOME is not set")
                    .with_hint("set MITOSIS_HOME to choose where mitosis keeps its data")
            })?;
            Ok(home.join("Library/Application Support/mitosis"))
        }
    }
}

const MARKER: &str = ".mitosis-home";

/// Names Mitosis itself puts in the home root (Finder's `.DS_Store` is tolerated too).
fn is_own_root_entry(name: &str) -> bool {
    matches!(
        name,
        MARKER | "templates" | "clones" | "sockets" | "logs" | ".DS_Store"
    ) || name.starts_with("state.db")
        || name.starts_with("daemon.")
        || name.starts_with("config.toml")
}

#[derive(Clone, Debug)]
pub struct Home {
    pub root: PathBuf,
}

impl Home {
    /// `$MITOSIS_HOME`, else `~/Library/Application Support/mitosis`.
    pub fn from_env() -> Result<Home> {
        Ok(Home::at(resolve(
            std::env::var_os("MITOSIS_HOME"),
            std::env::var_os("HOME").map(PathBuf::from),
        )?))
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

    /// Postgres builds installed by `mitosis pg install`, one folder per `x.y` version.
    pub fn pg_dir(&self) -> PathBuf {
        self.root.join("pg")
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

    pub fn config_file(&self) -> PathBuf {
        self.root.join("config.toml")
    }

    /// Marks a folder as a Mitosis home; gc and chmod only ever touch a marked root.
    pub fn marker(&self) -> PathBuf {
        self.root.join(MARKER)
    }

    /// Creates the layout. An existing root without the marker is adopted only when it is empty or
    /// holds `state.db` plus nothing but Mitosis's own entries; any other folder is refused, so gc
    /// never sees it.
    pub fn ensure_dirs(&self) -> Result<()> {
        if !self.marker().exists() {
            std::fs::create_dir_all(&self.root)?;
            let names: Vec<String> = std::fs::read_dir(&self.root)?
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n != ".DS_Store")
                .collect();
            // Own-named entries alone (say a stray `clones/`) don't prove the folder is ours; a
            // hand-written `config.toml` alone (set up before the first run) does not make it foreign.
            let has_db = names.iter().any(|n| n == "state.db");
            let only_config = names.iter().all(|n| n == "config.toml");
            let mut foreign: Vec<String> = if !only_config && !has_db {
                names
            } else {
                names
                    .into_iter()
                    .filter(|n| !is_own_root_entry(n))
                    .collect()
            };
            if !foreign.is_empty() {
                foreign.sort();
                if foreign.len() > 5 {
                    foreign.truncate(5);
                    foreign.push("…".into());
                }
                return Err(MitosisError::new(
                    ErrorCode::ForeignHome,
                    format!(
                        "{} already holds files Mitosis did not create ({})",
                        self.root.display(),
                        foreign.join(", ")
                    ),
                )
                .with_hint(
                    "MITOSIS_HOME points at a folder Mitosis did not create; choose an empty or new folder",
                ));
            }
            std::fs::write(self.marker(), "")?;
        }
        // Clone sockets under sockets/ use trust auth: keep other local users out.
        std::fs::set_permissions(&self.root, std::fs::Permissions::from_mode(0o700))?;
        for d in [
            self.templates_dir(),
            self.clones_dir(),
            self.sockets_dir(),
            self.logs_dir(),
        ] {
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
        for d in [
            h.templates_dir(),
            h.clones_dir(),
            h.sockets_dir(),
            h.logs_dir(),
        ] {
            assert!(d.is_dir());
        }
        assert!(h.marker().is_file());
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
        assert_eq!(
            std::fs::metadata(&h.root).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn foreign_home_is_refused_and_left_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let h = Home::at(tmp.path().join("m"));
        std::fs::create_dir(&h.root).unwrap();
        std::fs::set_permissions(&h.root, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::create_dir(h.root.join("clones")).unwrap();
        std::fs::write(h.root.join("notes.txt"), "mine").unwrap();
        let e = h.ensure_dirs().unwrap_err();
        assert_eq!(e.code, ErrorCode::ForeignHome);
        assert!(e.message.contains("notes.txt"), "{e}");
        assert!(!h.marker().exists());
        assert_eq!(
            std::fs::metadata(&h.root).unwrap().permissions().mode() & 0o777,
            0o755,
            "an unmarked root is never chmodded"
        );

        // Only Mitosis's own entries (a home from before the marker existed): adopted.
        std::fs::remove_file(h.root.join("notes.txt")).unwrap();
        std::fs::write(h.root.join("state.db"), "").unwrap();
        std::fs::write(h.root.join("state.db-wal"), "").unwrap();
        std::fs::write(h.root.join("daemon.lock"), "").unwrap();
        h.ensure_dirs().unwrap();
        assert!(h.marker().is_file());

        // Once marked, other files in the root no longer matter.
        std::fs::write(h.root.join("notes.txt"), "mine").unwrap();
        h.ensure_dirs().unwrap();
    }

    #[test]
    fn unmarked_root_without_state_db_is_foreign() {
        let tmp = tempfile::tempdir().unwrap();
        let h = Home::at(tmp.path().join("m"));
        std::fs::create_dir(&h.root).unwrap();
        std::fs::create_dir(h.root.join("clones")).unwrap();
        let e = h.ensure_dirs().unwrap_err();
        assert_eq!(e.code, ErrorCode::ForeignHome);
        assert!(!h.marker().exists());
    }

    #[test]
    fn root_with_only_config_toml_is_adopted() {
        let tmp = tempfile::tempdir().unwrap();
        let h = Home::at(tmp.path().join("m"));
        std::fs::create_dir(&h.root).unwrap();
        std::fs::write(h.config_file(), "max_clones = 4\n").unwrap();
        h.ensure_dirs().unwrap();
        assert!(h.marker().is_file());
        assert_eq!(
            std::fs::read_to_string(h.config_file()).unwrap(),
            "max_clones = 4\n"
        );
    }

    #[test]
    fn resolve_empty_is_default_and_relative_is_absolute() {
        let home = Some(PathBuf::from("/Users/x"));
        let default = PathBuf::from("/Users/x/Library/Application Support/mitosis");
        assert_eq!(resolve(Some("".into()), home.clone()).unwrap(), default);
        assert_eq!(resolve(None, home.clone()).unwrap(), default);
        assert_eq!(
            resolve(Some("/abs/m".into()), home.clone()).unwrap(),
            PathBuf::from("/abs/m")
        );
        let rel = resolve(Some("rel/m".into()), home).unwrap();
        assert!(rel.is_absolute());
        assert!(rel.ends_with("rel/m"));
        assert!(resolve(None, None).is_err());
    }
}
