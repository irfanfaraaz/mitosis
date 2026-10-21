use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Clone, Debug, PartialEq)]
pub struct PgInstall {
    pub bin_dir: PathBuf,
    pub major: u32,
    pub version: String,
}

impl PgInstall {
    pub fn bin(&self, name: &str) -> PathBuf {
        self.bin_dir.join(name)
    }

    /// Not required by `discover`; callers check it exists before an upgrade.
    pub fn pg_upgrade(&self) -> PathBuf {
        self.bin("pg_upgrade")
    }
}

/// Parses `postgres --version` output, e.g. "postgres (PostgreSQL) 16.4" -> (16, "16.4").
pub fn parse_version(output: &str) -> Option<(u32, String)> {
    let rest = output.split("(PostgreSQL)").nth(1)?.trim_start();
    let version: String = rest
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let version = version.trim_end_matches('.').to_string();
    let major = version.split('.').next()?.parse().ok()?;
    Some((major, version))
}

pub(crate) fn version_key(version: &str) -> Vec<u32> {
    version.split('.').filter_map(|p| p.parse().ok()).collect()
}

/// Expands `<parent>/<prefix>*/<suffix>`; missing parents yield nothing.
fn glob_dirs(parent: impl AsRef<Path>, prefix: &str, suffix: &str) -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(parent) else {
        return vec![];
    };
    rd.flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
        .map(|e| e.path().join(suffix))
        .collect()
}

/// The fixed system folders, under `root` (`/` in real use; a temp folder in tests).
fn candidates_in(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for prefix in ["opt/homebrew/opt", "usr/local/opt"] {
        let dir = root.join(prefix);
        out.extend(glob_dirs(&dir, "postgresql@", "bin"));
        out.push(dir.join("postgresql/bin"));
    }
    if cfg!(target_os = "macos") {
        out.extend(glob_dirs(
            root.join("Applications/Postgres.app/Contents/Versions"),
            "",
            "bin",
        ));
    }
    if cfg!(target_os = "linux") {
        // apt/PGDG: /usr/lib/postgresql/<N>/bin; dnf/PGDG: /usr/pgsql-<N>/bin.
        out.extend(glob_dirs(root.join("usr/lib/postgresql"), "", "bin"));
        out.extend(glob_dirs(root.join("usr"), "pgsql-", "bin"));
    }
    out
}

fn candidates() -> Vec<PathBuf> {
    let mut out = candidates_in(Path::new("/"));
    if let Ok(home) = std::env::var("HOME") {
        out.extend(glob_dirs(
            format!("{home}/.asdf/installs/postgres"),
            "",
            "bin",
        ));
        out.extend(glob_dirs(format!("{home}/.pgenv"), "pgsql-", "bin"));
    }
    if cfg!(target_os = "linux") {
        if let Ok(o) = Command::new("pg_config").arg("--bindir").output() {
            let dir = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if o.status.success() && !dir.is_empty() {
                out.push(PathBuf::from(dir));
            }
        }
    }
    if let Some(path) = std::env::var_os("PATH") {
        out.extend(std::env::split_paths(&path).filter(|d| d.join("postgres").is_file()));
    }
    out
}

fn probe(bin_dir: &Path) -> Option<PgInstall> {
    let required = ["postgres", "initdb", "pg_ctl", "psql", "pg_isready"];
    if !required.iter().all(|b| bin_dir.join(b).is_file()) {
        return None;
    }
    let out = Command::new(bin_dir.join("postgres"))
        .arg("--version")
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let (major, version) = parse_version(&String::from_utf8_lossy(&out.stdout))?;
    Some(PgInstall {
        bin_dir: bin_dir.to_path_buf(),
        major,
        version,
    })
}

/// System installs only; the daemon uses `discover_with` so managed builds count too.
pub fn discover() -> Vec<PgInstall> {
    discover_with(None)
}

/// System installs plus, given a home, the builds `mitosis pg install` put in `<home>/pg/`.
pub fn discover_with(home: Option<&Home>) -> Vec<PgInstall> {
    let mut seen = HashSet::new();
    let mut probe_all = |dirs: Vec<PathBuf>| -> Vec<PgInstall> {
        dirs.into_iter()
            .filter_map(|dir| {
                // By the real `postgres` file: Homebrew links one install under several folders.
                let canon = std::fs::canonicalize(dir.join("postgres")).ok()?;
                if !seen.insert(canon) {
                    return None;
                }
                probe(&dir)
            })
            .collect()
    };
    let system = probe_all(candidates());
    let managed = home
        .map(|h| probe_all(managed_candidates(h)))
        .unwrap_or_default();
    merge(system, managed)
}

fn managed_candidates(home: &Home) -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(home.pg_dir()) else {
        return vec![];
    };
    rd.flatten()
        .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
        .map(|e| e.path().join("bin"))
        .collect()
}

/// A system install of a major wins over a managed one of the same major; newest first.
fn merge(system: Vec<PgInstall>, managed: Vec<PgInstall>) -> Vec<PgInstall> {
    let majors: HashSet<u32> = system.iter().map(|i| i.major).collect();
    let mut installs = system;
    installs.extend(managed.into_iter().filter(|m| !majors.contains(&m.major)));
    installs.sort_by(|a, b| {
        b.major
            .cmp(&a.major)
            .then_with(|| version_key(&b.version).cmp(&version_key(&a.version)))
    });
    installs
}

pub fn find_major(installs: &[PgInstall], major: u32) -> Result<&PgInstall> {
    installs.iter().find(|i| i.major == major).ok_or_else(|| {
        MitosisError::new(
            ErrorCode::NoMatchingPostgres,
            format!("no Postgres {major} found"),
        )
        .with_hint(install_hint(major))
    })
}

/// Newest Postgres major; used only for the install hint when none is found.
pub const LATEST_PG_MAJOR: u32 = 18;

/// The package-manager command that installs Postgres `major` on this OS.
pub fn install_hint(major: u32) -> String {
    if cfg!(target_os = "macos") {
        format!("brew install postgresql@{major}")
    } else if Path::new("/usr/bin/dnf").exists() {
        format!("sudo dnf install postgresql{major}-server")
    } else {
        format!("sudo apt install postgresql-{major}")
    }
}

pub fn newest(installs: &[PgInstall]) -> Result<&PgInstall> {
    installs.first().ok_or_else(|| {
        MitosisError::new(ErrorCode::NoMatchingPostgres, "no Postgres found")
            .with_hint(install_hint(LATEST_PG_MAJOR))
    })
}

pub fn read_pg_version(data_dir: &Path) -> Result<u32> {
    let path = data_dir.join("PG_VERSION");
    let text = std::fs::read_to_string(&path).map_err(|e| {
        MitosisError::new(ErrorCode::Internal, format!("read {}: {e}", path.display()))
    })?;
    text.trim().parse().map_err(|_| {
        MitosisError::new(
            ErrorCode::Internal,
            format!("invalid PG_VERSION in {}", path.display()),
        )
    })
}

pub fn check_extensions(install: &PgInstall, extensions: &[String]) -> Result<()> {
    let pg_config = install.bin("pg_config");
    let out = Command::new(&pg_config).arg("--sharedir").output()?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stderr = stderr.trim();
        let detail = if stderr.is_empty() {
            format!("{}, no output", out.status)
        } else {
            stderr.to_string()
        };
        return Err(MitosisError::new(
            ErrorCode::Internal,
            format!("{} --sharedir failed: {detail}", pg_config.display()),
        ));
    }
    let share = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim());
    let missing: Vec<&str> = extensions
        .iter()
        .map(String::as_str)
        .filter(|n| {
            *n != "plpgsql"
                && !share
                    .join("extension")
                    .join(format!("{n}.control"))
                    .is_file()
        })
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(MitosisError::new(
        ErrorCode::MissingExtension,
        format!(
            "missing extensions for Postgres {}: {}",
            install.major,
            missing.join(", ")
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_candidates_include_apt_and_dnf_layouts() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("usr/lib/postgresql/16/bin")).unwrap();
        std::fs::create_dir_all(root.path().join("usr/pgsql-15/bin")).unwrap();
        let c = candidates_in(root.path());
        assert!(c.contains(&root.path().join("usr/lib/postgresql/16/bin")));
        assert!(c.contains(&root.path().join("usr/pgsql-15/bin")));
    }

    #[test]
    fn parses_versions() {
        assert_eq!(
            parse_version("postgres (PostgreSQL) 16.4"),
            Some((16, "16.4".into()))
        );
        assert_eq!(
            parse_version("postgres (PostgreSQL) 17.0 (Homebrew)"),
            Some((17, "17.0".into()))
        );
        assert_eq!(
            parse_version("postgres (PostgreSQL) 9.6.24"),
            Some((9, "9.6.24".into()))
        );
        assert_eq!(parse_version("garbage"), None);
    }

    #[test]
    fn version_sort_is_numeric() {
        assert!(version_key("16.10") > version_key("16.9"));
    }

    #[test]
    fn missing_postgres_errors() {
        let e = find_major(&[], 16).unwrap_err();
        assert_eq!(e.code, ErrorCode::NoMatchingPostgres);
        assert_eq!(e.message, "no Postgres 16 found");
        assert_eq!(e.hint, Some(install_hint(16)));
        let e = newest(&[]).unwrap_err();
        assert_eq!(e.code, ErrorCode::NoMatchingPostgres);
        assert_eq!(e.hint, Some(install_hint(18)));
        let want = if cfg!(target_os = "macos") {
            "brew install postgresql@16"
        } else if Path::new("/usr/bin/dnf").exists() {
            "sudo dnf install postgresql16-server"
        } else {
            "sudo apt install postgresql-16"
        };
        assert_eq!(install_hint(16), want);
    }

    #[test]
    fn find_major_and_newest_pick_first_match() {
        let mk = |major, v: &str| PgInstall {
            bin_dir: "/x".into(),
            major,
            version: v.into(),
        };
        let installs = vec![mk(17, "17.0"), mk(16, "16.4")];
        assert_eq!(newest(&installs).unwrap().major, 17);
        assert_eq!(find_major(&installs, 16).unwrap().version, "16.4");
    }

    #[test]
    fn system_install_wins_same_major() {
        let mk = |dir: &str, major, v: &str| PgInstall {
            bin_dir: dir.into(),
            major,
            version: v.into(),
        };
        let system = vec![mk("/opt/homebrew/opt/postgresql@17/bin", 17, "17.2")];
        let managed = vec![
            mk("/h/pg/17.9/bin", 17, "17.9"),
            mk("/h/pg/16.4/bin", 16, "16.4"),
            mk("/h/pg/18.1/bin", 18, "18.1"),
        ];
        let all = merge(system, managed);
        let got: Vec<_> = all.iter().map(|i| (i.major, i.version.as_str())).collect();
        assert_eq!(got, vec![(18, "18.1"), (17, "17.2"), (16, "16.4")]);
        assert_eq!(
            find_major(&all, 17).unwrap().bin_dir,
            PathBuf::from("/opt/homebrew/opt/postgresql@17/bin")
        );
    }

    #[test]
    fn reads_pg_version_file() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("PG_VERSION"), "16\n").unwrap();
        assert_eq!(read_pg_version(d.path()).unwrap(), 16);
        assert!(read_pg_version(&d.path().join("nope")).is_err());
    }
}
