use crate::error::{ErrorCode, MitosisError, Result};
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

fn version_key(version: &str) -> Vec<u32> {
    version.split('.').filter_map(|p| p.parse().ok()).collect()
}

/// Expands `<parent>/<prefix>*/<suffix>`; missing parents yield nothing.
fn glob_dirs(parent: &str, prefix: &str, suffix: &str) -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(parent) else {
        return vec![];
    };
    rd.flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
        .map(|e| e.path().join(suffix))
        .collect()
}

fn candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for root in ["/opt/homebrew/opt", "/usr/local/opt"] {
        out.extend(glob_dirs(root, "postgresql@", "bin"));
        out.push(Path::new(root).join("postgresql/bin"));
    }
    out.extend(glob_dirs(
        "/Applications/Postgres.app/Contents/Versions",
        "",
        "bin",
    ));
    if let Ok(home) = std::env::var("HOME") {
        out.extend(glob_dirs(
            &format!("{home}/.asdf/installs/postgres"),
            "",
            "bin",
        ));
        out.extend(glob_dirs(&format!("{home}/.pgenv"), "pgsql-", "bin"));
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

pub fn discover() -> Vec<PgInstall> {
    let mut seen = HashSet::new();
    let mut installs: Vec<PgInstall> = candidates()
        .into_iter()
        .filter_map(|dir| {
            let canon = std::fs::canonicalize(&dir).ok()?;
            if !seen.insert(canon) {
                return None;
            }
            probe(&dir)
        })
        .collect();
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
        .with_hint(format!("brew install postgresql@{major}"))
    })
}

pub fn newest(installs: &[PgInstall]) -> Result<&PgInstall> {
    installs.first().ok_or_else(|| {
        MitosisError::new(ErrorCode::NoMatchingPostgres, "no Postgres found")
            .with_hint("brew install postgresql")
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
    let out = Command::new(install.bin("pg_config"))
        .arg("--sharedir")
        .output()?;
    if !out.status.success() {
        return Err(MitosisError::new(
            ErrorCode::Internal,
            String::from_utf8_lossy(&out.stderr).trim().to_string(),
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
        assert_eq!(e.hint.as_deref(), Some("brew install postgresql@16"));
        let e = newest(&[]).unwrap_err();
        assert_eq!(e.code, ErrorCode::NoMatchingPostgres);
        assert_eq!(e.hint.as_deref(), Some("brew install postgresql"));
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
    fn reads_pg_version_file() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("PG_VERSION"), "16\n").unwrap();
        assert_eq!(read_pg_version(d.path()).unwrap(), 16);
        assert!(read_pg_version(&d.path().join("nope")).is_err());
    }
}
