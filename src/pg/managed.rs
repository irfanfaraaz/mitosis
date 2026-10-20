//! Managed Postgres builds: `mitosis pg install | list | remove`. The only code in Mitosis that
//! downloads, and only when the user runs `pg install`. Runs in the CLI process, not the daemon.
//!
//! Source: the `theseus-rs/postgresql-binaries` GitHub releases. Checked by reading the release
//! list on 2026-10-20 (nothing downloaded):
//! - Tags are `<pg major>.<pg minor>.<build>`, e.g. `17.9.1`. Majors 14 to 18 are built for macOS;
//!   a minor can have several builds (`17.9.0`, `17.9.1`), and the newest is taken.
//! - Per release and target there is `postgresql-<tag>-<target>.tar.gz` plus `.sha256`, for both
//!   `aarch64-apple-darwin` and `x86_64-apple-darwin`. Also `.manifest.json`, `.provenance.json`,
//!   `.sbom-attestation.json` and `.spdx.json`, which are not used here.
//! - The `.sha256` file is one line, `<64 hex>  <asset file name>` (sha256sum format, ~112 bytes).
//! - URL shape: `<base>/download/<tag>/<asset>`; GitHub redirects it to a CDN, which ureq follows.
//! - Not verifiable without a download: the archive's top-level folder (handled both ways below:
//!   `bin/` at the root, or one folder holding `bin/`) and whether contrib extensions are in.
//!
//! `file://<folder>` bases (tests) read the same layout from disk: `<folder>/download/<tag>/<asset>`.
//! An `http(s)` base other than the GitHub one lists its tags from `<base>/tags.txt` (one per line).

use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use crate::pg::binaries::{discover_with, parse_version, version_key};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

pub const DEFAULT_RELEASES_URL: &str = "https://github.com/theseus-rs/postgresql-binaries/releases";

/// How many of the newest builds of a major to try before giving up on this target.
const MAX_TRIES: usize = 5;

#[derive(Clone, Debug)]
pub struct ReleaseSource {
    pub base_url: String,
}

impl ReleaseSource {
    /// `MITOSIS_PG_RELEASES_URL` (an `http://` or `file://` base), else the GitHub releases.
    pub fn from_env() -> ReleaseSource {
        let base_url = std::env::var("MITOSIS_PG_RELEASES_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_RELEASES_URL.to_string());
        ReleaseSource { base_url }
    }
}

impl Default for ReleaseSource {
    fn default() -> Self {
        ReleaseSource {
            base_url: DEFAULT_RELEASES_URL.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ManagedInstall {
    /// `x.y`, also the folder name under `<home>/pg/`.
    pub version: String,
    pub major: u32,
    pub dir: PathBuf,
    /// Discovery picks this build (no system install of the same major, no newer managed one).
    pub used: bool,
}

pub fn target_triple() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "aarch64-apple-darwin"
    } else {
        "x86_64-apple-darwin"
    }
}

fn invalid(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::InvalidRequest, msg)
}

fn net_error(what: &str, e: impl std::fmt::Display) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, format!("{what}: {e}")).with_hint(
        "check the network, or point MITOSIS_PG_RELEASES_URL at a mirror (http:// or file://)",
    )
}

// ---- fetching -------------------------------------------------------------------------------

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_connect(Some(Duration::from_secs(30)))
        .timeout_global(Some(Duration::from_secs(30 * 60)))
        .build()
        .into()
}

/// Opens `url` for reading; `None` when it does not exist (404 or a missing file).
fn open(url: &str) -> Result<Option<Box<dyn Read>>> {
    if let Some(path) = url.strip_prefix("file://") {
        return match std::fs::File::open(path) {
            Ok(f) => Ok(Some(Box::new(f))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(MitosisError::new(
                ErrorCode::Internal,
                format!("read {path}: {e}"),
            )),
        };
    }
    match agent().get(url).call() {
        Ok(resp) => Ok(Some(Box::new(resp.into_body().into_reader()))),
        Err(ureq::Error::StatusCode(404)) => Ok(None),
        Err(ureq::Error::StatusCode(403 | 429)) if url.starts_with("https://api.github.com/") => {
            Err(MitosisError::new(
                ErrorCode::Internal,
                "GitHub refused the release lookup (rate limit)",
            )
            .with_hint(
                "wait a while, or pass the exact tag: mitosis pg install <major> --version <x.y.z>",
            ))
        }
        Err(e) => Err(net_error(&format!("GET {url}"), e)),
    }
}

/// Small text files only (checksums, tag lists); capped at 1 MiB.
fn fetch_text(url: &str) -> Result<Option<String>> {
    let Some(r) = open(url)? else { return Ok(None) };
    let mut s = String::new();
    r.take(1 << 20)
        .read_to_string(&mut s)
        .map_err(|e| net_error(&format!("read {url}"), e))?;
    Ok(Some(s))
}

/// Streams `url` into `dest` and returns the SHA-256 (lowercase hex) of the bytes written.
fn download(url: &str, dest: &Path) -> Result<String> {
    let mut r = open(url)?.ok_or_else(|| {
        MitosisError::new(
            ErrorCode::NoMatchingPostgres,
            format!("{url} does not exist"),
        )
    })?;
    let mut f = std::fs::File::create(dest)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];
    let mut total = 0u64;
    let mut next_report = 16u64 << 20;
    loop {
        let n = r
            .read(&mut buf)
            .map_err(|e| net_error(&format!("download {url}"), e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        f.write_all(&buf[..n])?;
        total += n as u64;
        if total >= next_report {
            println!("  {} MiB", total >> 20);
            next_report += 16 << 20;
        }
    }
    f.flush()?;
    Ok(hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

// ---- release lookup -------------------------------------------------------------------------

fn tag_key(tag: &str) -> Option<Vec<u32>> {
    let parts: Vec<&str> = tag.split('.').collect();
    if parts.len() != 3
        || !parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    Some(version_key(tag))
}

/// All tags `<major>.<minor>.<build>` of `major`, newest first.
fn list_tags(src: &ReleaseSource, major: u32) -> Result<Vec<String>> {
    let base = src.base_url.trim_end_matches('/');
    let raw: Vec<String> = if let Some(dir) = base.strip_prefix("file://") {
        std::fs::read_dir(Path::new(dir).join("download"))
            .map(|rd| {
                rd.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default()
    } else if let Some(repo) = base
        .strip_prefix("https://github.com/")
        .and_then(|r| r.strip_suffix("/releases"))
    {
        let url = format!(
            "https://api.github.com/repos/{repo}/git/matching-refs/tags/{major}.?per_page=100"
        );
        let body = fetch_text(&url)?.unwrap_or_default();
        let refs: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap_or_default();
        refs.iter()
            .filter_map(|r| {
                r["ref"]
                    .as_str()?
                    .strip_prefix("refs/tags/")
                    .map(String::from)
            })
            .collect()
    } else {
        fetch_text(&format!("{base}/tags.txt"))?
            .unwrap_or_default()
            .lines()
            .map(|l| l.trim().to_string())
            .collect()
    };
    let mut tags: Vec<String> = raw
        .into_iter()
        .filter(|t| tag_key(t).is_some_and(|k| k[0] == major))
        .collect();
    tags.sort_by_key(|t| std::cmp::Reverse(tag_key(t)));
    Ok(tags)
}

/// `x.y` of a tag `x.y.z`.
fn minor_of(tag: &str) -> String {
    tag.rsplit_once('.')
        .map(|(xy, _)| xy)
        .unwrap_or(tag)
        .to_string()
}

fn asset_name(tag: &str) -> String {
    format!("postgresql-{tag}-{}.tar.gz", target_triple())
}

fn asset_url(src: &ReleaseSource, tag: &str, asset: &str) -> String {
    format!(
        "{}/download/{tag}/{asset}",
        src.base_url.trim_end_matches('/')
    )
}

/// The tag to install and its checksum file text. Candidates are tried newest first; one with no
/// build for this target (no `.sha256` asset) is skipped.
fn resolve(src: &ReleaseSource, major: u32, version: Option<&str>) -> Result<(String, String)> {
    let candidates: Vec<String> = match version {
        None => list_tags(src, major)?,
        Some(v) => {
            let parts: Vec<&str> = v.split('.').collect();
            let numeric = parts
                .iter()
                .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
            if !numeric || !(2..=3).contains(&parts.len()) {
                return Err(invalid(format!("invalid version {v:?}")).with_hint(
                    "use x.y (newest build of that minor) or the exact release tag x.y.z",
                ));
            }
            if parts[0].parse::<u32>().ok() != Some(major) {
                return Err(invalid(format!(
                    "version {v} is not a Postgres {major} version"
                )));
            }
            if parts.len() == 3 {
                vec![v.to_string()]
            } else {
                list_tags(src, major)?
                    .into_iter()
                    .filter(|t| minor_of(t) == v)
                    .collect()
            }
        }
    };
    for tag in candidates.iter().take(MAX_TRIES) {
        let url = asset_url(src, tag, &format!("{}.sha256", asset_name(tag)));
        if let Some(text) = fetch_text(&url)? {
            return Ok((tag.clone(), text));
        }
    }
    Err(MitosisError::new(
        ErrorCode::NoMatchingPostgres,
        format!(
            "no Postgres {} build for {} in {}",
            version
                .map(str::to_string)
                .unwrap_or_else(|| major.to_string()),
            target_triple(),
            src.base_url
        ),
    )
    .with_hint(
        "check the number; releases: https://github.com/theseus-rs/postgresql-binaries/releases",
    ))
}

/// The hash in a `sha256sum`-style line (`<hex>  <file>`); a bare hash also works.
fn parse_sha256(text: &str) -> Result<String> {
    let h = text
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(h)
    } else {
        Err(invalid("the published checksum file is not a SHA-256 line"))
    }
}

// ---- install --------------------------------------------------------------------------------

fn existing(dir: &Path, major: u32, version: &str) -> Option<ManagedInstall> {
    dir.join("bin/postgres").is_file().then(|| ManagedInstall {
        version: version.to_string(),
        major,
        dir: dir.to_path_buf(),
        used: false,
    })
}

/// The folder holding `bin/postgres`: the unpack folder itself, or its only child folder.
fn archive_root(part: &Path) -> Option<PathBuf> {
    if part.join("bin/postgres").is_file() {
        return Some(part.to_path_buf());
    }
    let dirs: Vec<PathBuf> = std::fs::read_dir(part)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    (dirs.len() == 1 && dirs[0].join("bin/postgres").is_file()).then(|| dirs[0].clone())
}

fn unpack(tarball: &Path, part: &Path) -> Result<()> {
    let gz = flate2::read::GzDecoder::new(std::fs::File::open(tarball)?);
    // `unpack` skips entries that would escape `part` (absolute paths, `..`).
    tar::Archive::new(gz)
        .unpack(part)
        .map_err(|e| invalid(format!("cannot unpack the archive: {e}")))
}

/// Unpacks, checks and moves a verified tarball into place. The caller removes `part` on error.
fn finish(tarball: &Path, part: &Path, final_dir: &Path, major: u32, xy: &str) -> Result<()> {
    unpack(tarball, part)?;
    let root = archive_root(part).ok_or_else(|| {
        invalid("the archive has no bin/postgres")
            .with_hint("this build looks broken; try --version for another one")
    })?;
    let out = Command::new(root.join("bin/postgres"))
        .arg("--version")
        .output()
        .map_err(|e| invalid(format!("the unpacked bin/postgres does not run: {e}")))?;
    let (got_major, got) = parse_version(&String::from_utf8_lossy(&out.stdout))
        .filter(|_| out.status.success())
        .ok_or_else(|| invalid("the unpacked bin/postgres --version gave no version"))?;
    if got_major != major || got != xy {
        return Err(invalid(format!(
            "the archive holds Postgres {got}, expected {xy}"
        )));
    }
    if final_dir.exists() {
        // A half-removed or broken folder of the same name (a valid one returned earlier).
        std::fs::remove_dir_all(final_dir)?;
    }
    std::fs::rename(&root, final_dir)?;
    Ok(())
}

/// Downloads, verifies and unpacks the newest build of `major` (or the exact `version`, `x.y` or
/// the release tag `x.y.z`) into `<home>/pg/<x.y>/`. Prints progress lines. Installing a version
/// that is already there downloads nothing.
// shortcut: two `pg install` of the same version at once can trip over each other's `.part`
// folder; the loser errors out and nothing half-built is ever visible under the final name.
pub fn install(
    home: &Home,
    major: u32,
    version: Option<&str>,
    src: &ReleaseSource,
) -> Result<ManagedInstall> {
    home.ensure_dirs()?;
    let pg = home.pg_dir();
    std::fs::create_dir_all(&pg)?;
    println!(
        "looking up Postgres {} for {}",
        version
            .map(str::to_string)
            .unwrap_or_else(|| major.to_string()),
        target_triple()
    );
    let (tag, sha_text) = resolve(src, major, version)?;
    let xy = minor_of(&tag);
    let final_dir = pg.join(&xy);
    if let Some(i) = existing(&final_dir, major, &xy) {
        println!(
            "Postgres {xy} is already installed at {}",
            final_dir.display()
        );
        return Ok(i);
    }
    let want = parse_sha256(&sha_text)?;
    let asset = asset_name(&tag);
    let tmp = pg.join(format!(".download-{}.tmp", std::process::id()));
    let part = pg.join(format!(".{xy}.part"));
    let cleanup = || {
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_dir_all(&part);
    };
    cleanup();

    println!("downloading {asset}");
    let got = download(&asset_url(src, &tag, &asset), &tmp).inspect_err(|_| cleanup())?;
    if got != want {
        cleanup();
        return Err(invalid(format!(
            "checksum mismatch for {asset}: published {want}, downloaded {got}"
        ))
        .with_hint("nothing was installed; retry, or report it if it keeps happening"));
    }
    println!("checksum ok (sha256 {want})");
    println!("unpacking into {}", final_dir.display());
    std::fs::create_dir_all(&part)?;
    let done = finish(&tmp, &part, &final_dir, major, &xy);
    cleanup();
    done?;
    println!("installed Postgres {xy}");
    Ok(ManagedInstall {
        version: xy,
        major,
        dir: final_dir,
        used: false,
    })
}

// ---- list and remove ------------------------------------------------------------------------

/// The builds under `<home>/pg/`, newest first. `used` says which one discovery picks.
pub fn list(home: &Home) -> Result<Vec<ManagedInstall>> {
    let Ok(rd) = std::fs::read_dir(home.pg_dir()) else {
        return Ok(vec![]);
    };
    let mut out: Vec<ManagedInstall> = rd
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let major = name.split('.').next()?.parse().ok()?;
            if name.starts_with('.') || !name.contains('.') {
                return None;
            }
            existing(&e.path(), major, &name)
        })
        .collect();
    out.sort_by(|a, b| version_key(&b.version).cmp(&version_key(&a.version)));
    let found = discover_with(Some(home));
    for i in &mut out {
        i.used = found
            .iter()
            .find(|f| f.major == i.major)
            .is_some_and(|f| f.bin_dir == i.dir.join("bin"));
    }
    Ok(out)
}

/// Removes the build named by `spec` (a major like `17`, or `x.y`). `in_use` gets the build's
/// `bin` folder (what templates record as `bin_dir`) and refuses the removal when it says true.
pub fn remove(home: &Home, spec: &str, in_use: &dyn Fn(&Path) -> bool) -> Result<ManagedInstall> {
    let by_major = !spec.is_empty() && spec.bytes().all(|b| b.is_ascii_digit());
    let mut hits: Vec<ManagedInstall> = list(home)?
        .into_iter()
        .filter(|i| {
            if by_major {
                spec.parse() == Ok(i.major)
            } else {
                i.version == spec
            }
        })
        .collect();
    let install = match hits.len() {
        0 => {
            return Err(invalid(format!("no managed Postgres {spec}"))
                .with_hint("mitosis pg list shows what is installed"))
        }
        1 => hits.remove(0),
        _ => {
            let names: Vec<&str> = hits.iter().map(|i| i.version.as_str()).collect();
            return Err(invalid(format!(
                "more than one managed Postgres {spec}: {}",
                names.join(", ")
            ))
            .with_hint("name one: mitosis pg remove <x.y>"));
        }
    };
    if in_use(&install.dir.join("bin")) {
        return Err(invalid(format!(
            "Postgres {} is used by a template or a clone",
            install.version
        ))
        .with_hint(
            "destroy those clones and rebuild the templates with another Postgres, then retry",
        ));
    }
    // Rename first so discovery never sees a half-deleted install.
    let gone = home.pg_dir().join(format!(".{}.removing", install.version));
    let _ = std::fs::remove_dir_all(&gone);
    std::fs::rename(&install.dir, &gone)?;
    std::fs::remove_dir_all(&gone)?;
    Ok(install)
}

/// True when a template generation records a `bin_dir` inside `bin_dir`, or a clone folder's
/// `postmaster.opts` names it (a clone that runs, or ran last, with that build). Reads `state.db`
/// read-only like `doctor` does, so it works with or without a daemon. A store it cannot read
/// (an unmigrated v0.1 store, say) counts as in use: start the daemon once, or fix `state.db`.
pub fn used_by_store(home: &Home, bin_dir: &Path) -> bool {
    use rusqlite::{Connection, OpenFlags};
    let path = home.state_db();
    if !path.is_file() {
        return false;
    }
    let Ok(conn) = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) else {
        return true;
    };
    let column = |sql: &str| -> Option<Vec<String>> {
        conn.prepare(sql)
            .ok()?
            .query_map([], |r| r.get::<_, String>(0))
            .ok()?
            .collect::<std::result::Result<_, _>>()
            .ok()
    };
    let Some(bins) = column("SELECT bin_dir FROM generations") else {
        return true;
    };
    if bins.iter().any(|b| Path::new(b).starts_with(bin_dir)) {
        return true;
    }
    let Some(dirs) = column("SELECT dir FROM clones") else {
        return true;
    };
    let needle = bin_dir.to_string_lossy();
    dirs.iter().any(|d| {
        std::fs::read_to_string(Path::new(d).join("postmaster.opts"))
            .is_ok_and(|s| s.contains(needle.as_ref()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_are_filtered_and_sorted() {
        let d = tempfile::tempdir().unwrap();
        for t in [
            "17.9.0", "17.10.1", "17.9.1", "16.4.0", "17.9", "latest", "17.x.1",
        ] {
            std::fs::create_dir_all(d.path().join("download").join(t)).unwrap();
        }
        let src = ReleaseSource {
            base_url: format!("file://{}", d.path().display()),
        };
        assert_eq!(
            list_tags(&src, 17).unwrap(),
            vec!["17.10.1", "17.9.1", "17.9.0"]
        );
    }

    #[test]
    fn parses_checksum_lines() {
        let h = "a".repeat(64);
        assert_eq!(
            parse_sha256(&format!("{h}  postgresql-x.tar.gz\n")).unwrap(),
            h
        );
        assert_eq!(parse_sha256(&h.to_uppercase()).unwrap(), h);
        assert!(parse_sha256("nope").is_err());
        assert!(parse_sha256("").is_err());
    }

    #[test]
    fn minor_and_urls() {
        assert_eq!(minor_of("17.9.1"), "17.9");
        let src = ReleaseSource {
            base_url: "https://x/releases/".into(),
        };
        assert_eq!(
            asset_url(&src, "17.9.1", "a.tar.gz"),
            "https://x/releases/download/17.9.1/a.tar.gz"
        );
        assert!(asset_name("17.9.1").starts_with("postgresql-17.9.1-"));
        assert!(asset_name("17.9.1").ends_with("-apple-darwin.tar.gz"));
    }
}
