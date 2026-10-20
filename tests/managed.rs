//! `mitosis pg install | list | remove` against a fake `file://` release. Nothing here touches the
//! network except `real_download`, which runs only with MITOSIS_TEST_REAL_DOWNLOAD=1.

use mitosis::error::ErrorCode;
use mitosis::paths::Home;
use mitosis::pg::discover_with;
use mitosis::pg::managed::{self, ReleaseSource};
use sha2::{Digest, Sha256};
use std::path::Path;

/// A fake major no real machine has, so a system install can never hide the managed one.
const MAJOR: u32 = 99;

fn tarball(version: &str) -> Vec<u8> {
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut b = tar::Builder::new(gz);
    for name in ["postgres", "initdb", "pg_ctl", "psql", "pg_isready"] {
        let body = if name == "postgres" {
            format!("#!/bin/sh\necho 'postgres (PostgreSQL) {version}'\n")
        } else {
            "#!/bin/sh\nexit 0\n".to_string()
        };
        let mut h = tar::Header::new_gnu();
        h.set_size(body.len() as u64);
        h.set_mode(0o755);
        b.append_data(&mut h, format!("bin/{name}"), body.as_bytes())
            .unwrap();
    }
    b.into_inner().unwrap().finish().unwrap()
}

/// Adds `download/<tag>/postgresql-<tag>-<target>.tar.gz` and its `.sha256` (`wrong` swaps in a bad hash).
fn add_release(root: &Path, tag: &str, printed_version: &str, wrong: bool) {
    let dir = root.join("download").join(tag);
    std::fs::create_dir_all(&dir).unwrap();
    let asset = format!("postgresql-{tag}-{}.tar.gz", managed::target_triple());
    let bytes = tarball(printed_version);
    std::fs::write(dir.join(&asset), &bytes).unwrap();
    let hash: String = if wrong {
        "0".repeat(64)
    } else {
        Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    };
    std::fs::write(
        dir.join(format!("{asset}.sha256")),
        format!("{hash}  {asset}\n"),
    )
    .unwrap();
}

fn setup() -> (tempfile::TempDir, Home, ReleaseSource) {
    let tmp = tempfile::tempdir().unwrap();
    let release = tmp.path().join("release");
    std::fs::create_dir(&release).unwrap();
    let home = Home::at(tmp.path().join("home"));
    let src = ReleaseSource {
        base_url: format!("file://{}", release.display()),
    };
    (tmp, home, src)
}

fn release_dir(tmp: &tempfile::TempDir) -> std::path::PathBuf {
    tmp.path().join("release")
}

#[test]
fn install_from_fake_release_verifies_checksum() {
    let (tmp, home, src) = setup();
    add_release(&release_dir(&tmp), "99.9.0", "99.9", false);
    add_release(&release_dir(&tmp), "99.9.1", "99.9", false);
    add_release(&release_dir(&tmp), "99.8.0", "99.8", false);

    let i = managed::install(&home, MAJOR, None, &src).unwrap();
    assert_eq!((i.version.as_str(), i.major), ("99.9", MAJOR));
    assert_eq!(i.dir, home.pg_dir().join("99.9"));
    assert!(i.dir.join("bin/postgres").is_file());

    let listed = managed::list(&home).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].version, "99.9");
    assert!(listed[0].used, "nothing else of this major exists");

    let found = discover_with(Some(&home));
    assert!(found
        .iter()
        .any(|f| f.major == MAJOR && f.bin_dir == i.dir.join("bin")));

    // Installing it again finds it in place and returns the same folder.
    assert_eq!(
        managed::install(&home, MAJOR, None, &src).unwrap().dir,
        i.dir
    );

    // An older minor on request, resolved to its newest build.
    let (tmp2, home2, src2) = setup();
    add_release(&release_dir(&tmp2), "99.9.1", "99.9", false);
    add_release(&release_dir(&tmp2), "99.8.0", "99.8", false);
    let old = managed::install(&home2, MAJOR, Some("99.8"), &src2).unwrap();
    assert_eq!(old.version, "99.8");
    let both = managed::install(&home2, MAJOR, None, &src2).unwrap();
    assert_eq!(both.version, "99.9");
    let listed = managed::list(&home2).unwrap();
    let flags: Vec<(&str, bool)> = listed
        .iter()
        .map(|i| (i.version.as_str(), i.used))
        .collect();
    assert_eq!(flags, vec![("99.9", true), ("99.8", false)]);

    // A wrong major for --version is refused before any lookup.
    let e = managed::install(&home2, 98, Some("99.9"), &src2).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
}

#[test]
fn bad_checksum_refused_and_cleaned() {
    let (tmp, home, src) = setup();
    add_release(&release_dir(&tmp), "99.9.0", "99.9", true);
    let e = managed::install(&home, MAJOR, None, &src).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e.message.contains("checksum mismatch"), "{e}");
    let left: Vec<_> = std::fs::read_dir(home.pg_dir())
        .unwrap()
        .flatten()
        .map(|e| e.file_name())
        .collect();
    assert!(left.is_empty(), "left behind: {left:?}");
    assert!(managed::list(&home).unwrap().is_empty());
}

#[test]
fn archive_with_another_version_is_refused_and_cleaned() {
    let (tmp, home, src) = setup();
    add_release(&release_dir(&tmp), "99.9.0", "99.7", false);
    let e = managed::install(&home, MAJOR, None, &src).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert_eq!(std::fs::read_dir(home.pg_dir()).unwrap().count(), 0);
}

#[test]
fn missing_build_for_target_is_reported() {
    let (_tmp, home, src) = setup();
    let e = managed::install(&home, MAJOR, None, &src).unwrap_err();
    assert_eq!(e.code, ErrorCode::NoMatchingPostgres);
}

#[test]
fn remove_refuses_in_use() {
    let (tmp, home, src) = setup();
    add_release(&release_dir(&tmp), "99.9.0", "99.9", false);
    let i = managed::install(&home, MAJOR, None, &src).unwrap();

    let e = managed::remove(&home, "99", &|bin| bin == i.dir.join("bin")).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(i.dir.join("bin/postgres").is_file(), "still installed");

    let e = managed::remove(&home, "98", &|_| false).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);

    let gone = managed::remove(&home, "99.9", &|_| false).unwrap();
    assert_eq!(gone.version, "99.9");
    assert!(!i.dir.exists());
    assert!(managed::list(&home).unwrap().is_empty());
    assert!(!discover_with(Some(&home)).iter().any(|f| f.major == MAJOR));
}

#[test]
fn store_use_is_read_from_generations_and_clone_folders() {
    let tmp = tempfile::tempdir().unwrap();
    let home = Home::at(tmp.path().to_path_buf());
    let bin = home.pg_dir().join("99.9/bin");
    assert!(!managed::used_by_store(&home, &bin), "no store, not in use");

    let clone_dir = tmp.path().join("clones/c1");
    std::fs::create_dir_all(&clone_dir).unwrap();
    let conn = rusqlite::Connection::open(home.state_db()).unwrap();
    conn.execute_batch(&format!(
        "CREATE TABLE generations (bin_dir TEXT); CREATE TABLE clones (dir TEXT);
         INSERT INTO generations VALUES ('/opt/homebrew/opt/postgresql@17/bin');
         INSERT INTO clones VALUES ('{}');",
        clone_dir.display()
    ))
    .unwrap();
    assert!(!managed::used_by_store(&home, &bin));

    std::fs::write(
        clone_dir.join("postmaster.opts"),
        format!("{}/postgres \"-D\" \"x\"\n", bin.display()),
    )
    .unwrap();
    assert!(managed::used_by_store(&home, &bin), "a clone ran with it");
    std::fs::remove_file(clone_dir.join("postmaster.opts")).unwrap();

    conn.execute(
        "INSERT INTO generations VALUES (?1)",
        [bin.to_string_lossy().as_ref()],
    )
    .unwrap();
    assert!(
        managed::used_by_store(&home, &bin),
        "a template generation uses it"
    );
}

/// Downloads a real build (about 90 MB) from GitHub into a temp home. Never part of a normal run.
#[test]
fn real_download() {
    if std::env::var("MITOSIS_TEST_REAL_DOWNLOAD").as_deref() != Ok("1") {
        eprintln!("skipped: set MITOSIS_TEST_REAL_DOWNLOAD=1 to download a real Postgres 17");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let home = Home::at(tmp.path().join("home"));
    let i = managed::install(&home, 17, None, &ReleaseSource::from_env()).unwrap();
    assert_eq!(i.major, 17);
    assert!(discover_with(Some(&home)).iter().any(|f| f.major == 17));
}
