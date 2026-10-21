#![cfg(target_os = "linux")]
//! ZFS back end, end to end. Runs only with `MITOSIS_BACKEND=zfs` on a pool set up by
//! `mitosis zfs setup` (see the `zfs` job in .github/workflows/ci.yml); skips otherwise.
//! `MITOSIS_ZFS_SOURCE` names the separate dataset for the running-source test (default tank/src).

mod common;

use common::*;
use mitosis::clones::{create, destroy};
use mitosis::names::random_hex;
use mitosis::pg::newest;
use mitosis::protocol::{CloneCreateArgs, TemplateSource};
use mitosis::templates::{self, history};
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

fn zfs_selected() -> bool {
    let on = std::env::var("MITOSIS_BACKEND").is_ok_and(|b| b == "zfs");
    if !on {
        println!("skipping: MITOSIS_BACKEND is not zfs");
    }
    on
}

fn args(template: &str) -> CloneCreateArgs {
    CloneCreateArgs {
        template: template.into(),
        name: None,
        ttl_secs: 7200,
        durable: false,
        parent_pid: None,
        fresh: false,
        protect: false,
        env: None,
        wait_secs: None,
        branch: None,
    }
}

const VALUES: &str = "select string_agg(v, ',' order by v) from t";
const SEED: &str = "create table t(v text); insert into t values ('a'), ('b')";

#[tokio::test]
async fn zfs_clone_destroy_roundtrip() {
    if !zfs_selected() {
        return;
    }
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;

    let c = create(&env.ctx, args("main")).await.unwrap();
    assert_eq!(psql_url(&env, &c.url, VALUES), "a,b");
    psql_url(&env, &c.url, "insert into t values ('c')");
    assert_eq!(psql_url(&env, &c.url, VALUES), "a,b,c");

    // The template is untouched by writes to the clone.
    let other = create(&env.ctx, args("main")).await.unwrap();
    assert_eq!(psql_url(&env, &other.url, VALUES), "a,b");

    let dir = env.ctx.home.clones_dir().join(&c.id);
    assert!(dir.join("PG_VERSION").exists());
    destroy(&env.ctx, &c.id).await.unwrap();
    assert!(!dir.exists(), "{} still exists", dir.display());
    destroy(&env.ctx, &other.id).await.unwrap();
}

#[tokio::test]
async fn zfs_generation_kept_while_cloned() {
    if !zfs_selected() {
        return;
    }
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", SEED).await;
    {
        let store = env.ctx.store();
        let mut t = store.get_template("main").unwrap().unwrap();
        t.keep = 1;
        store.update_template_settings(&t).unwrap();
    }
    let early = create(&env.ctx, args("main")).await.unwrap();
    assert_eq!(early.generation, 1);
    for _ in 0..2 {
        templates::refresh(&env.ctx, "main", None).await.unwrap();
    }

    // Generation 1 is pruned from the history, yet the clone made from it keeps working.
    let gens: Vec<u32> = history(&env.ctx, "main")
        .unwrap()
        .iter()
        .map(|g| g.gen)
        .collect();
    assert!(!gens.contains(&1), "{gens:?}");
    assert_eq!(psql_url(&env, &early.url, VALUES), "a,b");

    // Destroying the clone afterwards succeeds and the newest generation still clones.
    destroy(&env.ctx, &early.id).await.unwrap();
    let late = create(&env.ctx, args("main")).await.unwrap();
    assert_eq!(psql_url(&env, &late.url, VALUES), "a,b");
}

/// A Postgres of the newest install running on its own dataset, stopped on drop.
struct SourcePg {
    bin_dir: std::path::PathBuf,
    data: std::path::PathBuf,
    url: String,
    _sock: tempfile::TempDir,
}

impl SourcePg {
    fn start(env: &TestEnv, dataset: &str) -> SourcePg {
        let data = env
            .ctx
            .backend
            .external_mountpoint(dataset)
            .expect("mountpoint of the source dataset");
        let installs = env.ctx.installs();
        let install = newest(&installs).expect("postgres installed");
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o700)).unwrap();
        mitosis::pg::initdb(install, &data).expect("initdb on the source dataset");
        let sock = tempfile::Builder::new().prefix("src").tempdir_in("/tmp").unwrap();
        let port = 54_000 + u16::from_str_radix(&random_hex(2), 16).unwrap() % 1000;
        let out = Command::new(install.bin("pg_ctl"))
            .arg("-D")
            .arg(&data)
            .arg("-w")
            .arg("-l")
            .arg(sock.path().join("log"))
            .arg("-o")
            .arg(format!(
                "-p {port} -k {} -c listen_addresses=''",
                sock.path().display()
            ))
            .arg("start")
            .output()
            .unwrap();
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        let url = format!(
            "postgresql:///postgres?host={}&port={port}&user=postgres",
            sock.path().display()
        );
        SourcePg {
            bin_dir: install.bin("pg_ctl").parent().unwrap().to_path_buf(),
            data,
            url,
            _sock: sock,
        }
    }
}

impl Drop for SourcePg {
    fn drop(&mut self) {
        let _ = Command::new(self.bin_dir.join("pg_ctl"))
            .args(["-m", "immediate", "-D"])
            .arg(&self.data)
            .arg("stop")
            .output();
    }
}

#[tokio::test]
async fn running_source_snapshot_recovers() {
    if !zfs_selected() {
        return;
    }
    let dataset = std::env::var("MITOSIS_ZFS_SOURCE").unwrap_or_else(|_| "tank/src".into());
    let Some(env) = TestEnv::new() else { return };
    let src = SourcePg::start(&env, &dataset);
    psql_url(&env, &src.url, "create table t(v text)");
    psql_url(&env, &src.url, "insert into t values ('a'), ('b')");

    let info = templates::init(
        &env.ctx,
        template_args("live", TemplateSource::Running { dataset }),
    )
    .await
    .unwrap();
    assert_eq!((info.current_gen, info.generations), (1, 1));

    // The snapshot was taken under a live server; the copy recovers and holds the rows.
    let c = create(&env.ctx, args("live")).await.unwrap();
    assert_eq!(psql_url(&env, &c.url, VALUES), "a,b");

    // The source kept running, and writes to either side do not reach the other.
    psql_url(&env, &src.url, "insert into t values ('x')");
    assert_eq!(psql_url(&env, &src.url, VALUES), "a,b,x");
    assert_eq!(psql_url(&env, &c.url, VALUES), "a,b");
}
