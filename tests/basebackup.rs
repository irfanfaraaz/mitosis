//! `template init --basebackup`: a physical copy of a running server as a template source.

mod common;

use common::*;
use mitosis::clones::create;
use mitosis::error::ErrorCode;
use mitosis::pg::{discover, initdb, make_standalone, newest};
use mitosis::protocol::{CloneCreateArgs, TemplateSource};
use mitosis::templates;
use std::io::Write;

const REP_PASSWORD: &str = "rep-secret-pw-91";
const PLAIN_PASSWORD: &str = "plain-secret-pw-77";

fn clone_args(template: &str) -> CloneCreateArgs {
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
    }
}

/// A durable clone with two roles, `rep` (REPLICATION) and `plain` (not), and a `replication`
/// line for `rep` in its `pg_hba.conf`. Returns `host:port/db` of the clone.
async fn replication_source(env: &TestEnv) -> Option<String> {
    template(
        env,
        "src",
        "create table t(id int); insert into t values (1), (2), (3)",
    )
    .await;
    let mut args = clone_args("src");
    args.durable = true;
    let src = match create(&env.ctx, args).await {
        Ok(c) => c,
        Err(e) => {
            println!("skipping: cannot set up the source clone: {e}");
            return None;
        }
    };
    let setup = format!(
        "create role rep replication login password '{REP_PASSWORD}'; \
         create role plain login password '{PLAIN_PASSWORD}'"
    );
    psql_url(env, &src.url, &setup);
    let hba = env.ctx.home.clones_dir().join(&src.id).join("pg_hba.conf");
    let appended = std::fs::OpenOptions::new()
        .append(true)
        .open(&hba)
        .and_then(|mut f| writeln!(f, "host replication rep 127.0.0.1/32 scram-sha-256"));
    if let Err(e) = appended {
        println!("skipping: cannot edit {}: {e}", hba.display());
        return None;
    }
    psql_url(env, &src.url, "select pg_reload_conf()");
    // The clone URL is postgres://postgres:<password>@host:port/db.
    let (_, rest) = src.url.split_once('@')?;
    Some(rest.to_string())
}

fn url_of(user: &str, password: &str, rest: &str) -> String {
    format!("postgres://{user}:{password}@{rest}")
}

#[tokio::test]
async fn basebackup_from_running_clone() {
    let Some(env) = TestEnv::new() else { return };
    let Some(rest) = replication_source(&env).await else {
        return;
    };
    let url = url_of("rep", REP_PASSWORD, &rest);

    let info = templates::init(
        &env.ctx,
        template_args("bb", TemplateSource::Basebackup { url }),
    )
    .await
    .unwrap();

    assert_eq!(info.database, "app");
    // The listed URL has no password.
    let listed = serde_json::to_string(&templates::list(&env.ctx).unwrap()).unwrap();
    assert!(!listed.contains(REP_PASSWORD), "{listed}");
    assert!(listed.contains("\"kind\":\"basebackup\""), "{listed}");
    let log = std::fs::read_to_string(env.ctx.home.logs_dir().join("template-bb.log")).unwrap();
    assert!(!log.contains(REP_PASSWORD), "{log}");
    // A normal standalone cluster: no standby file, no leftover build folder.
    let gen_dir = env.ctx.home.templates_dir().join("bb@1");
    assert!(!gen_dir.join("standby.signal").exists());
    assert!(!gen_dir.join("postmaster.pid").exists());

    let copy = create(&env.ctx, clone_args("bb")).await.unwrap();
    assert_eq!(psql_url(&env, &copy.url, "select count(*) from t"), "3");
}

#[tokio::test]
async fn basebackup_refusal_explains_replication() {
    let Some(env) = TestEnv::new() else { return };
    let Some(rest) = replication_source(&env).await else {
        return;
    };
    let url = url_of("plain", PLAIN_PASSWORD, &rest);

    let e = templates::init(
        &env.ctx,
        template_args("bb", TemplateSource::Basebackup { url }),
    )
    .await
    .unwrap_err();

    assert_eq!(e.code, ErrorCode::TemplateBuildFailed);
    assert!(e.message.contains("REPLICATION"), "{e}");
    assert!(e.message.contains("pg_hba.conf"), "{e}");
    assert!(!e.to_string().contains(PLAIN_PASSWORD), "{e}");
    // A failed first build leaves no template and no folder behind.
    assert!(env.ctx.store().get_template("bb").unwrap().is_none());
    let left: Vec<_> = std::fs::read_dir(env.ctx.home.templates_dir())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(".bb."))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

#[tokio::test]
async fn basebackup_unknown_database_is_invalid_request() {
    let Some(env) = TestEnv::new() else { return };
    let Some(rest) = replication_source(&env).await else {
        return;
    };
    let url = url_of("rep", REP_PASSWORD, &rest);
    let mut args = template_args("bb", TemplateSource::Basebackup { url });
    args.database = Some("nope".into());

    let e = templates::init(&env.ctx, args).await.unwrap_err();

    assert_eq!(e.code, ErrorCode::InvalidRequest, "{e}");
    assert!(
        e.message.contains("nope") && e.message.contains("app"),
        "{e}"
    );
    assert!(env.ctx.store().get_template("bb").unwrap().is_none());
}

#[test]
fn make_standalone_cleans_files() {
    let tmp = tempfile::tempdir().unwrap();
    let d = tmp.path();
    for f in [
        "standby.signal",
        "recovery.signal",
        "postmaster.pid",
        "postmaster.opts",
    ] {
        std::fs::write(d.join(f), "x").unwrap();
    }
    std::fs::write(
        d.join("pg_hba.conf"),
        "host replication rep 10.0.0.0/8 md5\n",
    )
    .unwrap();
    std::fs::write(d.join("postgresql.auto.conf"), "primary_conninfo = 'x'\n").unwrap();
    std::fs::write(d.join("postgresql.conf"), "shared_buffers = 1GB\n").unwrap();

    let warnings = make_standalone(d).unwrap();

    assert!(warnings.is_empty(), "{warnings:?}");
    for f in [
        "standby.signal",
        "recovery.signal",
        "postmaster.pid",
        "postmaster.opts",
    ] {
        assert!(!d.join(f).exists(), "{f} should be gone");
    }
    assert_eq!(
        std::fs::read_to_string(d.join("postgresql.auto.conf")).unwrap(),
        ""
    );
    let hba = std::fs::read_to_string(d.join("pg_hba.conf")).unwrap();
    assert!(!hba.contains("md5"), "{hba}");

    // The new pg_hba.conf says what initdb writes for Mitosis.
    let installs = discover();
    let Ok(install) = newest(&installs) else {
        println!("skipping the initdb comparison: no Postgres installed");
        return;
    };
    let real = tmp.path().join("real");
    initdb(install, &real).unwrap();
    let words = |text: &str| -> Vec<Vec<String>> {
        text.lines()
            .filter(|l| !l.trim().is_empty() && !l.trim_start().starts_with('#'))
            .map(|l| l.split_whitespace().map(String::from).collect())
            .collect()
    };
    let real_hba = std::fs::read_to_string(real.join("pg_hba.conf")).unwrap();
    assert_eq!(words(&hba), words(&real_hba));
}
