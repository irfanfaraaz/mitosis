//! `template init --basebackup`: a physical copy of a running server as a template source.

mod common;

use common::*;
use mitosis::clones::create;
use mitosis::error::ErrorCode;
use mitosis::pg::{disable_preload_libraries, discover, initdb, make_standalone, newest};
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
        branch: None,
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
    std::fs::write(d.join("pg_ident.conf"), "").unwrap();

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

#[test]
fn make_standalone_without_conf_files_writes_defaults() {
    // The Debian layout: pg_basebackup copies only the data folder, which then has neither
    // postgresql.conf nor pg_ident.conf.
    let tmp = tempfile::tempdir().unwrap();
    let d = tmp.path();

    let warnings = make_standalone(d).unwrap();

    assert!(
        warnings.iter().any(|w| w.contains("postgresql.conf")),
        "{warnings:?}"
    );
    assert!(
        warnings.iter().any(|w| w.contains("pg_ident.conf")),
        "{warnings:?}"
    );
    for f in ["postgresql.conf", "pg_ident.conf", "pg_hba.conf"] {
        assert!(d.join(f).is_file(), "{f} should exist");
    }
    // Nothing to disable, and no file needed for that.
    let tmp2 = tempfile::tempdir().unwrap();
    assert!(disable_preload_libraries(tmp2.path()).unwrap().is_empty());
}

#[test]
fn make_standalone_turns_off_archive_and_ssl() {
    let tmp = tempfile::tempdir().unwrap();
    let d = tmp.path();
    std::fs::write(
        d.join("postgresql.conf"),
        "shared_buffers = 1GB\narchive_mode = on\nARCHIVE_COMMAND = 'aws s3 cp %p s3://wal/%f'\n\
         ssl = on\nssl_cert_file = '/etc/ssl/pg.crt'\nrestore_command = 'cp /a/%f %p'\n\
         primary_conninfo = 'host=prod password=hunter2-secret'\nprimary_slot_name = 'slot'\n\
         synchronous_standby_names = 'standby1'\nhba_file = '/etc/pg/hba'\n",
    )
    .unwrap();

    let warnings = make_standalone(d).unwrap();

    let conf = std::fs::read_to_string(d.join("postgresql.conf")).unwrap();
    // The originals are commented out, not deleted.
    for original in [
        "#mitosis: archive_mode = on",
        "#mitosis: ARCHIVE_COMMAND = 'aws s3 cp %p s3://wal/%f'",
        "#mitosis: ssl = on",
        "#mitosis: restore_command = 'cp /a/%f %p'",
        "#mitosis: primary_conninfo = 'host=prod password=hunter2-secret'",
        "#mitosis: primary_slot_name = 'slot'",
        "#mitosis: synchronous_standby_names = 'standby1'",
        "#mitosis: hba_file = '/etc/pg/hba'",
    ] {
        assert!(conf.contains(original), "{original} in:\n{conf}");
    }
    // The values that now apply: the last line of each setting.
    let last = |name: &str| {
        conf.lines()
            .rfind(|l| !l.starts_with('#') && l.split(" =").next() == Some(name))
            .map(String::from)
    };
    assert_eq!(last("archive_mode").as_deref(), Some("archive_mode = off"));
    assert_eq!(
        last("archive_command").as_deref(),
        Some("archive_command = ''")
    );
    assert_eq!(last("ssl").as_deref(), Some("ssl = off"));
    assert_eq!(
        last("restore_command").as_deref(),
        Some("restore_command = ''")
    );
    assert_eq!(
        last("primary_conninfo").as_deref(),
        Some("primary_conninfo = ''")
    );
    assert_eq!(
        last("primary_slot_name").as_deref(),
        Some("primary_slot_name = ''")
    );
    assert_eq!(
        last("synchronous_standby_names").as_deref(),
        Some("synchronous_standby_names = ''")
    );
    assert_eq!(last("hba_file"), None);
    assert!(conf.lines().any(|l| l == "shared_buffers = 1GB"));
    // Warnings name the settings and never show a value (it can hold a password or keys).
    let all = warnings.join("\n");
    for name in ["archive_mode", "archive_command", "primary_conninfo", "ssl"] {
        assert!(all.contains(name), "{name} in {warnings:?}");
    }
    for value in [
        "hunter2-secret",
        "password=",
        "aws s3",
        "s3://wal",
        "host=prod",
    ] {
        assert!(!all.contains(value), "{value} leaked in {warnings:?}");
    }
}
