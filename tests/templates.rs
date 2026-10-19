mod common;

use common::*;
use mitosis::ctx::{Config, Ctx};
use mitosis::error::ErrorCode;
use mitosis::paths::Home;
use mitosis::pg::newest;
use mitosis::protocol::TemplateSource;
use mitosis::state::TemplateRow;
use mitosis::templates::{self, hash_dir, info, redact_url};
use std::path::PathBuf;
use tempfile::TempDir;

/// A Ctx that needs no Postgres, for tests that must always run.
fn bare_ctx() -> (TempDir, Ctx) {
    let tmp = tmp_home();
    let ctx = Ctx::open(Home::at(tmp.path().to_path_buf()), Config::default()).unwrap();
    (tmp, ctx)
}

fn leftovers(ctx: &Ctx, name: &str) -> Vec<String> {
    std::fs::read_dir(ctx.home.templates_dir())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|f| f.starts_with(&format!(".{name}.")))
        .collect()
}

fn row(name: &str, source: TemplateSource) -> TemplateRow {
    TemplateRow {
        name: name.into(),
        dir: PathBuf::from("/tmp/none"),
        pg_major: 16,
        bin_dir: PathBuf::from("/opt/pg/bin"),
        extensions: vec!["plpgsql".into()],
        database: "app".into(),
        source,
        migrations: None,
        migrations_hash: None,
        max_age_secs: 7 * 86_400,
        built_at: 1_700_000_000,
        size_bytes: 1,
    }
}

#[tokio::test]
async fn seed_template_builds_and_records_metadata() {
    let Some(env) = TestEnv::new() else { return };
    let t = template(
        &env,
        "main",
        "create table t(id int); insert into t values (1)",
    )
    .await;
    assert_eq!(t.database, "app");
    assert!(t.pg_major >= 12);
    assert!(t.size_bytes > 0);
    let dir = env.ctx.home.templates_dir().join("main");
    assert!(dir.join("PG_VERSION").exists());
    assert!(
        !dir.join("postmaster.pid").exists(),
        "build server must be stopped"
    );
    assert!(leftovers(&env.ctx, "main").is_empty());
    let row = env.ctx.store().get_template("main").unwrap().unwrap();
    assert!(row.extensions.iter().any(|e| e == "plpgsql"));
    assert_eq!(templates::list(&env.ctx).unwrap().len(), 1);
}

#[tokio::test]
async fn failing_seed_leaves_old_template_intact() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(id int)").await;
    let before = env.ctx.store().get_template("main").unwrap().unwrap();
    let e = templates::init(&env.ctx, template_args("main", seed("select * from nope")))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::TemplateBuildFailed);
    assert!(e.message.contains("seed"), "{}", e.message);
    assert!(
        e.message.contains("nope"),
        "stderr tail missing: {}",
        e.message
    );
    assert!(env
        .ctx
        .home
        .templates_dir()
        .join("main/PG_VERSION")
        .exists());
    assert!(leftovers(&env.ctx, "main").is_empty());
    assert_eq!(
        env.ctx.store().get_template("main").unwrap().unwrap(),
        before
    );
}

#[tokio::test]
async fn seed_runs_in_its_cwd() {
    let Some(env) = TestEnv::new() else { return };
    let work = env.tmp.path().join("work");
    std::fs::create_dir(&work).unwrap();
    std::fs::write(work.join("schema.sql"), "create table s(id int);").unwrap();
    let psql = newest(&env.ctx.installs).unwrap().bin("psql");
    let source = TemplateSource::Seed {
        command: format!(
            "'{}' \"$DATABASE_URL\" -v ON_ERROR_STOP=1 -X -f schema.sql",
            psql.display()
        ),
        cwd: Some(work),
    };
    templates::init(&env.ctx, template_args("main", source))
        .await
        .unwrap();
}

#[tokio::test]
async fn refresh_reuses_source_and_updates_built_at() {
    let Some(env) = TestEnv::new() else { return };
    let t1 = template(&env, "main", "create table t(id int)").await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let t2 = templates::refresh(&env.ctx, "main").await.unwrap();
    assert!(t2.built_at > t1.built_at);
    assert_eq!(t2.source, t1.source);
    assert_eq!(t2.database, t1.database);
    assert_eq!(t2.pg_major, t1.pg_major);
    assert!(env
        .ctx
        .home
        .templates_dir()
        .join("main/PG_VERSION")
        .exists());
    assert!(leftovers(&env.ctx, "main").is_empty());
}

#[tokio::test]
async fn refresh_unknown_template_not_found() {
    let (_tmp, ctx) = bare_ctx();
    assert_eq!(
        templates::refresh(&ctx, "nope").await.unwrap_err().code,
        ErrorCode::TemplateNotFound
    );
}

#[test]
fn list_masks_url_password() {
    assert_eq!(
        redact_url("postgres://u:secret@h:5432/db"),
        "postgres://u:***@h:5432/db"
    );
    assert_eq!(redact_url("postgres://h/db"), "postgres://h/db");
    let (_tmp, ctx) = bare_ctx();
    let url = "postgres://u:secret@h:5432/db".to_string();
    ctx.store()
        .upsert_template(&row("main", TemplateSource::Url { url }))
        .unwrap();
    let list = templates::list(&ctx).unwrap();
    assert_eq!(
        list[0].source,
        TemplateSource::Url {
            url: "postgres://u:***@h:5432/db".into()
        }
    );
    assert!(!serde_json::to_string(&list).unwrap().contains("secret"));
}

#[test]
fn stale_and_migrations_warnings() {
    let tmp = tmp_home();
    let mig = tmp.path().join("migrations");
    std::fs::create_dir_all(mig.join("sub")).unwrap();
    std::fs::write(mig.join("001.sql"), "create table a(id int);").unwrap();
    std::fs::write(mig.join("sub/002.sql"), "create table b(id int);").unwrap();
    let now = 1_800_000_000;
    let mut r = row(
        "main",
        TemplateSource::Seed {
            command: "true".into(),
            cwd: None,
        },
    );
    r.built_at = now - 8 * 86_400;
    r.migrations = Some(mig.clone());
    r.migrations_hash = Some(hash_dir(&mig).unwrap());

    let i = info(&r, now);
    assert_eq!(i.age_secs, 8 * 86_400);
    assert_eq!(
        i.warnings,
        vec!["template is 8 days old; run: mitosis template refresh main".to_string()]
    );

    r.built_at = now - 60;
    assert!(info(&r, now).warnings.is_empty());

    std::fs::write(mig.join("sub/002.sql"), "create table b(id bigint);").unwrap();
    assert_eq!(
        info(&r, now).warnings,
        vec![format!(
            "migrations in {} changed since the template was built",
            mig.display()
        )]
    );

    std::fs::remove_dir_all(&mig).unwrap();
    assert!(hash_dir(&mig).is_err());
    assert!(
        info(&r, now).warnings.is_empty(),
        "a missing migrations folder gives no warning"
    );
}

#[tokio::test]
async fn invalid_template_name_rejected() {
    let (_tmp, ctx) = bare_ctx();
    let source = TemplateSource::Seed {
        command: "true".into(),
        cwd: None,
    };
    let e = templates::init(&ctx, template_args("../x", source))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    for d in [
        ctx.home.templates_dir(),
        ctx.home.sockets_dir(),
        ctx.home.logs_dir(),
    ] {
        assert_eq!(
            std::fs::read_dir(&d).unwrap().count(),
            0,
            "{} not empty",
            d.display()
        );
    }
}
