mod common;

use common::*;
use mitosis::clones::create;
use mitosis::ctx::{Config, Ctx};
use mitosis::error::ErrorCode;
use mitosis::paths::Home;
use mitosis::pg::newest;
use mitosis::protocol::{CloneCreateArgs, GenKind, TemplateSource};
use mitosis::state::{GenerationRow, TemplateRow};
use mitosis::templates::{self, hash_dir, info, redact_url};
use std::path::PathBuf;
use tempfile::TempDir;

// Built at runtime so no `scheme://user:password@host` literal sits in the source.
fn pg_url(user: &str, pass: &str, rest: &str) -> String {
    format!("postgres://{user}:{pass}@{rest}")
}

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

/// A template whose current generation is 1.
fn row(name: &str, source: TemplateSource) -> (TemplateRow, GenerationRow) {
    let mut t = TemplateRow::new(name, "app", source, None, 7 * 86_400);
    t.current_gen = 1;
    t.next_gen = 2;
    let g = GenerationRow {
        template: name.into(),
        gen: 1,
        kind: GenKind::Full,
        dir: PathBuf::from("/tmp/none"),
        pg_major: 16,
        bin_dir: PathBuf::from("/opt/pg/bin"),
        extensions: vec!["plpgsql".into()],
        migrations_hash: None,
        built_at: 1_700_000_000,
        size_bytes: 1,
        from_clone: None,
        from_gen: None,
    };
    (t, g)
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
    assert_eq!((t.current_gen, t.generations), (1, 1));
    let dir = env.ctx.home.templates_dir().join("main@1");
    assert!(dir.join("PG_VERSION").exists());
    assert!(
        !dir.join("postmaster.pid").exists(),
        "build server must be stopped"
    );
    assert!(leftovers(&env.ctx, "main").is_empty());
    let (_, gen) = env.ctx.store().current("main").unwrap().unwrap();
    assert_eq!(gen.dir, dir);
    assert!(gen.extensions.iter().any(|e| e == "plpgsql"));
    assert_eq!(templates::list(&env.ctx).unwrap().len(), 1);
}

#[tokio::test]
async fn failing_seed_leaves_old_template_intact() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(id int)").await;
    let before = env.ctx.store().current("main").unwrap().unwrap();
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
        .join("main@1/PG_VERSION")
        .exists());
    assert!(leftovers(&env.ctx, "main").is_empty());
    assert_eq!(env.ctx.store().current("main").unwrap().unwrap(), before);
    assert_eq!(env.ctx.store().list_generations("main").unwrap().len(), 1);
}

#[tokio::test]
async fn seed_runs_in_its_cwd() {
    let Some(env) = TestEnv::new() else { return };
    let work = env.tmp.path().join("work");
    std::fs::create_dir(&work).unwrap();
    std::fs::write(work.join("schema.sql"), "create table s(id int);").unwrap();
    let psql = newest(&env.ctx.installs()).unwrap().bin("psql");
    let source = TemplateSource::Seed {
        command: format!(
            "'{}' \"$DATABASE_URL\" -v ON_ERROR_STOP=1 -X -f schema.sql",
            psql.display()
        ),
        cwd: Some(work),
        path: None,
    };
    templates::init(&env.ctx, template_args("main", source))
        .await
        .unwrap();
}

#[tokio::test]
async fn seed_uses_the_callers_path() {
    use std::os::unix::fs::PermissionsExt;
    let Some(env) = TestEnv::new() else { return };
    let bin = tempfile::tempdir().unwrap();
    let tool = bin.path().join("mitosis-test-seed");
    let psql = newest(&env.ctx.installs()).unwrap().bin("psql");
    std::fs::write(
        &tool,
        format!(
            "#!/bin/sh\nexec '{}' \"$DATABASE_URL\" -v ON_ERROR_STOP=1 -X -c 'create table p(id int)'\n",
            psql.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:/usr/bin:/bin", bin.path().display());
    let seed = |path: Option<String>| TemplateSource::Seed {
        command: "mitosis-test-seed".into(),
        cwd: None,
        path,
    };

    templates::init(&env.ctx, template_args("main", seed(Some(path.clone()))))
        .await
        .unwrap();
    // A refresh without a PATH reuses the recorded one.
    templates::refresh(&env.ctx, "main", None).await.unwrap();
    // Without it the tool is not found: the daemon's own PATH does not have it.
    let e = templates::init(&env.ctx, template_args("other", seed(None)))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::TemplateBuildFailed);
    // A failed first build leaves no template row behind.
    assert!(env.ctx.store().get_template("other").unwrap().is_none());
}

#[tokio::test]
async fn from_url_copies_data_without_leaking_the_password() {
    let Some(env) = TestEnv::new() else { return };
    template(
        &env,
        "a",
        "create table t(id int); insert into t values (1), (2)",
    )
    .await;
    let args = |template: &str| CloneCreateArgs {
        template: template.into(),
        name: None,
        ttl_secs: 7200,
        durable: false,
        parent_pid: None,
        fresh: false,
        protect: false,
        env: None,
        wait_secs: None,
    };
    let src = create(&env.ctx, args("a")).await.unwrap();
    let password = src
        .url
        .strip_prefix("postgres://postgres:")
        .and_then(|r| r.split_once('@'))
        .map(|(pw, _)| pw.to_string())
        .unwrap();
    assert_eq!(password.len(), 32, "{password}");

    let b = templates::init(
        &env.ctx,
        template_args(
            "b",
            TemplateSource::Url {
                url: src.url.clone(),
            },
        ),
    )
    .await
    .unwrap();
    assert_eq!(b.database, "app");
    let copy = create(&env.ctx, args("b")).await.unwrap();
    assert_eq!(psql_url(&env, &copy.url, "select count(*) from t"), "2");

    let listed = serde_json::to_string(&templates::list(&env.ctx).unwrap()).unwrap();
    assert!(!listed.contains(&password), "{listed}");
    assert!(
        listed.contains("postgres://postgres:***@127.0.0.1"),
        "{listed}"
    );
    let log = std::fs::read_to_string(env.ctx.home.logs_dir().join("template-b.log")).unwrap();
    assert!(!log.contains(&password), "{log}");
}

#[tokio::test]
async fn refresh_reuses_source_and_updates_built_at() {
    let Some(env) = TestEnv::new() else { return };
    let t1 = template(&env, "main", "create table t(id int)").await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let t2 = templates::refresh(&env.ctx, "main", None).await.unwrap();
    assert!(t2.built_at > t1.built_at);
    assert_eq!(t2.source, t1.source);
    assert_eq!(t2.database, t1.database);
    assert_eq!(t2.pg_major, t1.pg_major);
    assert_eq!((t2.current_gen, t2.generations), (2, 2));
    for dir in ["main@1", "main@2"] {
        assert!(env
            .ctx
            .home
            .templates_dir()
            .join(dir)
            .join("PG_VERSION")
            .exists());
    }
    assert!(leftovers(&env.ctx, "main").is_empty());
}

#[tokio::test]
async fn refresh_unknown_template_not_found() {
    let (_tmp, ctx) = bare_ctx();
    assert_eq!(
        templates::refresh(&ctx, "nope", None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::TemplateNotFound
    );
}

#[test]
fn list_masks_url_password() {
    assert_eq!(
        redact_url(&pg_url("u", "secret", "h:5432/db")),
        "postgres://u:***@h:5432/db"
    );
    assert_eq!(redact_url("postgres://h/db"), "postgres://h/db");
    let (_tmp, ctx) = bare_ctx();
    let url = pg_url("u", "secret", "h:5432/db");
    let (t, g) = row("main", TemplateSource::Url { url });
    ctx.store().insert_template(&t).unwrap();
    ctx.store().insert_generation(&g).unwrap();
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
    let (mut r, mut g) = row(
        "main",
        TemplateSource::Seed {
            command: "true".into(),
            cwd: None,
            path: None,
        },
    );
    g.built_at = now - 8 * 86_400;
    r.migrations = Some(mig.clone());
    g.migrations_hash = Some(hash_dir(&mig).unwrap());

    let i = info(&r, &g, 1, now);
    assert_eq!(i.age_secs, 8 * 86_400);
    assert_eq!(
        i.warnings,
        vec!["template is 8 days old; run: mitosis template refresh main".to_string()]
    );

    g.built_at = now - 60;
    assert!(info(&r, &g, 1, now).warnings.is_empty());

    std::fs::write(mig.join("sub/002.sql"), "create table b(id bigint);").unwrap();
    assert_eq!(
        info(&r, &g, 1, now).warnings,
        vec![format!(
            "migrations in {} changed since the template was built",
            mig.display()
        )]
    );

    std::fs::remove_dir_all(&mig).unwrap();
    assert!(hash_dir(&mig).is_err());
    assert!(
        info(&r, &g, 1, now).warnings.is_empty(),
        "a missing migrations folder gives no warning"
    );
}

#[tokio::test]
async fn invalid_template_name_rejected() {
    let (_tmp, ctx) = bare_ctx();
    let source = TemplateSource::Seed {
        command: "true".into(),
        cwd: None,
        path: None,
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
