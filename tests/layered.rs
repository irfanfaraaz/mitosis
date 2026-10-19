mod common;

use common::*;
use mitosis::clones::create;
use mitosis::ctx::{Config, Ctx};
use mitosis::error::ErrorCode;
use mitosis::paths::Home;
use mitosis::pg::newest;
use mitosis::protocol::{
    CloneCreateArgs, GenKind, MigrateCmd, TemplateSetArgs, TemplateSettings, TemplateSource,
};
use mitosis::state::{GenerationRow, TemplateRow};
use mitosis::templates::{self, history, layered};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

/// A Ctx that needs no Postgres, for tests that must always run.
fn bare_ctx() -> (TempDir, Arc<Ctx>) {
    let tmp = tmp_home();
    let ctx = Ctx::open(Home::at(tmp.path().to_path_buf()), Config::default()).unwrap();
    (tmp, Arc::new(ctx))
}

/// Rows only: template `main` with current generation 1.
fn fake_template(ctx: &Ctx) {
    let store = ctx.store();
    let mut t = TemplateRow::new(
        "main",
        "app",
        TemplateSource::Seed {
            command: "true".into(),
            cwd: None,
            path: None,
        },
        None,
        7 * 86_400,
    );
    t.current_gen = 1;
    t.next_gen = 2;
    store.insert_template(&t).unwrap();
    store
        .insert_generation(&GenerationRow {
            template: "main".into(),
            gen: 1,
            kind: GenKind::Full,
            dir: ctx.home.templates_dir().join("main@1"),
            pg_major: 16,
            bin_dir: PathBuf::from("/opt/pg/bin"),
            extensions: vec![],
            migrations_hash: None,
            built_at: 1_700_000_000,
            size_bytes: 1,
            from_clone: None,
            from_gen: None,
        })
        .unwrap();
}

fn set_args(settings: TemplateSettings) -> TemplateSetArgs {
    TemplateSetArgs {
        name: "main".into(),
        max_age_secs: None,
        settings,
        resume: false,
    }
}

fn migrate(command: &str) -> MigrateCmd {
    MigrateCmd {
        command: command.into(),
        cwd: std::env::temp_dir(),
        path: None,
    }
}

/// A migrate command that runs `sql` with the full path of the newest `psql`.
fn psql_migrate(env: &TestEnv, sql: &str) -> MigrateCmd {
    let psql = newest(&env.ctx.installs()).unwrap().bin("psql");
    migrate(&format!(
        "'{}' \"$DATABASE_URL\" -v ON_ERROR_STOP=1 -X -c \"{sql}\"",
        psql.display()
    ))
}

fn set_migrate(ctx: &Ctx, m: MigrateCmd) {
    templates::set(
        ctx,
        set_args(TemplateSettings {
            migrate: Some(m),
            ..Default::default()
        }),
    )
    .unwrap();
}

fn clone_args() -> CloneCreateArgs {
    CloneCreateArgs {
        template: "main".into(),
        name: None,
        ttl_secs: 7200,
        durable: false,
        parent_pid: None,
        fresh: false,
        protect: false,
        env: None,
    }
}

fn side_folders(ctx: &Ctx) -> Vec<String> {
    std::fs::read_dir(ctx.home.templates_dir())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".main.building-"))
        .collect()
}

#[tokio::test]
async fn layered_runs_only_migrate_and_keeps_data() {
    let Some(env) = TestEnv::new() else { return };
    template(
        &env,
        "main",
        "create table t(v int); insert into t values (1)",
    )
    .await;
    set_migrate(
        &env.ctx,
        psql_migrate(&env, "alter table t add column c int"),
    );
    let t = layered(&env.ctx, "main", None).await.unwrap();
    assert_eq!(t.current_gen, 2);
    let g = history(&env.ctx, "main").unwrap();
    let g2 = g.iter().find(|g| g.gen == 2).unwrap();
    assert_eq!(
        (g2.kind, g2.from_gen, g2.current),
        (GenKind::Layered, Some(1), true)
    );
    assert!(side_folders(&env.ctx).is_empty());

    let c = create(&env.ctx, clone_args()).await.unwrap();
    assert_eq!(c.generation, 2);
    assert_eq!(psql_url(&env, &c.url, "select v from t"), "1");
    assert_eq!(
        psql_url(
            &env,
            &c.url,
            "select count(*) from information_schema.columns \
             where table_name = 't' and column_name = 'c'"
        ),
        "1"
    );
}

#[tokio::test]
async fn layered_without_migrate_is_invalid() {
    let (_tmp, ctx) = bare_ctx();
    fake_template(&ctx);
    let e = layered(&ctx, "main", None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e.hint.as_deref().unwrap().contains("--migrate"), "{e:?}");
    // No generation number was used up.
    assert_eq!(
        ctx.store().get_template("main").unwrap().unwrap().next_gen,
        2
    );
}

#[tokio::test]
async fn failed_migrate_keeps_current() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(v int)").await;
    set_migrate(&env.ctx, migrate("exit 3"));
    let e = layered(&env.ctx, "main", None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::TemplateBuildFailed);
    assert!(e.message.contains("migrate"), "{}", e.message);
    let store = env.ctx.store();
    assert_eq!(store.current("main").unwrap().unwrap().1.gen, 1);
    assert!(store.get_generation("main", 2).unwrap().is_none());
    drop(store);
    assert!(side_folders(&env.ctx).is_empty());
}

#[tokio::test]
async fn auto_migrate_requires_migrations_and_migrate() {
    let (tmp, ctx) = bare_ctx();
    fake_template(&ctx);
    let auto_on = || TemplateSettings {
        auto_migrate: Some(true),
        ..Default::default()
    };
    let invalid = |s: TemplateSettings| {
        let e = templates::set(&ctx, set_args(s)).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidRequest);
        e.message
    };
    assert!(invalid(auto_on()).contains("--migrations"));

    let folder = tmp.path().join("migrations");
    std::fs::create_dir(&folder).unwrap();
    {
        let store = ctx.store();
        let mut t = store.get_template("main").unwrap().unwrap();
        t.migrations = Some(folder);
        store.update_template_settings(&t).unwrap();
    }
    assert!(invalid(auto_on()).contains("--migrate"));
    // Nothing was stored by the refused requests.
    assert!(
        !ctx.store()
            .get_template("main")
            .unwrap()
            .unwrap()
            .auto_migrate
    );

    let t = templates::set(
        &ctx,
        set_args(TemplateSettings {
            migrate: Some(migrate("make migrate")),
            ..auto_on()
        }),
    )
    .unwrap();
    assert!(t.auto_migrate);
    assert_eq!(t.migrate.as_deref(), Some("make migrate"));
    // Turning the command off while auto-migrate is on is refused too.
    assert!(invalid(TemplateSettings {
        migrate_off: true,
        ..Default::default()
    })
    .contains("--migrate"));

    // Other checks: keep >= 1, every >= 1m, a value and its off flag are exclusive.
    for s in [
        TemplateSettings {
            keep: Some(0),
            ..Default::default()
        },
        TemplateSettings {
            every_secs: Some(30),
            ..Default::default()
        },
        TemplateSettings {
            every_secs: Some(3600),
            every_off: true,
            ..Default::default()
        },
        TemplateSettings {
            migrate: Some(migrate("x")),
            migrate_off: true,
            ..Default::default()
        },
    ] {
        invalid(s);
    }
}

#[tokio::test]
async fn manual_refresh_clears_pause() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(v int)").await;
    let pause = || {
        let store = env.ctx.store();
        let mut t = store.get_template("main").unwrap().unwrap();
        t.paused = true;
        t.last_error = Some("boom".into());
        store.update_template_settings(&t).unwrap();
    };

    pause();
    let t = templates::refresh(&env.ctx, "main", None).await.unwrap();
    assert_eq!((t.current_gen, t.paused, t.last_error), (2, false, None));

    pause();
    set_migrate(&env.ctx, migrate("true"));
    let t = layered(&env.ctx, "main", None).await.unwrap();
    assert_eq!((t.current_gen, t.paused, t.last_error), (3, false, None));

    // `set --resume` clears the pause without a build.
    pause();
    let t = templates::set(
        &env.ctx,
        TemplateSetArgs {
            resume: true,
            ..set_args(TemplateSettings::default())
        },
    )
    .unwrap();
    assert!(!t.paused);
}
