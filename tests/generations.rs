mod common;

use common::*;
use mitosis::clones::{create, destroy};
use mitosis::ctx::{Config, Ctx};
use mitosis::error::ErrorCode;
use mitosis::lifecycle::reconcile;
use mitosis::paths::Home;
use mitosis::pg::newest;
use mitosis::protocol::{CloneCreateArgs, GenKind, TemplateSource};
use mitosis::state::{GenerationRow, TemplateRow};
use mitosis::templates::{self, history, rollback};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

const VALUES: &str = "select string_agg(v, ',' order by v) from t";

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
    }
}

/// A Ctx that needs no Postgres, for tests that must always run.
fn bare_ctx() -> (TempDir, Arc<Ctx>) {
    let tmp = tmp_home();
    let ctx = Ctx::open(Home::at(tmp.path().to_path_buf()), Config::default()).unwrap();
    (tmp, Arc::new(ctx))
}

fn fake_gen(ctx: &Ctx, name: &str, gen: u32, pg_major: u32) -> GenerationRow {
    GenerationRow {
        template: name.into(),
        gen,
        kind: GenKind::Full,
        dir: ctx.home.templates_dir().join(format!("{name}@{gen}")),
        pg_major,
        bin_dir: PathBuf::from("/opt/pg/bin"),
        extensions: vec![],
        migrations_hash: None,
        built_at: 1_700_000_000,
        size_bytes: 1,
        from_clone: None,
        from_gen: None,
    }
}

/// Rows only: template `main` (database `app`) with current generation 1 on Postgres 16.
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
        .insert_generation(&fake_gen(ctx, "main", 1, 16))
        .unwrap();
}

/// A seed that runs `seed.sql` from a work folder, so a refresh picks up an edited file.
fn file_seed(env: &TestEnv, values: &str) -> TemplateSource {
    let work = env.tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    write_seed(&work, values);
    let psql = newest(&env.ctx.installs()).unwrap().bin("psql");
    TemplateSource::Seed {
        command: format!(
            "'{}' \"$DATABASE_URL\" -v ON_ERROR_STOP=1 -X -f seed.sql",
            psql.display()
        ),
        cwd: Some(work),
        path: None,
    }
}

fn write_seed(work: &Path, values: &str) {
    std::fs::write(
        work.join("seed.sql"),
        format!("create table t(v text); insert into t values {values};"),
    )
    .unwrap();
}

/// Generation 1 holds `a`; generation 2 (a refresh after editing the seed) holds `a,b`.
async fn two_generations(env: &TestEnv) {
    let source = file_seed(env, "('a')");
    templates::init(&env.ctx, template_args("main", source))
        .await
        .unwrap();
    write_seed(&env.tmp.path().join("work"), "('a'), ('b')");
    let t = templates::refresh(&env.ctx, "main", None).await.unwrap();
    assert_eq!((t.current_gen, t.generations), (2, 2));
}

fn gen_numbers(ctx: &Ctx) -> Vec<(u32, bool)> {
    history(ctx, "main")
        .unwrap()
        .iter()
        .map(|g| (g.gen, g.current))
        .collect()
}

#[tokio::test]
async fn refresh_adds_generation_and_clones_follow() {
    let Some(env) = TestEnv::new() else { return };
    two_generations(&env).await;
    assert_eq!(gen_numbers(&env.ctx), vec![(1, false), (2, true)]);
    let c = create(&env.ctx, args("main")).await.unwrap();
    assert_eq!(c.generation, 2);
    assert_eq!(psql_url(&env, &c.url, VALUES), "a,b");
}

#[tokio::test]
async fn rollback_switches_source_and_pauses() {
    let Some(env) = TestEnv::new() else { return };
    two_generations(&env).await;
    let t = rollback(&env.ctx, "main", None).await.unwrap();
    assert_eq!((t.current_gen, t.paused), (1, true));
    assert!(
        env.ctx
            .store()
            .get_template("main")
            .unwrap()
            .unwrap()
            .paused
    );
    let c = create(&env.ctx, args("main")).await.unwrap();
    assert_eq!(c.generation, 1);
    assert_eq!(psql_url(&env, &c.url, VALUES), "a");
    assert_eq!(gen_numbers(&env.ctx), vec![(1, true), (2, false)]);
}

#[tokio::test]
async fn rollback_to_missing_gen_is_invalid() {
    let (_tmp, ctx) = bare_ctx();
    fake_template(&ctx);
    let e = rollback(&ctx, "main", Some(9)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    // Nothing older than the only generation either.
    let e = rollback(&ctx, "main", None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert_eq!(
        rollback(&ctx, "nope", None).await.unwrap_err().code,
        ErrorCode::TemplateNotFound
    );
}

#[tokio::test]
async fn rollback_refuses_missing_major() {
    let (_tmp, ctx) = bare_ctx();
    fake_template(&ctx);
    let gen = ctx.store().alloc_gen("main").unwrap();
    ctx.store()
        .insert_generation(&fake_gen(&ctx, "main", gen, 99))
        .unwrap();
    let e = rollback(&ctx, "main", Some(gen)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::NoMatchingPostgres);
    let t = ctx.store().get_template("main").unwrap().unwrap();
    assert_eq!((t.current_gen, t.paused), (1, false));
}

#[tokio::test]
async fn prune_keeps_newest_and_current() {
    let Some(env) = TestEnv::new() else { return };
    template(
        &env,
        "main",
        "create table t(v text); insert into t values ('a')",
    )
    .await;
    {
        let store = env.ctx.store();
        let mut t = store.get_template("main").unwrap().unwrap();
        t.keep = 2;
        store.update_template_settings(&t).unwrap();
    }
    let early = create(&env.ctx, args("main")).await.unwrap();
    assert_eq!(early.generation, 1);
    for _ in 0..3 {
        templates::refresh(&env.ctx, "main", None).await.unwrap();
    }
    assert_eq!(gen_numbers(&env.ctx), vec![(3, false), (4, true)]);
    let t = env.ctx.home.templates_dir();
    for gone in ["main@1", "main@2"] {
        assert!(!t.join(gone).exists(), "{gone} still exists");
    }
    for kept in ["main@3", "main@4"] {
        assert!(t.join(kept).join("PG_VERSION").exists(), "{kept} missing");
    }
    let leftovers: Vec<String> = std::fs::read_dir(&t)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|f| f.starts_with(".main."))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    // A clone is its own copy: pruning its generation does not touch it.
    assert_eq!(psql_url(&env, &early.url, VALUES), "a");
}

#[tokio::test]
async fn init_with_other_database_is_refused() {
    let (_tmp, ctx) = bare_ctx();
    fake_template(&ctx);
    let mut a = template_args(
        "main",
        TemplateSource::Seed {
            command: "true".into(),
            cwd: None,
            path: None,
        },
    );
    a.database = Some("other".into());
    let e = templates::init(&ctx, a).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e.message.contains("app"), "{}", e.message);
    let url = TemplateSource::Url {
        url: "postgres://h/shop".into(),
    };
    let e = templates::init(&ctx, template_args("main", url))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert_eq!(ctx.store().list_generations("main").unwrap().len(), 1);
}

/// The v0.1 schema, verbatim.
const V01_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS templates (
    name TEXT PRIMARY KEY,
    dir TEXT NOT NULL,
    pg_major INTEGER NOT NULL,
    bin_dir TEXT NOT NULL,
    extensions TEXT NOT NULL,
    database TEXT NOT NULL,
    source TEXT NOT NULL,
    migrations TEXT,
    migrations_hash TEXT,
    max_age_secs INTEGER NOT NULL,
    built_at INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS clones (
    id TEXT PRIMARY KEY,
    name TEXT,
    template TEXT NOT NULL,
    dir TEXT NOT NULL,
    port INTEGER NOT NULL,
    socket_dir TEXT NOT NULL,
    password TEXT NOT NULL,
    database TEXT NOT NULL,
    pg_pid INTEGER,
    pg_start_time INTEGER,
    parent_pid INTEGER,
    parent_start_time INTEGER,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    durable INTEGER NOT NULL,
    status TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS clones_name_uniq ON clones(name) WHERE name IS NOT NULL;
";

#[tokio::test]
async fn v01_store_is_migrated() {
    let Some(env) = TestEnv::new() else { return };
    template(
        &env,
        "main",
        "create table t(v text); insert into t values ('a')",
    )
    .await;
    let (_, g) = env.ctx.store().current("main").unwrap().unwrap();

    // A v0.1 home: the template folder at templates/main and a v0.1 state.db describing it.
    let old = tmp_home();
    let home = Home::at(old.path().to_path_buf());
    std::fs::create_dir(home.templates_dir()).unwrap();
    let dir = home.templates_dir().join("main");
    std::fs::rename(&g.dir, &dir).unwrap();
    {
        let conn = rusqlite::Connection::open(home.state_db()).unwrap();
        conn.execute_batch(V01_SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO templates VALUES (?1, ?2, ?3, ?4, ?5, 'app', ?6, NULL, NULL, 604800, ?7, ?8)",
            rusqlite::params![
                "main",
                dir.to_string_lossy(),
                g.pg_major,
                g.bin_dir.to_string_lossy(),
                serde_json::to_string(&g.extensions).unwrap(),
                serde_json::to_string(&TemplateSource::Seed {
                    command: "true".into(),
                    cwd: None,
                    path: None
                })
                .unwrap(),
                g.built_at,
                g.size_bytes as i64,
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO clones VALUES ('cold001', NULL, 'main', '/x', 1, '/x', 'pw', 'app', \
             NULL, NULL, NULL, NULL, 1, 2, 0, 'ready')",
            [],
        )
        .unwrap();
    }

    let ctx = Arc::new(Ctx::open(home, env.ctx.config()).unwrap());
    {
        let store = ctx.store();
        assert_eq!(store.list_templates().unwrap().len(), 1);
        let gens = store.list_generations("main").unwrap();
        assert_eq!(gens.len(), 1);
        let (t, cur) = store.current("main").unwrap().unwrap();
        assert_eq!((t.current_gen, cur.gen, cur.kind), (1, 1, GenKind::Full));
        assert_eq!(cur.dir, dir, "the folder is not moved");
        assert_eq!(store.get_clone("cold001").unwrap().unwrap().generation, 1);
        store.delete_clone("cold001").unwrap();
    }
    assert!(dir.join("PG_VERSION").exists());
    let c = create(&ctx, args("main")).await.unwrap();
    assert_eq!(c.generation, 1);
    assert_eq!(psql_url(&env, &c.url, VALUES), "a");
    destroy(&ctx, &c.id).await.unwrap();
}

#[tokio::test]
async fn stale_generation_folder_is_gc_orphan() {
    let (_tmp, ctx) = bare_ctx();
    let t = ctx.home.templates_dir();
    let stale = t.join("main@7");
    std::fs::create_dir(&stale).unwrap();
    std::fs::write(stale.join("PG_VERSION"), "16").unwrap();
    let notes = t.join("notes");
    std::fs::create_dir(&notes).unwrap();
    // A generation with a row is not an orphan.
    fake_template(&ctx);
    let live = t.join("main@1");
    std::fs::create_dir(&live).unwrap();

    let report = reconcile(ctx.clone(), now()).await.unwrap();
    assert_eq!(report.orphan_folders_removed, vec![stale.clone()]);
    assert!(!stale.exists());
    assert!(notes.is_dir() && live.is_dir());
    assert!(report.failed.is_empty(), "{report:?}");
}
