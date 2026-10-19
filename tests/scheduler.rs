mod common;

use common::*;
use mitosis::clones::create;
use mitosis::ctx::{Config, Ctx};
use mitosis::lifecycle::reconcile;
use mitosis::paths::Home;
use mitosis::protocol::{
    CloneCreateArgs, GenKind, MigrateCmd, OnStale, TemplateInitArgs, TemplateSetArgs,
    TemplateSettings, TemplateSource,
};
use mitosis::scheduler;
use mitosis::state::{GenerationRow, TemplateRow};
use mitosis::templates::{self, layered, rollback};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// The scheduler on `ctx`, ticking every 200 ms; it stops when the returned sender drops.
fn run_scheduler(ctx: &Arc<Ctx>) -> watch::Sender<bool> {
    ctx.set_config(Config {
        scheduler_tick: Duration::from_millis(200),
        ..ctx.config()
    });
    let (tx, rx) = watch::channel(false);
    scheduler::run(ctx.clone(), rx);
    tx
}

fn row(ctx: &Ctx) -> TemplateRow {
    ctx.store().get_template("main").unwrap().unwrap()
}

/// Polls `f` every 100 ms until it is true; panics with `what` after `secs` seconds.
async fn wait_for(secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn set(ctx: &Ctx, max_age_secs: Option<u64>, settings: TemplateSettings) {
    templates::set(
        ctx,
        TemplateSetArgs {
            name: "main".into(),
            max_age_secs,
            settings,
            resume: false,
        },
    )
    .unwrap();
}

fn every_minute() -> TemplateSettings {
    TemplateSettings {
        every_secs: Some(60),
        ..Default::default()
    }
}

/// Moves the build time of every generation of `main` `secs` seconds into the past (rows only).
fn backdate(ctx: &Ctx, secs: i64) {
    let store = ctx.store();
    for g in store.list_generations("main").unwrap() {
        store.delete_generation("main", g.gen).unwrap();
        store
            .insert_generation(&GenerationRow {
                built_at: g.built_at - secs,
                ..g
            })
            .unwrap();
    }
}

fn migrate(command: &str, cwd: &Path) -> MigrateCmd {
    MigrateCmd {
        command: command.into(),
        cwd: cwd.to_path_buf(),
        path: None,
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
async fn scheduled_refresh_runs_when_overdue() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(v int)").await;
    set(&env.ctx, None, every_minute());
    backdate(&env.ctx, 120);
    let _stop = run_scheduler(&env.ctx);
    wait_for(10, "generation 2", || row(&env.ctx).current_gen == 2).await;
    // The background build records its attempt before it releases the template.
    wait_for(10, "the attempt", || {
        row(&env.ctx).last_attempt_at.is_some()
    })
    .await;
    let t = row(&env.ctx);
    assert_eq!((t.paused, t.last_error), (false, None));
}

#[tokio::test]
async fn rollback_is_not_undone_by_schedule() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(v int)").await;
    templates::refresh(&env.ctx, "main", None).await.unwrap();
    set(&env.ctx, None, every_minute());
    backdate(&env.ctx, 120);
    let t = rollback(&env.ctx, "main", None).await.unwrap();
    assert_eq!((t.current_gen, t.paused), (1, true));
    let _stop = run_scheduler(&env.ctx);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let t = row(&env.ctx);
    assert_eq!((t.current_gen, t.next_gen, t.paused), (1, 3, true));
    assert_eq!(t.last_attempt_at, None);
}

#[tokio::test]
async fn build_finishing_after_rollback_is_not_current() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(v int)").await;
    templates::refresh(&env.ctx, "main", None).await.unwrap();
    set(
        &env.ctx,
        None,
        TemplateSettings {
            migrate: Some(migrate("sleep 2 && true", env.tmp.path())),
            ..Default::default()
        },
    );
    let ctx = env.ctx.clone();
    let build = tokio::spawn(async move { layered(&ctx, "main", None).await });
    // The build took generation 3 when it started; it still has the slow migrate ahead.
    wait_for(10, "the build to start", || row(&env.ctx).next_gen == 4).await;
    rollback(&env.ctx, "main", Some(1)).await.unwrap();
    let t = build.await.unwrap().unwrap();
    assert_eq!(t.current_gen, 1);
    let store = env.ctx.store();
    let g3 = store.get_generation("main", 3).unwrap().unwrap();
    assert_eq!((g3.kind, g3.from_gen), (GenKind::Layered, Some(2)));
    let t = store.get_template("main").unwrap().unwrap();
    assert_eq!((t.current_gen, t.paused), (1, true), "the rollback sticks");
}

#[tokio::test]
async fn stale_clone_is_served_and_refresh_starts() {
    let Some(env) = TestEnv::new() else { return };
    template(
        &env,
        "main",
        "create table t(v int); insert into t values (1)",
    )
    .await;
    set(
        &env.ctx,
        Some(1),
        TemplateSettings {
            on_stale: Some(OnStale::Refresh),
            ..Default::default()
        },
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let c = create(
        &env.ctx,
        CloneCreateArgs {
            template: "main".into(),
            name: None,
            ttl_secs: 7200,
            durable: false,
            parent_pid: None,
        },
    )
    .await
    .unwrap();
    assert!(c.stale && c.refreshing, "{c:?}");
    assert_eq!(c.generation, 1);
    assert_eq!(psql_url(&env, &c.url, "select v from t"), "1");
    wait_for(30, "generation 2", || row(&env.ctx).current_gen == 2).await;
    wait_for(10, "the attempt", || {
        row(&env.ctx).last_attempt_at.is_some()
    })
    .await;
    assert_eq!(row(&env.ctx).last_error, None);
}

#[tokio::test]
async fn failed_background_build_sets_last_error_and_keeps_current() {
    let Some(env) = TestEnv::new() else { return };
    // The seed fails once the marker file exists.
    let marker = env.tmp.path().join("fail-seed");
    let TemplateSource::Seed { command, .. } = seed("create table t(v int)") else {
        unreachable!()
    };
    let source = TemplateSource::Seed {
        command: format!("test ! -e '{}' && {command}", marker.display()),
        cwd: None,
        path: None,
    };
    templates::init(&env.ctx, template_args("main", source))
        .await
        .unwrap();
    std::fs::write(&marker, "").unwrap();
    set(&env.ctx, None, every_minute());
    backdate(&env.ctx, 120);
    let _stop = run_scheduler(&env.ctx);
    wait_for(30, "last_error", || row(&env.ctx).last_error.is_some()).await;

    let listed = templates::list(&env.ctx).unwrap();
    let err = listed[0].last_error.clone().unwrap();
    assert!(err.contains("seed"), "{err}");
    let t = row(&env.ctx);
    assert_eq!((t.current_gen, t.next_gen, t.paused), (1, 3, false));
    assert!(env.ctx.store().get_generation("main", 2).unwrap().is_none());
    assert!(side_folders(&env.ctx).is_empty());

    // No retry before the next interval: five more ticks start nothing.
    let attempt = t.last_attempt_at;
    assert!(attempt.is_some());
    tokio::time::sleep(Duration::from_secs(1)).await;
    let t = row(&env.ctx);
    assert_eq!((t.next_gen, t.last_attempt_at), (3, attempt));
}

/// Review Focus 3, simulated in-process (ruling R6): a build that died with the daemon leaves an
/// allocated generation number and a side folder, but no row.
#[tokio::test]
async fn crashed_build_leaves_no_generation() {
    let tmp = tmp_home();
    let home = || Home::at(tmp.path().to_path_buf());
    let ctx = Arc::new(Ctx::open(home(), Config::default()).unwrap());
    let gen1 = ctx.home.templates_dir().join("main@1");
    std::fs::create_dir(&gen1).unwrap();
    {
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
                dir: gen1.clone(),
                pg_major: 16,
                bin_dir: PathBuf::from("/opt/pg/bin"),
                extensions: vec![],
                migrations_hash: None,
                built_at: now(),
                size_bytes: 1,
                from_clone: None,
                from_gen: None,
            })
            .unwrap();
        assert_eq!(store.alloc_gen("main").unwrap(), 2);
    }
    let side = ctx
        .home
        .templates_dir()
        .join(format!(".main.building-{}", now()));
    std::fs::create_dir(&side).unwrap();
    std::fs::write(side.join("PG_VERSION"), "16\n").unwrap();
    drop(ctx); // the daemon dies mid-build

    let ctx = Arc::new(Ctx::open(home(), Config::default()).unwrap());
    let report = reconcile(ctx.clone(), now()).await.unwrap();
    assert!(report.orphan_folders_removed.contains(&side), "{report:?}");
    assert!(side_folders(&ctx).is_empty());
    let store = ctx.store();
    assert!(store.get_generation("main", 2).unwrap().is_none());
    let (t, cur) = store.current("main").unwrap().unwrap();
    assert_eq!((t.current_gen, cur.gen, t.next_gen), (1, 1, 3));
    assert!(gen1.is_dir());
}

#[tokio::test]
async fn auto_migrate_fires_on_folder_change() {
    let Some(env) = TestEnv::new() else { return };
    let folder = env.tmp.path().join("migrations");
    std::fs::create_dir(&folder).unwrap();
    std::fs::write(folder.join("001.sql"), "create table t(v int);").unwrap();
    templates::init(
        &env.ctx,
        TemplateInitArgs {
            migrations: Some(folder.clone()),
            ..template_args("main", seed("create table t(v int)"))
        },
    )
    .await
    .unwrap();
    set(
        &env.ctx,
        None,
        TemplateSettings {
            migrate: Some(migrate("true", &folder)),
            auto_migrate: Some(true),
            ..Default::default()
        },
    );
    let _stop = run_scheduler(&env.ctx);
    // The folder matches the template: nothing to do.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(row(&env.ctx).next_gen, 2);

    std::fs::write(folder.join("002.sql"), "alter table t add column c int;").unwrap();
    wait_for(30, "generation 2", || row(&env.ctx).current_gen == 2).await;
    wait_for(10, "the attempt", || {
        row(&env.ctx).last_attempt_at.is_some()
    })
    .await;
    let store = env.ctx.store();
    let g2 = store.get_generation("main", 2).unwrap().unwrap();
    assert_eq!(g2.kind, GenKind::Layered);
    assert_eq!(
        g2.migrations_hash,
        Some(templates::hash_dir(&folder).unwrap())
    );
    drop(store);
    // The new generation matches the folder, so no further build starts.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(row(&env.ctx).next_gen, 3);
}
