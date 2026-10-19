mod common;

use common::*;
use mitosis::clones::{create, list};
use mitosis::ctx::{Config, Ctx};
use mitosis::error::ErrorCode;
use mitosis::paths::Home;
use mitosis::promote::promote;
use mitosis::protocol::{
    CloneCreateArgs, GenKind, RefreshMode, TemplateSetArgs, TemplateSettings, TemplateSource,
};
use mitosis::state::{GenerationRow, TemplateRow};
use mitosis::templates::{self, history, refresh};
use std::path::PathBuf;
use std::sync::Arc;

fn clone_args(template: &str) -> CloneCreateArgs {
    CloneCreateArgs {
        template: template.into(),
        name: None,
        ttl_secs: 7200,
        durable: false,
        parent_pid: None,
        fresh: false,
        protect: false,
    }
}

#[tokio::test]
async fn promote_creates_generation_and_clone_keeps_url() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(v text)").await;
    let c = create(&env.ctx, clone_args("main")).await.unwrap();
    psql_url(&env, &c.url, "insert into t values ('p')");

    let t = promote(&env.ctx, &c.id, None).await.unwrap();
    assert_eq!(t.current_gen, 2);
    let g = history(&env.ctx, "main").unwrap();
    let g2 = g.iter().find(|g| g.gen == 2).unwrap();
    assert_eq!(
        (g2.kind, g2.current, g2.from_clone.as_deref(), g2.from_gen),
        (GenKind::Promoted, true, Some(c.id.as_str()), Some(1))
    );

    // The same URL connects again and still sees the row.
    assert_eq!(psql_url(&env, &c.url, "select v from t"), "p");
    let again = list(&env.ctx).unwrap().into_iter().find(|i| i.id == c.id);
    assert_eq!(again.unwrap().url, c.url);

    // A new clone starts from the promoted generation.
    let n = create(&env.ctx, clone_args("main")).await.unwrap();
    assert_eq!(n.generation, 2);
    assert_eq!(psql_url(&env, &n.url, "select v from t"), "p");
}

#[tokio::test]
async fn promote_to_new_template() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(v text)").await;
    let c = create(&env.ctx, clone_args("main")).await.unwrap();
    psql_url(&env, &c.url, "insert into t values ('s')");

    let t = promote(&env.ctx, &c.id, Some("snap")).await.unwrap();
    assert_eq!((t.name.as_str(), t.current_gen), ("snap", 1));
    let listed = templates::list(&env.ctx).unwrap();
    let snap = listed.iter().find(|t| t.name == "snap").unwrap();
    assert_eq!(snap.current_gen, 1);
    assert!(matches!(snap.source, TemplateSource::Promoted { .. }));
    // The template it came from is untouched.
    assert_eq!(
        listed
            .iter()
            .find(|t| t.name == "main")
            .unwrap()
            .current_gen,
        1
    );

    let e = refresh(&env.ctx, "snap", None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert_eq!(e.hint.as_deref(), Some("use --layered or promote again"));

    let n = create(&env.ctx, clone_args("snap")).await.unwrap();
    assert_eq!(psql_url(&env, &n.url, "select v from t"), "s");
}

#[tokio::test]
async fn promote_rejects_mismatched_database() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(v text)").await;
    let mut other = template_args("other", seed("create table u(v text)"));
    other.database = Some("other".into());
    templates::init(&env.ctx, other).await.unwrap();

    let c = create(&env.ctx, clone_args("main")).await.unwrap();
    let e = promote(&env.ctx, &c.id, Some("other")).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e.message.contains("database"), "{}", e.message);
    // Nothing changed, and the clone still runs.
    assert_eq!(history(&env.ctx, "other").unwrap().len(), 1);
    assert_eq!(psql_url(&env, &c.url, "select count(*) from t"), "0");
}

#[tokio::test]
async fn promoted_generation_has_no_postmaster_pid() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(v text)").await;
    let c = create(&env.ctx, clone_args("main")).await.unwrap();
    promote(&env.ctx, &c.id, None).await.unwrap();

    let gen = env.ctx.store().get_generation("main", 2).unwrap().unwrap();
    assert!(gen.dir.join("PG_VERSION").is_file());
    assert!(!gen.dir.join("postmaster.pid").exists());
    assert!(!gen.dir.join("postmaster.opts").exists());
    // The clone itself runs again and has its own pid file.
    let row = env.ctx.store().get_clone(&c.id).unwrap().unwrap();
    assert!(row.dir.join("postmaster.pid").is_file());
}

/// Rows only: template `snap`, promoted from a clone, with current generation 1.
#[tokio::test]
async fn promoted_template_refuses_a_full_schedule() {
    let tmp = tmp_home();
    let ctx = Arc::new(Ctx::open(Home::at(tmp.path().to_path_buf()), Config::default()).unwrap());
    {
        let store = ctx.store();
        let mut t = TemplateRow::new(
            "snap",
            "app",
            TemplateSource::Promoted {
                clone_id: "c0a1b2".into(),
            },
            None,
            7 * 86_400,
        );
        t.current_gen = 1;
        t.next_gen = 2;
        store.insert_template(&t).unwrap();
        store
            .insert_generation(&GenerationRow {
                template: "snap".into(),
                gen: 1,
                kind: GenKind::Promoted,
                dir: ctx.home.templates_dir().join("snap@1"),
                pg_major: 16,
                bin_dir: PathBuf::from("/opt/pg/bin"),
                extensions: vec![],
                migrations_hash: None,
                built_at: 1_700_000_000,
                size_bytes: 1,
                from_clone: Some("c0a1b2".into()),
                from_gen: None,
            })
            .unwrap();
    }
    let set = |every_mode: Option<RefreshMode>| {
        templates::set(
            &ctx,
            TemplateSetArgs {
                name: "snap".into(),
                max_age_secs: None,
                settings: TemplateSettings {
                    every_secs: Some(3600),
                    every_mode,
                    ..Default::default()
                },
                resume: false,
            },
        )
    };
    // The default mode is full.
    let e = set(None).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert_eq!(e.hint.as_deref(), Some("use --mode layered"));
    assert!(set(Some(RefreshMode::Full)).is_err());
    assert!(ctx
        .store()
        .get_template("snap")
        .unwrap()
        .unwrap()
        .every_secs
        .is_none());

    let t = set(Some(RefreshMode::Layered)).unwrap();
    assert_eq!(
        (t.every_secs, t.every_mode),
        (Some(3600), RefreshMode::Layered)
    );
}
