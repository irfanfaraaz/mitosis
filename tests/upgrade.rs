mod common;

use common::*;
use mitosis::clones::create;
use mitosis::ctx::{Config, Ctx};
use mitosis::error::ErrorCode;
use mitosis::paths::Home;
use mitosis::pg::discover;
use mitosis::protocol::{CloneCreateArgs, GenKind, TemplateSource};
use mitosis::state::{GenerationRow, TemplateRow};
use mitosis::templates::{self, history, rollback};
use mitosis::upgrade::upgrade;
use std::path::PathBuf;
use std::sync::Arc;

/// Built from JSON so fields added to the request later (all defaulted) need no change here.
fn clone_args(template: &str) -> CloneCreateArgs {
    serde_json::from_value(serde_json::json!({
        "template": template,
        "ttl_secs": 7200,
        "durable": false,
    }))
    .unwrap()
}

/// `(old, new)`: the two newest installed majors, or `None` (with a skip message naming a major
/// to install) when fewer than two are installed.
fn two_majors() -> Option<(u32, u32)> {
    let mut majors: Vec<u32> = discover().into_iter().map(|i| i.major).collect();
    majors.dedup(); // newest first
    match majors[..] {
        [] => {
            println!("skipping: no Postgres installed");
            None
        }
        [only] => {
            let other = if only > 13 { only - 1 } else { only + 1 };
            println!(
                "skipping: needs two Postgres majors, only {only} is installed; \
                 install a second with: brew install postgresql@{other}"
            );
            None
        }
        [new, old, ..] => Some((old, new)),
    }
}

/// Template `name` built on Postgres `major` from a seed.
async fn template_on(env: &TestEnv, name: &str, major: u32, sql: &str) {
    let mut args = template_args(name, seed(sql));
    args.pg_major = Some(major);
    let t = templates::init(&env.ctx, args)
        .await
        .expect("build template");
    assert_eq!(t.pg_major, major);
}

/// No `.<name>.building-*` side folder and no `upgrade-*` temp folder is left.
fn no_side_folders(ctx: &Ctx) {
    for dir in [ctx.home.templates_dir(), ctx.home.logs_dir()] {
        for e in std::fs::read_dir(&dir).unwrap().flatten() {
            let f = e.file_name().to_string_lossy().into_owned();
            assert!(
                !f.contains(".building-") && !f.starts_with("upgrade-"),
                "left behind: {}",
                e.path().display()
            );
        }
    }
}

#[tokio::test]
async fn upgrade_in_place_adds_generation_on_new_major() {
    let Some((old, new)) = two_majors() else {
        return;
    };
    let Some(env) = TestEnv::new() else { return };
    template_on(
        &env,
        "main",
        old,
        "create table t(v text); insert into t values ('u')",
    )
    .await;

    let t = upgrade(&env.ctx, "main", new, None).await.unwrap();
    assert_eq!(
        (t.name.as_str(), t.current_gen, t.pg_major),
        ("main", 2, new)
    );
    let g = history(&env.ctx, "main").unwrap();
    let g2 = g.iter().find(|g| g.gen == 2).unwrap();
    assert_eq!(
        (g2.kind, g2.current, g2.from_gen, g2.pg_major),
        (GenKind::Upgraded, true, Some(1), new)
    );
    no_side_folders(&env.ctx);

    let c = create(&env.ctx, clone_args("main")).await.unwrap();
    assert_eq!(c.generation, 2);
    let v = psql_url(&env, &c.url, "show server_version_num");
    assert!(v.starts_with(&new.to_string()), "server_version_num {v}");
    assert_eq!(psql_url(&env, &c.url, "select v from t"), "u");

    // Rollback returns to the old major.
    let t = rollback(&env.ctx, "main", Some(1)).await.unwrap();
    assert_eq!((t.current_gen, t.pg_major), (1, old));
    let c = create(&env.ctx, clone_args("main")).await.unwrap();
    let v = psql_url(&env, &c.url, "show server_version_num");
    assert!(v.starts_with(&old.to_string()), "server_version_num {v}");
}

#[tokio::test]
async fn upgrade_as_new_template_keeps_original() {
    let Some((old, new)) = two_majors() else {
        return;
    };
    let Some(env) = TestEnv::new() else { return };
    template_on(
        &env,
        "main",
        old,
        "create table t(v text); insert into t values ('n')",
    )
    .await;
    {
        let store = env.ctx.store();
        let mut t = store.get_template("main").unwrap().unwrap();
        t.every_secs = Some(3600);
        t.keep = 5;
        store.update_template_settings(&t).unwrap();
    }

    let t = upgrade(&env.ctx, "main", new, Some("next")).await.unwrap();
    assert_eq!(
        (t.name.as_str(), t.current_gen, t.pg_major),
        ("next", 1, new)
    );
    let row = env.ctx.store().get_template("next").unwrap().unwrap();
    assert_eq!(
        (row.database.as_str(), row.keep, row.every_secs, row.paused),
        ("app", 5, None, false)
    );
    let g = history(&env.ctx, "next").unwrap();
    assert_eq!((g.len(), g[0].kind), (1, GenKind::Upgraded));
    no_side_folders(&env.ctx);

    // The original is untouched.
    let (orig, cur) = env.ctx.store().current("main").unwrap().unwrap();
    assert_eq!((orig.current_gen, cur.pg_major), (1, old));
    assert_eq!(orig.every_secs, Some(3600));

    let c = create(&env.ctx, clone_args("next")).await.unwrap();
    let v = psql_url(&env, &c.url, "show server_version_num");
    assert!(v.starts_with(&new.to_string()), "server_version_num {v}");
    assert_eq!(psql_url(&env, &c.url, "select v from t"), "n");

    // An existing name is refused.
    let e = upgrade(&env.ctx, "main", new, Some("next"))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
}

/// Rows only: needs no Postgres at all.
#[tokio::test]
async fn upgrade_to_missing_major_fails_fast() {
    let tmp = tmp_home();
    let ctx = Arc::new(Ctx::open(Home::at(tmp.path().to_path_buf()), Config::default()).unwrap());
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

    let e = upgrade(&ctx, "main", 99, None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::NoMatchingPostgres);
    let e = upgrade(&ctx, "main", 99, Some("next")).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::NoMatchingPostgres);
    // Not newer than the current major.
    let e = upgrade(&ctx, "main", 16, None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);

    let t = ctx.store().get_template("main").unwrap().unwrap();
    assert_eq!((t.current_gen, t.next_gen), (1, 2));
    assert!(ctx.store().get_template("next").unwrap().is_none());
    no_side_folders(&ctx);
}
