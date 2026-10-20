mod common;

use common::*;
use mitosis::clones::create;
use mitosis::error::ErrorCode;
use mitosis::names::random_hex;
use mitosis::pg::{newest, psql};
use mitosis::protocol::{
    CloneInfo, GenKind, MigrateCmd, RefreshMode, TemplateSetArgs, TemplateSettings,
};
use mitosis::scheduler;
use mitosis::state::GenerationRow;
use mitosis::templates::{self, layered, refresh};
use std::path::{Path, PathBuf};

const USERS: &str = "create table users(id int primary key, email text not null, \
     full_name varchar(80), phone text, ssn text, notes text, plan text, age int, alt char(40)); \
     insert into users values \
     (1, 'alice@example.com', 'Alice Smith', '555-1234', '123-45-6789', 'secret', 'pro', 30, 'alice@work.example'), \
     (2, 'bob@example.com', null, null, null, null, null, 40, null)";

const USERS_MASK: &str = r#"
[tables.users]
email = "email"
full_name = "name"
phone = "null"
ssn = "hash"
notes = "redact"
plan = "const:free"
age = "keep"
alt = "email"
"#;

fn write_mask(env: &TestEnv, text: &str) -> PathBuf {
    let p = env.tmp.path().join("mask.toml");
    std::fs::write(&p, text).unwrap();
    p
}

/// Builds template `main` from a seed running `sql`, masked with `mask`.
async fn masked(
    env: &TestEnv,
    sql: &str,
    mask: &str,
) -> mitosis::error::Result<mitosis::protocol::TemplateInfo> {
    let mut args = template_args("main", seed(sql));
    args.settings.mask = Some(write_mask(env, mask));
    templates::init(&env.ctx, args).await
}

async fn clone(env: &TestEnv) -> CloneInfo {
    // Built from JSON, so fields other tasks add (all defaulted) never break this test.
    let args = serde_json::from_value(serde_json::json!({
        "template": "main", "ttl_secs": 7200, "durable": false, "parent_pid": null
    }))
    .unwrap();
    create(&env.ctx, args).await.unwrap()
}

/// Runs `sql` in the clone over its own socket (local trust), so the router plays no part.
fn q(env: &TestEnv, c: &CloneInfo, sql: &str) -> String {
    let install = newest(&env.ctx.installs()).unwrap().clone();
    psql(&install, &c.socket_dir, c.port, "app", sql).unwrap()
}

fn side_folders(env: &TestEnv) -> Vec<String> {
    std::fs::read_dir(env.ctx.home.templates_dir())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".main.building-"))
        .collect()
}

/// True when any file under `dir` (pg_wal included) contains `needle`.
fn contains_bytes(dir: &Path, needle: &[u8]) -> bool {
    std::fs::read_dir(dir).unwrap().flatten().any(|e| {
        let p = e.path();
        if e.file_type().unwrap().is_dir() {
            contains_bytes(&p, needle)
        } else {
            let data = std::fs::read(&p).unwrap_or_default();
            data.windows(needle.len()).any(|w| w == needle)
        }
    })
}

fn psql_migrate(env: &TestEnv, sql: &str) -> MigrateCmd {
    let psql = newest(&env.ctx.installs()).unwrap().bin("psql");
    MigrateCmd {
        command: format!(
            "'{}' \"$DATABASE_URL\" -v ON_ERROR_STOP=1 -X -c \"{sql}\"",
            psql.display()
        ),
        cwd: std::env::temp_dir(),
        path: None,
    }
}

fn set(env: &TestEnv, settings: TemplateSettings) -> mitosis::error::Result<()> {
    templates::set(
        &env.ctx,
        TemplateSetArgs {
            name: "main".into(),
            max_age_secs: None,
            settings,
            resume: false,
        },
    )
    .map(drop)
}

#[tokio::test]
async fn mask_each_strategy() {
    let Some(env) = TestEnv::new() else { return };
    let t = masked(&env, USERS, USERS_MASK).await.unwrap();
    assert!(t.mask.is_some());
    assert!(t.warnings.is_empty(), "{:?}", t.warnings);
    assert!(side_folders(&env).is_empty());
    let c = clone(&env).await;
    let row = q(
        &env,
        &c,
        "select email, full_name, coalesce(phone, '<null>'), ssn, notes, plan, age \
         from users where id = 1",
    );
    let f: Vec<&str> = row.split('|').collect();
    assert!(
        f[0].starts_with("u_") && f[0].ends_with("@masked.invalid") && f[0].len() == 29,
        "{row}"
    );
    assert!(f[1].starts_with("Masked ") && f[1].len() == 15, "{row}");
    assert_eq!(f[2], "<null>");
    assert!(f[3].starts_with("mh_") && f[3].len() == 67, "{row}");
    assert_eq!(f[4], "xxxxxx");
    assert_eq!(f[5], "free");
    assert_eq!(f[6], "30");
    // NULL inputs stay NULL.
    assert_eq!(
        q(
            &env,
            &c,
            "select full_name is null, ssn is null, plan is null from users where id = 2"
        ),
        "t|t|t"
    );
    // A changed masking file warns like changed migrations.
    std::fs::write(
        env.tmp.path().join("mask.toml"),
        format!("{USERS_MASK}\n# edited\n"),
    )
    .unwrap();
    let t = templates::list(&env.ctx).unwrap().remove(0);
    assert!(
        t.warnings
            .iter()
            .any(|w| w.contains("changed since the template was built")),
        "{:?}",
        t.warnings
    );
}

#[tokio::test]
async fn mask_is_idempotent_and_consistent() {
    let Some(env) = TestEnv::new() else { return };
    masked(&env, USERS, USERS_MASK).await.unwrap();
    // `alt` is char(40): a re-run must recognise its padded masked value and leave it.
    let all = "select email, full_name, ssn, notes, plan, alt from users order by id";
    let first = q(&env, &clone(&env).await, all);
    // A second full build, from the raw source again, gives the same masked values.
    assert_eq!(
        refresh(&env.ctx, "main", None).await.unwrap().current_gen,
        2
    );
    let second = q(&env, &clone(&env).await, all);
    assert_eq!(first, second);

    // A layered build masks the row its migration adds and leaves masked rows as they were.
    set(
        &env,
        TemplateSettings {
            migrate: Some(psql_migrate(
                &env,
                "insert into users values (3, 'carol@example.com', 'Carol Jones', '1', '2', 'abc', 'pro', 50, 'c@x.example')",
            )),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        layered(&env.ctx, "main", None).await.unwrap().current_gen,
        3
    );
    let c = clone(&env).await;
    let third = q(&env, &c, &format!("{all} limit 2"));
    assert_eq!(first, third);
    let new = q(
        &env,
        &c,
        "select email, full_name, ssn, notes, plan from users where id = 3",
    );
    let f: Vec<&str> = new.split('|').collect();
    assert!(
        f[0].ends_with("@masked.invalid") && f[1].starts_with("Masked "),
        "{new}"
    );
    assert!(
        f[2].starts_with("mh_") && f[3] == "xxx" && f[4] == "free",
        "{new}"
    );
}

#[tokio::test]
async fn layered_refuses_a_generation_built_unmasked() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", USERS).await;
    set(
        &env,
        TemplateSettings {
            migrate: Some(psql_migrate(&env, "select 1")),
            mask: Some(write_mask(&env, USERS_MASK)),
            ..Default::default()
        },
    )
    .unwrap();
    let e = layered(&env.ctx, "main", None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e.hint.unwrap().contains("template refresh"));

    // Built masked, then the file changes (a newly masked column): a layered copy would keep
    // that column's raw values in its pages, so only a full build may follow.
    refresh(&env.ctx, "main", None).await.unwrap();
    layered(&env.ctx, "main", None).await.unwrap();
    write_mask(
        &env,
        &USERS_MASK.replace("age = \"keep\"", "age = \"null\""),
    );
    let e = layered(&env.ctx, "main", None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(
        e.hint.unwrap().contains("template refresh"),
        "{}",
        e.message
    );
}

#[tokio::test]
async fn scheduled_layered_refresh_after_a_mask_change_builds_full() {
    let Some(env) = TestEnv::new() else { return };
    masked(&env, USERS, USERS_MASK).await.unwrap();
    set(
        &env,
        TemplateSettings {
            migrate: Some(psql_migrate(&env, "select 1")),
            every_secs: Some(60),
            every_mode: Some(RefreshMode::Layered),
            ..Default::default()
        },
    )
    .unwrap();
    write_mask(
        &env,
        &USERS_MASK.replace("age = \"keep\"", "age = \"null\""),
    );
    // Make the schedule due: generation 1 built two minutes ago (rows only).
    {
        let store = env.ctx.store();
        let g = store.get_generation("main", 1).unwrap().unwrap();
        store.delete_generation("main", 1).unwrap();
        store
            .insert_generation(&GenerationRow {
                built_at: g.built_at - 120,
                ..g
            })
            .unwrap();
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    scheduler::tick(&env.ctx, now).await;
    // The background build records its attempt as it finishes.
    for _ in 0..600 {
        let t = env.ctx.store().get_template("main").unwrap().unwrap();
        if t.last_attempt_at.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let history = templates::history(&env.ctx, "main").unwrap();
    let g2 = history.iter().find(|g| g.gen == 2).expect("generation 2");
    assert_eq!(g2.kind, GenKind::Full);
    let t = templates::list(&env.ctx).unwrap().remove(0);
    assert!(t.last_error.is_none(), "{:?}", t.last_error);
    assert!(t.warnings.is_empty(), "{:?}", t.warnings);
    let c = clone(&env).await;
    assert_eq!(q(&env, &c, "select count(age) from users"), "0");
}

#[tokio::test]
async fn mask_wrong_type_fails_naming_column() {
    let Some(env) = TestEnv::new() else { return };
    let e = masked(&env, USERS, "[tables.users]\nage = \"email\"")
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::TemplateBuildFailed);
    assert!(e.message.contains("users.age is integer"), "{}", e.message);
    // A failed first build leaves no template behind.
    assert!(env.ctx.store().get_template("main").unwrap().is_none());
}

#[tokio::test]
async fn mask_unknown_column_fails() {
    let Some(env) = TestEnv::new() else { return };
    let e = masked(&env, USERS, "[tables.users]\nnope = \"hash\"")
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::TemplateBuildFailed);
    assert!(e.message.contains("users.nope"), "{}", e.message);
    let e = masked(&env, USERS, "[tables.people]\nemail = \"hash\"")
        .await
        .unwrap_err();
    assert!(e.message.contains("people"), "{}", e.message);
}

#[tokio::test]
async fn masked_generation_has_no_raw_value() {
    let Some(env) = TestEnv::new() else { return };
    let raw = format!("rawsecret{}", random_hex(8));
    let kept = format!("keptvalue{}", random_hex(8));
    let sql = format!(
        "create table users(id int, email text, notes text, plan text); \
         insert into users values (1, '{raw}@example.com', '{raw}', '{kept}')"
    );
    masked(
        &env,
        &sql,
        "[tables.users]\nemail = \"email\"\nnotes = \"redact\"",
    )
    .await
    .unwrap();
    let (t, g) = env.ctx.store().current("main").unwrap().unwrap();
    // The search works: the kept value is found in the same files.
    assert!(contains_bytes(&g.dir, kept.as_bytes()));
    assert!(
        !contains_bytes(&g.dir, raw.as_bytes()),
        "raw value in {}",
        g.dir.display()
    );
    assert!(!contains_bytes(&g.dir, t.mask_salt.unwrap().as_bytes()));
    assert!(side_folders(&env).is_empty());
}

#[tokio::test]
async fn subset_keeps_foreign_keys_valid() {
    let Some(env) = TestEnv::new() else { return };
    let sql = "create table parents(id int primary key, keep boolean not null); \
         create table children(id int primary key, parent_id int references parents(id)); \
         create table grandchildren(id int primary key, child_id int references children(id)); \
         insert into parents values (1, true), (2, false), (3, true); \
         insert into children values (10, 1), (20, 2), (21, 2), (30, null); \
         insert into grandchildren values (100, 10), (200, 20), (210, 21)";
    masked(&env, sql, "[subset]\nparents = \"keep\"")
        .await
        .unwrap();
    let c = clone(&env).await;
    let ids = |t: &str| {
        q(
            &env,
            &c,
            &format!("select string_agg(id::text, ',' order by id) from {t}"),
        )
    };
    assert_eq!(ids("parents"), "1,3");
    // A NULL reference is no reference: row 30 stays.
    assert_eq!(ids("children"), "10,30");
    assert_eq!(ids("grandchildren"), "100");
    assert_eq!(
        q(
            &env,
            &c,
            "select count(*) from children c where c.parent_id is not null \
             and not exists (select 1 from parents p where p.id = c.parent_id)"
        ),
        "0"
    );
    // The constraints came through the dump and restore, valid.
    assert_eq!(
        q(
            &env,
            &c,
            "select count(*) from pg_constraint where contype = 'f' and convalidated"
        ),
        "2"
    );
}

#[tokio::test]
async fn staging_removed_after_failure() {
    let Some(env) = TestEnv::new() else { return };
    masked(&env, USERS, "[tables.users]\nnope = \"hash\"")
        .await
        .unwrap_err();
    assert!(side_folders(&env).is_empty(), "{:?}", side_folders(&env));
    // A failing source too: the staging folder never outlives the build.
    masked(&env, "select 1/0", USERS_MASK).await.unwrap_err();
    assert!(side_folders(&env).is_empty(), "{:?}", side_folders(&env));
}
