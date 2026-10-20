// The serial_home guard only orders whole tests, each on its own runtime thread; no task in the
// same runtime ever waits on it.
#![allow(clippy::await_holding_lock)]

#[path = "common/cli.rs"]
mod cli;
mod common;

use cli::*;
use common::*;
use mitosis::clones::{create, sleep};
use mitosis::diff::{diff, render};
use mitosis::error::ErrorCode;
use mitosis::pg::discover;
use mitosis::protocol::{CloneCreateArgs, CloneInfo, DiffReport, TemplateSource};
use mitosis::state::CloneStatus;
use serde_json::json;
use std::process::Command;

fn args() -> CloneCreateArgs {
    CloneCreateArgs {
        template: "main".into(),
        name: None,
        ttl_secs: 7200,
        durable: false,
        parent_pid: None,
        fresh: false,
        protect: false,
        env: None,
        wait_secs: Some(0),
        branch: None,
    }
}

async fn clone_of(env: &TestEnv) -> CloneInfo {
    create(&env.ctx, args()).await.expect("create clone")
}

async fn run_diff(env: &TestEnv, id: &str, table: Option<&str>) -> DiffReport {
    diff(&env.ctx, id, table, 20).await.expect("diff")
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_shows_schema_changes() {
    let Some(env) = TestEnv::new() else { return };
    template(
        &env,
        "main",
        "create table t(id int primary key, a int, b int); create index t_a on t(a); \
         create function f() returns int language sql as 'select 1'",
    )
    .await;
    let c = clone_of(&env).await;
    psql_url(
        &env,
        &c.url,
        "create table n(x int); alter table t drop column b; drop index t_a; \
         create index t_a on t(id, a); \
         create function g() returns int language sql as 'select 2'",
    );

    let report = run_diff(&env, &c.id, None).await;
    let got: Vec<(&str, &str, &str)> = report
        .schema
        .iter()
        .map(|s| (s.kind.as_str(), s.name.as_str(), s.change.as_str()))
        .collect();
    assert_eq!(
        got,
        [
            ("table", "n", "added"),
            ("column", "t.b", "removed"),
            ("index", "t_a", "changed"),
            ("function", "g()", "added"),
        ],
        "{report:?}"
    );
    let idx = &report.schema[2];
    assert!(idx.before.as_deref().unwrap().contains("(a)"), "{idx:?}");
    assert!(idx.after.as_deref().unwrap().contains("(id, a)"), "{idx:?}");
    assert_eq!(report.template, "main");
    assert_eq!(report.generation, 1);
    assert!(render(&report).contains("+ table n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_row_counts() {
    let Some(env) = TestEnv::new() else { return };
    template(
        &env,
        "main",
        "create table t(id int); insert into t values (1)",
    )
    .await;
    let c = clone_of(&env).await;
    psql_url(&env, &c.url, "insert into t values (2), (3), (4)");

    let report = run_diff(&env, &c.id, None).await;
    assert!(report.schema.is_empty(), "{:?}", report.schema);
    let t = report.tables.iter().find(|t| t.table == "t").expect("t");
    let before = t.before.as_ref().expect("before");
    assert_eq!((before.n, before.estimate), (1, false));
    let after = t.after.as_ref().expect("after");
    assert_eq!((after.n, after.estimate), (4, false));
    assert!(report.rows.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_table_rows() {
    let Some(env) = TestEnv::new() else { return };
    template(
        &env,
        "main",
        "create table t(id int primary key, v text); \
         insert into t values (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    let c = clone_of(&env).await;
    psql_url(
        &env,
        &c.url,
        "insert into t values (4, 'd'); delete from t where id = 1; \
         update t set v = 'bb' where id = 2",
    );

    let report = run_diff(&env, &c.id, Some("t")).await;
    let rows = report.rows.expect("rows");
    assert_eq!(rows.table, "t");
    assert_eq!(
        (rows.inserted_total, rows.deleted_total, rows.updated_total),
        (1, 1, 1)
    );
    assert_eq!(rows.inserted[0]["id"], 4);
    assert_eq!(rows.deleted[0]["id"], 1);
    assert_eq!(rows.updated[0].pk, json!(2));
    assert_eq!(rows.updated[0].columns, ["v"]);

    // The limit caps the rows shown, not the totals.
    psql_url(
        &env,
        &c.url,
        "insert into t select g, 'x' from generate_series(10, 14) g",
    );
    let limited = diff(&env.ctx, &c.id, Some("t"), 2).await.expect("diff");
    let rows = limited.rows.expect("rows");
    assert_eq!((rows.inserted.len(), rows.inserted_total), (2, 6));
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_table_without_pk_is_refused() {
    let Some(env) = TestEnv::new() else { return };
    template(
        &env,
        "main",
        "create table t(id int); insert into t values (1)",
    )
    .await;
    let c = clone_of(&env).await;
    let e = diff(&env.ctx, &c.id, Some("t"), 20).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e.message.contains("primary key"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_pruned_generation_errors() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(id int)").await;
    let c = clone_of(&env).await;
    let gen = env
        .ctx
        .store()
        .get_generation("main", c.generation)
        .unwrap()
        .unwrap();
    std::fs::remove_dir_all(&gen.dir).unwrap();

    let e = diff(&env.ctx, &c.id, None, 20).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e.message.contains("pruned"), "{e}");
    assert!(e.hint.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_of_sleeping_clone_wakes_it() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(id int)").await;
    let c = clone_of(&env).await;
    psql_url(&env, &c.url, "insert into t values (1)");
    // Retried while psql's just-closed backend still shows in pg_stat_activity.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match sleep(&env.ctx, &c.id, "test").await {
            Ok(()) => break,
            Err(e)
                if e.message.contains("open connections")
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await
            }
            Err(e) => panic!("sleep: {e}"),
        }
    }
    let status = |env: &TestEnv| env.ctx.store().get_clone(&c.id).unwrap().unwrap().status;
    assert_eq!(status(&env), CloneStatus::Sleeping);

    let report = run_diff(&env, &c.id, None).await;
    assert_eq!(status(&env), CloneStatus::Ready);
    let t = report.tables.iter().find(|t| t.table == "t").expect("t");
    assert_eq!(
        (
            t.before.as_ref().map(|c| c.n),
            t.after.as_ref().map(|c| c.n)
        ),
        (Some(0), Some(1))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_odd_identifiers_and_composite_key() {
    let Some(env) = TestEnv::new() else { return };
    template(
        &env,
        "main",
        r#"create table "My Table"(id int primary key, v text);
           create schema app;
           create table app."we""ird"(id int primary key, v text);
           create table comp(a int, b int, v text, primary key(a, b));
           insert into "My Table" values (1, 'a');
           insert into app."we""ird" values (1, 'a'), (2, 'b');
           insert into comp values (1, 1, 'x'), (1, 2, 'y')"#,
    )
    .await;
    let c = clone_of(&env).await;
    psql_url(
        &env,
        &c.url,
        r#"insert into "My Table" values (2, 'b');
           alter table "My Table" add column extra int;
           update app."we""ird" set v = 'z' where id = 2;
           create index "we""ird_idx" on app."we""ird"(v);
           update comp set v = 'yy' where a = 1 and b = 2;
           insert into comp values (2, 1, 'n');
           create table fresh(id int primary key);
           insert into fresh values (7)"#,
    );

    let report = run_diff(&env, &c.id, None).await;
    let got: Vec<(&str, &str, &str)> = report
        .schema
        .iter()
        .map(|s| (s.kind.as_str(), s.name.as_str(), s.change.as_str()))
        .collect();
    assert_eq!(
        got,
        [
            ("table", "fresh", "added"),
            ("column", "My Table.extra", "added"),
            ("index", "app.we\"ird_idx", "added"),
        ],
        "{report:?}"
    );
    // A table on one side only has no count on the other.
    let fresh = report.tables.iter().find(|t| t.table == "fresh").unwrap();
    assert!(fresh.before.is_none() && fresh.after.is_some(), "{fresh:?}");

    let rows = |t: &'static str| {
        let env = &env;
        let id = c.id.clone();
        async move { run_diff(env, &id, Some(t)).await.rows.expect("rows") }
    };
    let my = rows("My Table").await;
    assert_eq!(
        (my.inserted_total, my.deleted_total, my.updated_total),
        (1, 0, 1)
    );
    assert_eq!(my.updated[0].pk, json!(1));
    assert_eq!(my.updated[0].columns, ["extra"]);

    let weird = rows("app.we\"ird").await;
    assert_eq!(
        (
            weird.inserted_total,
            weird.deleted_total,
            weird.updated_total
        ),
        (0, 0, 1)
    );
    assert_eq!(weird.updated[0].pk, json!(2));
    assert_eq!(weird.updated[0].columns, ["v"]);

    let comp = rows("comp").await;
    assert_eq!(
        (comp.inserted_total, comp.deleted_total, comp.updated_total),
        (1, 0, 1)
    );
    assert_eq!(comp.updated[0].pk, json!([1, 2]));
    assert_eq!(comp.updated[0].columns, ["v"]);
    assert_eq!(comp.inserted[0]["a"], 2);

    let fresh = rows("fresh").await;
    assert_eq!((fresh.inserted_total, fresh.deleted_total), (1, 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_limit_is_capped() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(id int primary key)").await;
    let c = clone_of(&env).await;
    let e = diff(&env.ctx, &c.id, Some("t"), 1001).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e.hint.is_some());
    diff(&env.ctx, &c.id, Some("t"), 1000)
        .await
        .expect("1000 is allowed");
}

fn base_leftovers(env: &TestEnv) -> Vec<String> {
    let home = &env.ctx.home;
    let mut left = Vec::new();
    for dir in [home.templates_dir(), home.sockets_dir(), home.logs_dir()] {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if n.contains("diff") || (n.starts_with('d') && n.len() == 9 && n != "daemon.log") {
                left.push(n);
            }
        }
    }
    let ps = Command::new("ps")
        .args(["-axo", "command"])
        .output()
        .unwrap();
    let ps = String::from_utf8_lossy(&ps.stdout);
    let root = env.ctx.home.root.to_string_lossy().into_owned();
    left.extend(
        ps.lines()
            .filter(|l| l.contains(".diff-") && l.contains(&root))
            .map(String::from),
    );
    left
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_leaves_no_base_folder() {
    let Some(env) = TestEnv::new() else { return };
    template(&env, "main", "create table t(id int)").await;
    let c = clone_of(&env).await;

    run_diff(&env, &c.id, None).await;
    assert_eq!(base_leftovers(&env), Vec::<String>::new(), "after success");

    // Fails after the base server is up: the table has no primary key.
    diff(&env.ctx, &c.id, Some("t"), 20).await.unwrap_err();
    assert_eq!(base_leftovers(&env), Vec::<String>::new(), "after an error");
}

// ---- MCP, through a real daemon -------------------------------------------------------------

const SEED: &str = "create table t(id int primary key); insert into t values (1)";

#[tokio::test(flavor = "multi_thread")]
async fn mcp_clone_diff_refuses_other_session() {
    if discover().is_empty() {
        println!("skipping: no Postgres installed");
        return;
    }
    let (_serial, tmp) = serial_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);
    let TemplateSource::Seed { command, .. } = seed(SEED) else {
        unreachable!()
    };
    ok(home, &["template", "init", "--seed", &command]);

    // Made by the CLI: no MCP session owns it.
    let other: CloneInfo = serde_json::from_str(&ok(home, &["clone", "--json"])).unwrap();
    let client = mcp_client(home).await;
    let r = call(&client, "clone_diff", json!({"id_or_name": other.id})).await;
    let text = result_text(&r);
    assert_eq!(r.is_error, Some(true), "{text}");
    assert!(text.contains("not created by this session"), "{text}");

    // Its own clone works.
    let r = call(&client, "clone_create", json!({})).await;
    let mine: CloneInfo = serde_json::from_str(&result_text(&r)).expect("clone_create text");
    let r = call(&client, "clone_diff", json!({"id_or_name": mine.id})).await;
    let text = result_text(&r);
    assert_ne!(r.is_error, Some(true), "{text}");
    let report: DiffReport = serde_json::from_str(&text).expect("clone_diff text parses");
    assert_eq!(report.clone, mine.id);
    client.cancel().await.expect("stop mcp client");
}
