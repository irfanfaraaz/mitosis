//! Per-branch templates: `clone --branch` layers the caller's branch migrations on the base.
//! Each test makes its own git repository with a `migrations/` folder and a `migrate.sh` that
//! applies every `.sql` file not yet recorded in an `applied` table. Needs Postgres and git.

mod common;

use common::*;
use mitosis::clones::{create, destroy};
use mitosis::error::ErrorCode;
use mitosis::lifecycle::reconcile;
use mitosis::pg::{newest, psql};
use mitosis::protocol::{
    BranchArgs, CloneCreateArgs, CloneInfo, EventFilter, MigrateCmd, OnStale, TemplateInfo,
    TemplateSetArgs, TemplateSettings, TemplateSource,
};
use mitosis::scheduler;
use mitosis::templates::{self, branch_folder_slug, history, layered, refresh};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MIGRATE_SH: &str = r#"set -e
"$PSQL" "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -q -c "create table if not exists applied(name text primary key)"
for f in migrations/*.sql; do
  n=$(basename "$f")
  done_already=$("$PSQL" "$DATABASE_URL" -X -Atc "select 1 from applied where name = '$n'")
  if [ -z "$done_already" ]; then
    "$PSQL" "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -q -f "$f"
    "$PSQL" "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -q -c "insert into applied values ('$n')"
  fi
done
"#;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn git_installed() -> bool {
    let ok = Command::new("git").arg("--version").output().is_ok();
    if !ok {
        println!("skipping: git not installed");
    }
    ok
}

/// Runs git in `dir` without the user's global config (hooks, signing); panics on failure.
fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn commit_all(dir: &Path, msg: &str) {
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", msg]);
}

fn write(dir: &Path, rel: &str, text: &str) {
    std::fs::write(dir.join(rel), text).unwrap();
}

/// A git repository on `main` with `migrations/001.sql` running `first_sql`, committed.
fn repo(env: &TestEnv, first_sql: &str) -> PathBuf {
    let dir = env.tmp.path().join("repo");
    std::fs::create_dir_all(dir.join("migrations")).unwrap();
    let dir = dir.canonicalize().unwrap();
    git(&dir, &["init", "-q", "-b", "main"]);
    git(&dir, &["config", "user.email", "test@example.com"]);
    git(&dir, &["config", "user.name", "Test"]);
    git(&dir, &["config", "commit.gpgsign", "false"]);
    write(&dir, "migrate.sh", MIGRATE_SH);
    write(&dir, "migrations/001.sql", first_sql);
    commit_all(&dir, "first");
    dir
}

fn migrate_command(env: &TestEnv) -> String {
    let psql = newest(&env.ctx.installs()).unwrap().bin("psql");
    format!("PSQL='{}' sh migrate.sh", psql.display())
}

/// Template `main` built by the migrate script in `repo` (on whatever branch is checked out),
/// with that script as its migrate command and `repo/migrations` as its migrations folder.
async fn base(
    env: &TestEnv,
    repo: &Path,
    auto_migrate: bool,
    mask: Option<PathBuf>,
) -> TemplateInfo {
    let command = migrate_command(env);
    let mut args = template_args(
        "main",
        TemplateSource::Seed {
            command: command.clone(),
            cwd: Some(repo.to_path_buf()),
            path: None,
        },
    );
    args.migrations = Some(repo.join("migrations"));
    args.settings.migrate = Some(MigrateCmd {
        command,
        cwd: repo.to_path_buf(),
        path: None,
    });
    args.settings.auto_migrate = Some(auto_migrate);
    args.settings.mask = mask;
    templates::init(&env.ctx, args).await.expect("build base")
}

fn args(branch: Option<(&str, &Path)>) -> CloneCreateArgs {
    CloneCreateArgs {
        template: "main".into(),
        name: None,
        ttl_secs: 7200,
        durable: false,
        parent_pid: None,
        fresh: false,
        protect: false,
        env: None,
        wait_secs: None,
        branch: branch.map(|(name, root)| BranchArgs {
            name: name.into(),
            repo_root: root.to_path_buf(),
            path: None,
        }),
    }
}

async fn clone_branch(env: &TestEnv, branch: &str, root: &Path) -> CloneInfo {
    create(&env.ctx, args(Some((branch, root))))
        .await
        .expect("clone --branch")
}

/// Runs `sql` in the clone over its own socket.
fn q(env: &TestEnv, c: &CloneInfo, sql: &str) -> String {
    let install = newest(&env.ctx.installs()).unwrap().clone();
    psql(&install, &c.socket_dir, c.port, "app", sql).unwrap()
}

fn has_table(env: &TestEnv, c: &CloneInfo, table: &str) -> bool {
    q(
        env,
        c,
        &format!("select to_regclass('{table}') is not null"),
    ) == "t"
}

fn template_dirs(env: &TestEnv) -> Vec<String> {
    std::fs::read_dir(env.ctx.home.templates_dir())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

/// Polls `f` every 100 ms until it is true; panics with `what` after `secs` seconds.
async fn wait_for(secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn clone_branch_applies_branch_migration() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    base(&env, &repo, false, None).await;
    let t = env.ctx.store().get_template("main").unwrap().unwrap();
    assert_eq!(t.base_branch.as_deref(), Some("main"));
    assert_eq!(t.repo_rel_cwd, Some(PathBuf::new()));
    assert_eq!(t.repo_rel_migrations, Some(PathBuf::from("migrations")));

    git(&repo, &["switch", "-q", "-c", "feature"]);
    write(&repo, "migrations/002.sql", "create table b(id int);");
    commit_all(&repo, "feature");
    let c = clone_branch(&env, "feature", &repo).await;
    assert_eq!(c.template, "main~feature");
    assert_eq!(c.branch.as_deref(), Some("feature"));
    assert!(c.rebuilt);
    assert!(has_table(&env, &c, "a") && has_table(&env, &c, "b"));

    let plain = create(&env.ctx, args(None)).await.unwrap();
    assert_eq!(plain.branch, None);
    assert!(has_table(&env, &plain, "a"));
    assert!(
        !has_table(&env, &plain, "b"),
        "the base must not get the branch's migration"
    );

    // Nothing changed: the same branch generation is served.
    let again = clone_branch(&env, "feature", &repo).await;
    assert!(!again.rebuilt);
    assert_eq!(again.generation, c.generation);
}

#[tokio::test(flavor = "multi_thread")]
async fn base_refresh_rebuilds_branch() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    base(&env, &repo, false, None).await;
    git(&repo, &["switch", "-q", "-c", "feature"]);
    write(&repo, "migrations/002.sql", "create table b(id int);");
    commit_all(&repo, "feature");
    assert!(clone_branch(&env, "feature", &repo).await.rebuilt);
    assert!(!clone_branch(&env, "feature", &repo).await.rebuilt);

    git(&repo, &["switch", "-q", "main"]);
    let t = refresh(&env.ctx, "main", None).await.unwrap();
    assert_eq!(t.current_gen, 2);
    git(&repo, &["switch", "-q", "feature"]);
    let c = clone_branch(&env, "feature", &repo).await;
    assert!(
        c.rebuilt,
        "a new base generation must rebuild the branch template"
    );
    assert!(has_table(&env, &c, "b"));
    let row = env
        .ctx
        .store()
        .get_template("main~feature")
        .unwrap()
        .unwrap();
    assert_eq!(row.base_gen, Some(2));

    // A changed migration in the checkout rebuilds too.
    write(&repo, "migrations/003.sql", "create table c(id int);");
    let c = clone_branch(&env, "feature", &repo).await;
    assert!(c.rebuilt);
    assert!(has_table(&env, &c, "c"));
}

#[tokio::test(flavor = "multi_thread")]
async fn base_skips_auto_migrate_on_other_branch() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    base(&env, &repo, true, None).await;
    git(&repo, &["switch", "-q", "-c", "feature"]);
    write(&repo, "migrations/002.sql", "create table b(id int);");

    let skipped = || {
        env.ctx
            .store()
            .list_events(&EventFilter {
                kind: Some("template.skipped".into()),
                ..Default::default()
            })
            .unwrap()
    };
    scheduler::tick(&env.ctx, now()).await;
    wait_for(30, "template.skipped", || !skipped().is_empty()).await;
    let ev = &skipped()[0];
    assert_eq!(ev.template.as_deref(), Some("main"));
    assert_eq!(ev.detail["branch"], "feature");
    assert_eq!(ev.detail["base_branch"], "main");
    {
        let store = env.ctx.store();
        assert_eq!(store.get_template("main").unwrap().unwrap().current_gen, 1);
        assert_eq!(store.list_generations("main").unwrap().len(), 1);
    }

    // The next tick skips again (the skipped build released its slot) without another event.
    // `tick` claims the slot before it returns, so a free slot afterwards means that run ended.
    wait_for(30, "the first skip to release its slot", || {
        !templates::is_building(&env.ctx, "main")
    })
    .await;
    scheduler::tick(&env.ctx, now()).await;
    wait_for(30, "the second skip to release its slot", || {
        !templates::is_building(&env.ctx, "main")
    })
    .await;
    assert_eq!(skipped().len(), 1);
    assert_eq!(env.ctx.store().list_generations("main").unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_layered_refresh_on_other_branch_refused() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    base(&env, &repo, false, None).await;
    git(&repo, &["switch", "-q", "-c", "feature"]);
    let e = layered(&env.ctx, "main", None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    let hint = e.hint.unwrap_or_default();
    assert!(hint.contains("mitosis clone --branch"), "{hint}");
    assert!(hint.contains("git switch main"), "{hint}");
    assert_eq!(history(&env.ctx, "main").unwrap().len(), 1);

    // Back on its branch the same refresh works.
    git(&repo, &["switch", "-q", "main"]);
    assert_eq!(
        layered(&env.ctx, "main", None).await.unwrap().current_gen,
        2
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn worktree_checkout_maps_paths() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    base(&env, &repo, false, None).await;
    let wt = env.tmp.path().join("wt");
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            "feature",
            wt.to_str().unwrap(),
        ],
    );
    let wt = wt.canonicalize().unwrap();
    write(&wt, "migrations/002.sql", "create table b(id int);");
    commit_all(&wt, "feature");
    let c = clone_branch(&env, "feature", &wt).await;
    assert!(has_table(&env, &c, "b"));
    // The main checkout is untouched and still on main.
    assert!(!repo.join("migrations/002.sql").exists());
    let plain = create(&env.ctx, args(None)).await.unwrap();
    assert!(!has_table(&env, &plain, "b"));
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_with_slash_and_unicode() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    base(&env, &repo, false, None).await;
    for (i, branch) in ["feature/x.y", "fix/ümlaut"].into_iter().enumerate() {
        git(&repo, &["switch", "-q", "-c", branch, "main"]);
        write(
            &repo,
            &format!("migrations/1{i}.sql"),
            &format!("create table t{i}(id int);"),
        );
        commit_all(&repo, branch);
        let c = clone_branch(&env, branch, &repo).await;
        let name = format!("main~{branch}");
        assert_eq!(c.template, name);
        assert_eq!(c.branch.as_deref(), Some(branch));
        assert!(has_table(&env, &c, &format!("t{i}")));
        let slug = branch_folder_slug("main", branch);
        assert!(
            template_dirs(&env).contains(&format!("{slug}@{}", c.generation)),
            "{slug} in {:?}",
            template_dirs(&env)
        );
        assert!(history(&env.ctx, &name).unwrap()[0].current);
        let listed = templates::list(&env.ctx).unwrap();
        let t = listed.iter().find(|t| t.name == name).expect("listed");
        assert_eq!(
            (t.base.as_deref(), t.branch.as_deref()),
            (Some("main"), Some(branch))
        );
    }
    assert!(branch_folder_slug("main", "feature/x.y").starts_with("main~feature-x-y-"));
    assert!(branch_folder_slug("main", "fix/ümlaut").starts_with("main~fix--mlaut-"));
    // Plain-name requests keep refusing `~`.
    let a = template_args("main~x", seed("select 1"));
    assert_eq!(
        templates::init(&env.ctx, a).await.unwrap_err().code,
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        refresh(&env.ctx, "main~feature/x.y", None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidRequest
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn gc_drops_deleted_branch_template() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    base(&env, &repo, false, None).await;
    for branch in ["gone", "kept", "old"] {
        git(&repo, &["switch", "-q", "-c", branch, "main"]);
        let c = clone_branch(&env, branch, &repo).await;
        if branch != "kept" {
            destroy(&env.ctx, &c.id).await.unwrap();
        }
    }
    git(&repo, &["switch", "-q", "main"]);
    git(&repo, &["branch", "-q", "-D", "gone", "kept"]);
    // `old` still exists in git but was last used 15 days ago.
    {
        let store = env.ctx.store();
        let mut t = store.get_template("main~old").unwrap().unwrap();
        t.last_used = Some(now() - 15 * 86_400);
        store.update_template_settings(&t).unwrap();
    }
    let build_log = |branch: &str| {
        env.ctx.home.logs_dir().join(format!(
            "template-{}.log",
            branch_folder_slug("main", branch)
        ))
    };
    std::fs::write(build_log("gone"), "build output\n").unwrap();
    reconcile(env.ctx.clone(), now()).await.unwrap();
    assert!(
        !build_log("gone").exists(),
        "the dropped template's log is left"
    );
    let pruned = env
        .ctx
        .store()
        .list_events(&EventFilter {
            kind: Some("template.pruned".into()),
            ..Default::default()
        })
        .unwrap();
    let cause = |name: &str| {
        pruned
            .iter()
            .find(|e| e.template.as_deref() == Some(name))
            .map(|e| e.detail["cause"].clone())
    };
    assert_eq!(cause("main~gone"), Some("branch deleted".into()));
    assert_eq!(cause("main~old"), Some("unused".into()));
    assert_eq!(cause("main~kept"), None);
    let store = env.ctx.store();
    assert!(store.get_template("main~gone").unwrap().is_none());
    assert!(store.get_template("main~old").unwrap().is_none());
    assert!(store.list_generations("main~gone").unwrap().is_empty());
    // A clone still uses it, so it stays although its branch is gone.
    assert!(store.get_template("main~kept").unwrap().is_some());
    drop(store);
    let dirs = template_dirs(&env);
    for branch in ["gone", "old"] {
        let slug = branch_folder_slug("main", branch);
        assert!(
            !dirs.iter().any(|d| d.starts_with(&slug)),
            "{slug} left in {dirs:?}"
        );
    }
    let kept = branch_folder_slug("main", "kept");
    assert!(dirs.iter().any(|d| d.starts_with(&kept)));
    assert!(dirs.iter().any(|d| d.starts_with("main@")));
}

#[tokio::test(flavor = "multi_thread")]
async fn masked_base_branch_is_masked() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(
        &env,
        "create table users(id int primary key, email text not null); \
         insert into users values (1, 'alice@example.com');",
    );
    let mask = env.tmp.path().join("mask.toml");
    std::fs::write(&mask, "[tables.users]\nemail = \"email\"\n").unwrap();
    base(&env, &repo, false, Some(mask.clone())).await;
    git(&repo, &["switch", "-q", "-c", "feature"]);
    write(
        &repo,
        "migrations/002.sql",
        "insert into users values (2, 'bob@example.com');",
    );
    commit_all(&repo, "feature");
    let c = clone_branch(&env, "feature", &repo).await;
    let emails = q(
        &env,
        &c,
        "select string_agg(email, ',' order by id) from users",
    );
    assert!(!emails.contains("@example.com"), "raw emails in {emails}");
    let plain = create(&env.ctx, args(None)).await.unwrap();
    let first = |c: &CloneInfo| q(&env, c, "select email from users where id = 1");
    // One salt: the branch masks values as its base does.
    assert_eq!(first(&c), first(&plain));
    let gen = env.ctx.store().current("main~feature").unwrap().unwrap().1;
    assert!(gen.mask_hash.is_some());

    // A changed masking file makes the base stale for layered builds, branch builds included.
    std::fs::write(&mask, "[tables.users]\nemail = \"redact\"\n").unwrap();
    write(&repo, "migrations/003.sql", "select 1;");
    let e = create(&env.ctx, args(Some(("feature", &repo))))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e
        .hint
        .unwrap_or_default()
        .contains("mitosis template refresh main"));
}

fn set(env: &TestEnv, settings: TemplateSettings) {
    templates::set(
        &env.ctx,
        TemplateSetArgs {
            name: "main".into(),
            max_age_secs: None,
            settings,
            resume: false,
        },
    )
    .expect("template set");
}

fn base_row(env: &TestEnv) -> mitosis::state::TemplateRow {
    env.ctx.store().get_template("main").unwrap().unwrap()
}

const NO_BRANCH: &str = "does not know its own branch";

#[tokio::test(flavor = "multi_thread")]
async fn lazy_paths_never_guess_base_branch() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    // No migrate command and no migrations folder at init: nothing is recorded.
    let command = migrate_command(&env);
    let init = template_args(
        "main",
        TemplateSource::Seed {
            command: command.clone(),
            cwd: Some(repo.clone()),
            path: None,
        },
    );
    templates::init(&env.ctx, init).await.unwrap();
    set(
        &env,
        TemplateSettings {
            migrate: Some(MigrateCmd {
                command,
                cwd: repo.clone(),
                path: None,
            }),
            ..Default::default()
        },
    );
    let t = base_row(&env);
    assert_eq!((t.base_branch, t.repo_rel_cwd), (None, None));

    git(&repo, &["switch", "-q", "-c", "feature"]);
    write(&repo, "migrations/002.sql", "create table b(id int);");
    commit_all(&repo, "feature");
    let c = clone_branch(&env, "feature", &repo).await;
    assert!(has_table(&env, &c, "b"));
    assert!(
        c.warnings
            .iter()
            .any(|w| w.contains(NO_BRANCH)
                && w.contains("mitosis template set main --base-branch <b>")),
        "{:?}",
        c.warnings
    );
    let t = base_row(&env);
    assert_eq!(
        t.base_branch, None,
        "the feature branch must not become the base's"
    );
    assert_eq!(t.repo_rel_cwd, Some(PathBuf::new()));
    // Once: the paths are recorded now.
    let again = clone_branch(&env, "feature", &repo).await;
    assert!(!again.warnings.iter().any(|w| w.contains(NO_BRANCH)));
    // With no base branch, a layered refresh runs on any branch, as before v1.0.
    assert_eq!(
        layered(&env.ctx, "main", None).await.unwrap().current_gen,
        2
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn set_migrate_clears_repo_paths() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    std::fs::create_dir(repo.join("backend")).unwrap();
    write(&repo, "backend/README", "migrate from here");
    commit_all(&repo, "backend");
    base(&env, &repo, false, None).await;
    assert_eq!(base_row(&env).repo_rel_cwd, Some(PathBuf::new()));

    let psql = newest(&env.ctx.installs()).unwrap().bin("psql");
    set(
        &env,
        TemplateSettings {
            migrate: Some(MigrateCmd {
                command: format!("cd .. && PSQL='{}' sh migrate.sh", psql.display()),
                cwd: repo.join("backend"),
                path: None,
            }),
            ..Default::default()
        },
    );
    let t = base_row(&env);
    assert_eq!((t.repo_rel_cwd, t.repo_rel_migrations), (None, None));
    assert_eq!(t.base_branch.as_deref(), Some("main"), "base_branch stays");

    git(&repo, &["switch", "-q", "-c", "feature"]);
    write(&repo, "migrations/002.sql", "create table b(id int);");
    commit_all(&repo, "feature");
    let c = clone_branch(&env, "feature", &repo).await;
    assert!(has_table(&env, &c, "b"));
    assert!(!c.warnings.iter().any(|w| w.contains(NO_BRANCH)));
    let t = base_row(&env);
    assert_eq!(t.repo_rel_cwd, Some(PathBuf::from("backend")));
    assert_eq!(t.repo_rel_migrations, Some(PathBuf::from("migrations")));

    // `--base-branch off` forgets it; a branch and off together are refused.
    set(
        &env,
        TemplateSettings {
            base_branch_off: true,
            ..Default::default()
        },
    );
    assert_eq!(base_row(&env).base_branch, None);
    let both = templates::set(
        &env.ctx,
        TemplateSetArgs {
            name: "main".into(),
            max_age_secs: None,
            settings: TemplateSettings {
                base_branch: Some("main".into()),
                base_branch_off: true,
                ..Default::default()
            },
            resume: false,
        },
    );
    assert_eq!(both.unwrap_err().code, ErrorCode::InvalidRequest);
}

#[tokio::test(flavor = "multi_thread")]
async fn named_branch_clone_reused_is_not_rebuilt() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    base(&env, &repo, false, None).await;
    git(&repo, &["switch", "-q", "-c", "feature"]);
    let named = || CloneCreateArgs {
        name: Some("work".into()),
        ..args(Some(("feature", &repo)))
    };
    let first = create(&env.ctx, named()).await.unwrap();
    assert!(first.rebuilt && !first.existing);
    write(&repo, "migrations/002.sql", "create table b(id int);");
    let again = create(&env.ctx, named()).await.unwrap();
    assert!(again.existing);
    assert!(
        !again.rebuilt,
        "the reused clone was made before the rebuild"
    );
    assert_eq!(again.id, first.id);

    // A branch template takes no promoted generation.
    let e = mitosis::promote::promote(&env.ctx, &first.id, None)
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidRequest);
    assert!(e.hint.unwrap_or_default().contains("--template"));
}

#[tokio::test(flavor = "multi_thread")]
async fn background_branch_rebuild_checks_branch_and_backs_off() {
    let Some(env) = TestEnv::new() else { return };
    if !git_installed() {
        return;
    }
    let repo = repo(&env, "create table a(id int);");
    base(&env, &repo, false, None).await;
    set(
        &env,
        TemplateSettings {
            on_stale: Some(OnStale::Refresh),
            ..Default::default()
        },
    );
    git(&repo, &["switch", "-q", "-c", "feature"]);
    write(&repo, "migrations/002.sql", "create table b(id int);");
    commit_all(&repo, "feature");
    let first = clone_branch(&env, "feature", &repo).await;
    assert!(first.rebuilt);
    let row = || {
        env.ctx
            .store()
            .get_template("main~feature")
            .unwrap()
            .unwrap()
    };

    // The checkout moved to main: the background rebuild is skipped, not run on main's files.
    git(&repo, &["switch", "-q", "main"]);
    let c = clone_branch(&env, "feature", &repo).await;
    assert!(c.stale && c.refreshing && !c.rebuilt);
    assert_eq!(c.generation, first.generation);
    let skipped = || {
        env.ctx
            .store()
            .list_events(&EventFilter {
                template: Some("main~feature".into()),
                kind: Some("template.skipped".into()),
                ..Default::default()
            })
            .unwrap()
    };
    wait_for(30, "the skipped background build", || !skipped().is_empty()).await;
    assert_eq!(skipped()[0].detail["op"], "branch");
    assert_eq!(row().last_error, None);

    // A failing rebuild is recorded and then waits out the back-off.
    git(&repo, &["switch", "-q", "feature"]);
    write(&repo, "migrations/003.sql", "select no_such_function();");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let c = clone_branch(&env, "feature", &repo).await;
    assert!(c.stale && c.refreshing);
    wait_for(120, "the failed background build", || {
        row().last_error.is_some()
    })
    .await;
    assert!(row().last_attempt_at.is_some());
    let c = clone_branch(&env, "feature", &repo).await;
    assert!(
        c.stale && !c.refreshing,
        "a failed build waits out the back-off"
    );
    assert_eq!(c.generation, first.generation);
    assert!(
        c.warnings
            .iter()
            .any(|w| w.contains("last background rebuild")),
        "{:?}",
        c.warnings
    );
}
