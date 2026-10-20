use crate::clone_backend::apparent_size;
use crate::clones::START_TRIES;
use crate::ctx::Ctx;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::events;
use crate::mask::{mask_statements, subset_statements, ColumnInfo, ForeignKey, MaskFile};
use crate::names::{random_hex, validate_name};
use crate::paths::Home;
use crate::pg::{
    disable_preload_libraries, ensure_postgres_superuser, initdb, is_missing_library,
    make_standalone, psql, read_pg_version, start, stop, wait_ready, PgInstall, RunningPg,
    StartOptions,
};
use crate::ports::pick_port;
use crate::protocol::{
    BranchArgs, EventFilter, GenKind, GenerationInfo, MigrateCmd, OnStale, RefreshMode,
    TemplateInfo, TemplateInitArgs, TemplateSetArgs, TemplateSettings, TemplateSource,
};
use crate::state::{GenerationRow, TemplateRow};
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_SEED_DATABASE: &str = "app";
/// The shortest refresh interval `--every` accepts.
const MIN_EVERY_SECS: u64 = 60;

fn internal(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, msg)
}

fn invalid(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::InvalidRequest, msg)
}

pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Runs blocking work (Postgres tools, big file operations) off the async runtime.
async fn blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| internal(format!("background task failed: {e}")))?
}

pub async fn init(ctx: &Ctx, mut args: TemplateInitArgs) -> Result<TemplateInfo> {
    validate_name("template", &args.name)?;
    if let TemplateSource::Url { url } | TemplateSource::Basebackup { url } = &args.source {
        if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
            return Err(invalid(
                "source URL must start with postgres:// or postgresql://",
            ));
        }
        split_password(url)?;
    }
    if matches!(args.source, TemplateSource::Promoted { .. }) {
        return Err(
            invalid("a template cannot be created from a promoted source")
                .with_hint("make one with: mitosis promote <clone> --template <name>"),
        );
    }
    if let Some(m) = &args.migrations {
        if !m.is_dir() {
            return Err(invalid(format!(
                "migrations folder {} does not exist",
                m.display()
            )));
        }
    }
    // An existing template gets a new generation from the new source, on the same database.
    let existing = ctx.store().get_template(&args.name)?;
    if let Some(t) = &existing {
        if args.database.is_none() && matches!(args.source, TemplateSource::Seed { .. }) {
            args.database = Some(t.database.clone());
        }
        let db = database_for(&args)?;
        if db != t.database {
            return Err(invalid(format!(
                "template {} uses database {}, not {db}",
                t.name, t.database
            ))
            .with_hint(format!(
                "use another template name, or keep the database {}",
                t.database
            )));
        }
    }
    // Settings are checked before the build and applied with its generation.
    let mut preview =
        existing.unwrap_or_else(|| TemplateRow::new(&args.name, "", args.source.clone(), None, 0));
    preview.migrations = args.migrations.clone();
    preview.source = args.source.clone();
    apply_settings(&mut preview, None, &args.settings)?;
    check_mask_file(&args.settings)?;
    // The checkout as it is now, recorded once the build is in.
    let (cwd, mig) = (
        preview.migrate.as_ref().map(|m| m.cwd.clone()),
        preview.migrations.clone(),
    );
    let paths = blocking(move || Ok(git_paths(cwd.as_deref(), mig.as_deref()))).await?;
    let (name, explicit_branch) = (args.name.clone(), args.settings.base_branch.is_some());
    build(ctx, args).await?;
    {
        let store = ctx.store();
        let mut t = store.get_template(&name)?.ok_or_else(|| not_found(&name))?;
        record_git_paths(&mut t, paths, explicit_branch);
        store.update_template_settings(&t)?;
    }
    template_info(ctx, &name)
}

/// What `git_paths` found: the checked-out branch and the repo-relative migrate folder and
/// migrations folder.
type GitPaths = (Option<String>, Option<PathBuf>, Option<PathBuf>);

/// Forgets the repo-relative paths, which belong to the migrate and migrations folders recorded
/// when they were derived.
fn clear_repo_paths(t: &mut TemplateRow) {
    (t.repo_rel_cwd, t.repo_rel_migrations) = (None, None);
}

/// Writes `paths` into `t`; `base_branch` stays when `keep_branch` (it was given explicitly).
fn record_git_paths(t: &mut TemplateRow, paths: GitPaths, keep_branch: bool) {
    if !keep_branch {
        t.base_branch = paths.0;
    }
    (t.repo_rel_cwd, t.repo_rel_migrations) = (paths.1, paths.2);
}

/// Runs `git -C <dir> <args>`; its trimmed output when it succeeds, `None` when git fails or is
/// missing. Blocking.
pub(crate) fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// The branch checked out in the git work tree of `cwd` (else of `migrations`), and each folder
/// relative to the root of its work tree; `None` for what is not in a work tree. Blocking.
fn git_paths(cwd: Option<&Path>, migrations: Option<&Path>) -> GitPaths {
    let rel = |dir: &Path| {
        let top = git(dir, &["rev-parse", "--show-toplevel"])?;
        let top = Path::new(&top).canonicalize().ok()?;
        let dir = dir.canonicalize().ok()?;
        dir.strip_prefix(&top).ok().map(Path::to_path_buf)
    };
    let (rel_cwd, rel_mig) = (cwd.and_then(rel), migrations.and_then(rel));
    let branch = cwd
        .filter(|_| rel_cwd.is_some())
        .or(migrations.filter(|_| rel_mig.is_some()))
        .and_then(|d| git(d, &["branch", "--show-current"]))
        .filter(|b| !b.is_empty());
    (branch, rel_cwd, rel_mig)
}

/// A masking file given in a request must load. Checked at request time only: the settings
/// written after a build never fail on a file edited meanwhile.
fn check_mask_file(s: &TemplateSettings) -> Result<()> {
    match &s.mask {
        Some(p) => MaskFile::load(p).map(drop),
        None => Ok(()),
    }
}

pub(crate) fn not_found(name: &str) -> MitosisError {
    MitosisError::new(
        ErrorCode::TemplateNotFound,
        format!("template {name} not found"),
    )
    .with_hint("see existing templates with: mitosis template list")
}

/// Rebuilds `name` from its recorded source. `path`, when given, replaces a seed's recorded `PATH`.
pub async fn refresh(ctx: &Ctx, name: &str, path: Option<String>) -> Result<TemplateInfo> {
    validate_name("template", name)?;
    let args = refresh_args(ctx, name, path)?;
    build(ctx, args).await
}

/// The build arguments that rebuild `name` from its recorded source on the current major.
fn refresh_args(ctx: &Ctx, name: &str, path: Option<String>) -> Result<TemplateInitArgs> {
    let (row, cur) = ctx.store().current(name)?.ok_or_else(|| not_found(name))?;
    if matches!(row.source, TemplateSource::Promoted { .. }) {
        return Err(invalid(format!(
            "template {name} was promoted from a clone and has no source to rebuild"
        ))
        .with_hint("use --layered or promote again"));
    }
    let source = match row.source {
        TemplateSource::Seed {
            command,
            cwd,
            path: recorded,
        } => TemplateSource::Seed {
            command,
            cwd,
            path: path.or(recorded),
        },
        url => url,
    };
    Ok(TemplateInitArgs {
        name: row.name,
        source,
        database: Some(row.database),
        migrations: row.migrations,
        // None: a `template set --max-age` made during the build must survive it.
        max_age_secs: None,
        pg_major: Some(cur.pg_major),
        settings: TemplateSettings::default(),
    })
}

/// Changes the settings of `name`; `resume` clears `paused`.
pub fn set(ctx: &Ctx, args: TemplateSetArgs) -> Result<TemplateInfo> {
    validate_name("template", &args.name)?;
    if args.max_age_secs.is_none() && !args.resume && args.settings == TemplateSettings::default() {
        return Err(invalid("template set needs a setting to change")
            .with_hint("for example: mitosis template set <name> --keep 5"));
    }
    check_mask_file(&args.settings)?;
    {
        let store = ctx.store();
        let mut t = store
            .get_template(&args.name)?
            .ok_or_else(|| not_found(&args.name))?;
        apply_settings(&mut t, args.max_age_secs, &args.settings)?;
        if args.resume {
            t.paused = false;
            // The last failure stays visible, but it no longer holds the next automatic build back.
            t.last_attempt_at = None;
        }
        store.update_template_settings(&t)?;
    }
    template_info(ctx, &args.name)
}

/// Checks `s` and writes it, and `max_age_secs` when given, into `t`.
fn apply_settings(
    t: &mut TemplateRow,
    max_age_secs: Option<u64>,
    s: &TemplateSettings,
) -> Result<()> {
    if s.every_secs.is_some() && s.every_off {
        return Err(invalid("set --every to a duration or to off, not both"));
    }
    if s.migrate.is_some() && s.migrate_off {
        return Err(invalid("set --migrate to a command or to off, not both"));
    }
    if s.mask.is_some() && s.mask_off {
        return Err(invalid("set --mask to a file or to off, not both"));
    }
    if s.base_branch.is_some() && s.base_branch_off {
        return Err(invalid("set --base-branch to a branch or to off, not both"));
    }
    if s.keep == Some(0) {
        return Err(invalid("--keep must be at least 1"));
    }
    if s.every_secs.is_some_and(|e| e < MIN_EVERY_SECS) {
        return Err(invalid("--every must be at least 1m"));
    }
    if s.migrate
        .as_ref()
        .is_some_and(|m| m.command.trim().is_empty())
    {
        return Err(invalid("--migrate needs a command, or off"));
    }
    if let Some(m) = s.migrate.as_ref().filter(|m| !m.cwd.is_dir()) {
        return Err(invalid(format!(
            "--migrate would run in {}, which is not a folder",
            m.cwd.display()
        )));
    }
    if let Some(v) = max_age_secs {
        t.max_age_secs = v;
    }
    if let Some(k) = s.keep {
        t.keep = k;
    }
    if s.every_off {
        t.every_secs = None;
    } else if s.every_secs.is_some() {
        t.every_secs = s.every_secs;
    }
    if let Some(m) = s.every_mode {
        t.every_mode = m;
    }
    if s.migrate_off {
        t.migrate = None;
    } else if s.migrate.is_some() {
        t.migrate = s.migrate.clone();
    }
    if s.migrate_off || s.migrate.is_some() {
        // Re-derived from the new folder at the next `clone --branch` (or by `template init`).
        clear_repo_paths(t);
    }
    if let Some(a) = s.auto_migrate {
        t.auto_migrate = a;
    }
    if s.mask_off {
        t.mask_path = None;
    } else if s.mask.is_some() {
        t.mask_path = s.mask.clone();
    }
    if let Some(o) = s.on_stale {
        t.on_stale = o;
    }
    if s.base_branch_off {
        t.base_branch = None;
    } else if let Some(b) = &s.base_branch {
        if b.trim().is_empty() {
            return Err(invalid("--base-branch needs a branch name, or off"));
        }
        t.base_branch = Some(b.clone());
    }
    // A full refresh of a promoted template is refused, so a schedule must be layered.
    if matches!(t.source, TemplateSource::Promoted { .. })
        && t.every_secs.is_some()
        && t.every_mode == RefreshMode::Full
    {
        return Err(invalid(
            "--every with --mode full needs a source, and a promoted template has none",
        )
        .with_hint("use --mode layered"));
    }
    if t.auto_migrate && t.migrations.is_none() {
        return Err(
            invalid("--auto-migrate on needs --migrations").with_hint(format!(
            "record the migrations folder with: mitosis template init {} --migrations <folder> ...",
            t.name
        )),
        );
    }
    if t.auto_migrate && t.migrate.is_none() {
        return Err(invalid("--auto-migrate on needs --migrate")
            .with_hint(format!("mitosis template set {} --migrate \"…\"", t.name)));
    }
    Ok(())
}

/// Templates with a current generation; one still building its first is left out.
pub fn list(ctx: &Ctx) -> Result<Vec<TemplateInfo>> {
    let store = ctx.store();
    let now = now();
    let mut out = Vec::new();
    for t in store.list_templates()? {
        let gens = store.list_generations(&t.name)?;
        if let Some(cur) = gens.iter().find(|g| g.gen == t.current_gen) {
            out.push(info(&t, cur, gens.len() as u32, now));
        }
    }
    Ok(out)
}

/// The template with its current generation `cur`, out of `generations` kept.
pub(crate) fn template_info(ctx: &Ctx, name: &str) -> Result<TemplateInfo> {
    let store = ctx.store();
    let (t, cur) = store.current(name)?.ok_or_else(|| not_found(name))?;
    let n = store.list_generations(name)?.len() as u32;
    Ok(info(&t, &cur, n, now()))
}

pub(crate) fn age_secs(built_at: i64, now: i64) -> u64 {
    now.saturating_sub(built_at).max(0) as u64
}

pub fn info(row: &TemplateRow, cur: &GenerationRow, generations: u32, now: i64) -> TemplateInfo {
    let age_secs = age_secs(cur.built_at, now);
    let mut warnings = Vec::new();
    if age_secs > row.max_age_secs {
        // A branch template is rebuilt from its base, so the base is what to refresh.
        warnings.push(format!(
            "template is {} days old; run: mitosis template refresh {}",
            age_secs / 86_400,
            row.base.as_deref().unwrap_or(&row.name)
        ));
    }
    if let Some(dir) = row.migrations.as_deref().filter(|d| d.is_dir()) {
        if hash_dir(dir).ok() != cur.migrations_hash {
            warnings.push(format!(
                "migrations in {} changed since the template was built",
                dir.display()
            ));
        }
    }
    if let Some(p) = &row.mask_path {
        if MaskFile::hash(p).ok() != cur.mask_hash {
            warnings.push(format!(
                "mask file {} changed since the template was built",
                p.display()
            ));
        }
    }
    let source = match &row.source {
        TemplateSource::Url { url } => TemplateSource::Url {
            url: redact_url(url),
        },
        TemplateSource::Basebackup { url } => TemplateSource::Basebackup {
            url: redact_url(url),
        },
        TemplateSource::Seed { command, cwd, path } => TemplateSource::Seed {
            command: redact_url(command),
            cwd: cwd.clone(),
            path: path.clone(),
        },
        promoted @ TemplateSource::Promoted { .. } => promoted.clone(),
    };
    TemplateInfo {
        name: row.name.clone(),
        pg_major: cur.pg_major,
        source,
        database: row.database.clone(),
        size_bytes: cur.size_bytes,
        built_at: cur.built_at,
        age_secs,
        warnings,
        current_gen: row.current_gen,
        generations,
        paused: row.paused,
        last_error: row.last_error.clone(),
        every_secs: row.every_secs,
        every_mode: row.every_mode,
        migrate: row.migrate.as_ref().map(|m| redact_url(&m.command)),
        auto_migrate: row.auto_migrate,
        on_stale: row.on_stale,
        keep: row.keep,
        mask: row.mask_path.clone(),
        base: row.base.clone(),
        branch: row.branch.clone(),
        base_branch: row.base_branch.clone(),
    }
}

pub fn history(ctx: &Ctx, name: &str) -> Result<Vec<GenerationInfo>> {
    validate_template_ref(name)?;
    let store = ctx.store();
    let t = store.get_template(name)?.ok_or_else(|| not_found(name))?;
    Ok(store
        .list_generations(name)?
        .into_iter()
        .map(|g| GenerationInfo {
            gen: g.gen,
            kind: g.kind,
            built_at: g.built_at,
            pg_major: g.pg_major,
            size_bytes: g.size_bytes,
            current: g.gen == t.current_gen,
            from_clone: g.from_clone,
            from_gen: g.from_gen,
        })
        .collect())
}

/// Points `name` at generation `to` (default: the newest older than the current one) and pauses
/// scheduled refreshes, so the rollback sticks.
pub async fn rollback(ctx: &Ctx, name: &str, to: Option<u32>) -> Result<TemplateInfo> {
    validate_name("template", name)?;
    let (cur, gens) = {
        let store = ctx.store();
        let (_, cur) = store.current(name)?.ok_or_else(|| not_found(name))?;
        (cur, store.list_generations(name)?)
    };
    let hint = || format!("see generations with: mitosis template history {name}");
    let target = match to {
        Some(n) => gens.into_iter().find(|g| g.gen == n).ok_or_else(|| {
            invalid(format!("template {name} has no generation {n}")).with_hint(hint())
        })?,
        None => gens
            .into_iter()
            .rev()
            .find(|g| g.gen < cur.gen)
            .ok_or_else(|| {
                invalid(format!(
                    "template {name} has no generation older than {}",
                    cur.gen
                ))
                .with_hint(hint())
            })?,
    };
    // Clones of the target need its Postgres major.
    ctx.install_major(target.pg_major).await?;
    {
        let store = ctx.store();
        if !store.set_current(name, target.gen, Some(cur.gen))? {
            return Err(invalid(format!(
                "template {name} changed during the rollback; try again"
            )));
        }
        let mut t = store.get_template(name)?.ok_or_else(|| not_found(name))?;
        t.paused = true;
        store.update_template_settings(&t)?;
    }
    events::emit(
        ctx,
        events::TEMPLATE_ROLLED_BACK,
        None,
        Some(name),
        serde_json::json!({ "from": cur.gen, "to": target.gen }),
    );
    template_info(ctx, name)
}

/// Masks the password of every `scheme://user:password@host` and every `?password=` query value as `***`.
pub fn redact_url(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find("://") {
        let (head, tail) = rest.split_at(i + 3);
        out.push_str(head);
        let end = tail
            .find(|c: char| matches!(c, '/' | '?' | '#' | '\'' | '"') || c.is_whitespace())
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        match authority
            .rfind('@')
            .and_then(|at| authority[..at].find(':').map(|colon| (colon, at)))
        {
            Some((colon, at)) => {
                out.push_str(&authority[..colon]);
                out.push_str(":***");
                out.push_str(&authority[at..]);
            }
            None => out.push_str(authority),
        }
        let after = &tail[end..];
        let url_end = after
            .find(|c: char| matches!(c, '\'' | '"') || c.is_whitespace())
            .unwrap_or(after.len());
        out.push_str(&mask_query_password(&after[..url_end]));
        rest = &after[url_end..];
    }
    out.push_str(rest);
    mask_env_passwords(&out)
}

/// Masks the value of every `NAME_PASSWORD=<value>` assignment (`PGPASSWORD=secret psql ...`),
/// quoted or not, as `***`.
fn mask_env_passwords(text: &str) -> String {
    const KEY: &str = "PASSWORD=";
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find(KEY) {
        let (head, tail) = rest.split_at(i + KEY.len());
        out.push_str(head);
        let value_len = match tail.chars().next() {
            Some(q @ ('\'' | '"')) => tail[1..].find(q).map_or(tail.len(), |e| e + 2),
            _ => tail.find(char::is_whitespace).unwrap_or(tail.len()),
        };
        if value_len > 0 {
            out.push_str("***");
        }
        rest = &tail[value_len..];
    }
    out.push_str(rest);
    out
}

/// Masks every `password=<value>` query parameter of the URL tail `s` (path, query, fragment).
fn mask_query_password(s: &str) -> String {
    let Some(q) = s.find('?') else {
        return s.to_string();
    };
    let (path, query) = s.split_at(q + 1);
    let (query, frag) = query.split_at(query.find('#').unwrap_or(query.len()));
    let masked: Vec<&str> = query
        .split('&')
        .map(|p| {
            if p.starts_with("password=") {
                "password=***"
            } else {
                p
            }
        })
        .collect();
    format!("{path}{}{frag}", masked.join("&"))
}

/// A source URL with its password taken out, so the password can travel in `PGPASSWORD`.
#[derive(Debug, PartialEq)]
struct SplitUrl {
    /// The URL without the password (`postgres://u@h/db`).
    url: String,
    /// Percent-decoded password, as libpq would read it.
    password: Option<String>,
    /// The password exactly as written in the URL.
    raw_password: Option<String>,
}

fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = b
                .get(i + 1..i + 3)
                .filter(|h| h.iter().all(u8::is_ascii_hexdigit))?;
            out.push(u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Splits the password out of `url`: the userinfo one or a `?password=` query parameter
/// (the query one wins, as in libpq). Bad percent-encoding is an error that never echoes the password.
fn split_password(url: &str) -> Result<SplitUrl> {
    let plain = || SplitUrl {
        url: url.to_string(),
        password: None,
        raw_password: None,
    };
    let Some(i) = url.find("://") else {
        return Ok(plain());
    };
    let (head, tail) = url.split_at(i + 3);
    let end = tail.find(['/', '?', '#']).unwrap_or(tail.len());
    let authority = &tail[..end];
    let mut raw: Option<String> = None;
    let mut authority_out = authority.to_string();
    if let Some(at) = authority.rfind('@') {
        if let Some(colon) = authority[..at].find(':') {
            raw = Some(authority[colon + 1..at].to_string());
            authority_out = format!("{}{}", &authority[..colon], &authority[at..]);
        }
    }
    let mut rest = tail[end..].to_string();
    if let Some(q) = rest.find('?') {
        let frag = rest[q..].find('#').map_or(rest.len(), |f| q + f);
        let mut kept = Vec::new();
        for p in rest[q + 1..frag].split('&').filter(|p| !p.is_empty()) {
            match p.strip_prefix("password=") {
                Some(v) => raw = Some(v.to_string()),
                None => kept.push(p),
            }
        }
        let query = if kept.is_empty() {
            String::new()
        } else {
            format!("?{}", kept.join("&"))
        };
        let joined = format!("{}{query}{}", &rest[..q], &rest[frag..]);
        rest = joined;
    }
    let Some(raw) = raw else {
        return Ok(plain());
    };
    let password = percent_decode(&raw)
        .ok_or_else(|| invalid("the password in the source URL has invalid percent-encoding"))?;
    Ok(SplitUrl {
        url: format!("{head}{authority_out}{rest}"),
        password: Some(password),
        raw_password: Some(raw),
    })
}

/// Literal secrets of a source (the URL password, decoded and raw), longest first.
fn source_secrets(source: &TemplateSource) -> Vec<String> {
    let (TemplateSource::Url { url } | TemplateSource::Basebackup { url }) = source else {
        return Vec::new();
    };
    let Ok(split) = split_password(url) else {
        return Vec::new();
    };
    let mut s: Vec<String> = [split.password, split.raw_password]
        .into_iter()
        .flatten()
        .filter(|p| !p.is_empty())
        .collect();
    s.sort_by_key(|p| std::cmp::Reverse(p.len()));
    s.dedup();
    s
}

/// `redact_url` plus removal of the literal secrets, for anything shown to the user.
fn scrub(text: &str, secrets: &[String]) -> String {
    secrets
        .iter()
        .fold(redact_url(text), |t, s| t.replace(s.as_str(), "***"))
}

pub(crate) fn fnv1a(h: &mut u64, bytes: &[u8]) {
    for b in bytes {
        *h ^= *b as u64;
        *h = h.wrapping_mul(0x0100_0000_01b3);
    }
}

fn collect_files(root: &Path, rel: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let dir = root.join(rel);
    let rd =
        std::fs::read_dir(&dir).map_err(|e| internal(format!("read {}: {e}", dir.display())))?;
    for entry in rd {
        let entry = entry?;
        let rel = rel.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            collect_files(root, &rel, out)?;
        } else if entry.path().is_file() {
            out.push(rel); // regular files and symlinks to files; symlinked folders are skipped
        }
    }
    Ok(())
}

/// FNV-1a 64 over the sorted relative paths and contents of every file under `dir`, as hex.
pub fn hash_dir(dir: &Path) -> Result<String> {
    let mut files = Vec::new();
    collect_files(dir, Path::new(""), &mut files)?;
    files.sort();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for rel in files {
        let path = dir.join(&rel);
        let data =
            std::fs::read(&path).map_err(|e| internal(format!("read {}: {e}", path.display())))?;
        fnv1a(&mut h, rel.as_os_str().as_bytes());
        fnv1a(&mut h, &[0]);
        fnv1a(&mut h, &data);
        fnv1a(&mut h, &[0]);
    }
    Ok(format!("{h:016x}"))
}

// shortcut: no percent-decoding of the path; upgrade if sources use encoded database names.
fn db_from_url(url: &str) -> String {
    let after_scheme = url.split_once("://").map_or(url, |(_, r)| r);
    let path = after_scheme.split_once('/').map_or("", |(_, p)| p);
    let db = path.split(['?', '#']).next().unwrap_or("");
    if db.is_empty() {
        "postgres".into()
    } else {
        db.into()
    }
}

fn database_for(args: &TemplateInitArgs) -> Result<String> {
    let db = match &args.source {
        TemplateSource::Url { url } => db_from_url(url),
        TemplateSource::Basebackup { url } => {
            args.database.clone().unwrap_or_else(|| db_from_url(url))
        }
        TemplateSource::Seed { .. } | TemplateSource::Promoted { .. } => args
            .database
            .clone()
            .unwrap_or_else(|| DEFAULT_SEED_DATABASE.into()),
    };
    // psql and pg_restore read a -d value with '=' or a URI prefix as a connection string.
    if db.is_empty() || db.contains('=') || db.contains("://") || db.contains('\0') {
        return Err(invalid(format!("invalid database name {db:?}")));
    }
    Ok(db)
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub(crate) fn url_encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// Appends one line to the build log, with any URL password masked. Best effort.
fn append_log(log: &Path, text: &str) {
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(log) {
        let _ = writeln!(f, "{}", redact_url(text));
    }
}

fn log_tail(log: &Path, n: usize) -> String {
    let bytes = std::fs::read(log).unwrap_or_default();
    let text = String::from_utf8_lossy(&bytes);
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

/// One build per template name at a time; released on drop.
pub(crate) struct BuildGuard<'a> {
    ctx: &'a Ctx,
    name: String,
}

impl<'a> BuildGuard<'a> {
    pub(crate) fn take(ctx: &'a Ctx, name: &str) -> Result<BuildGuard<'a>> {
        if !ctx
            .building
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.to_string())
        {
            return Err(invalid(format!("template {name} is already being built")));
        }
        Ok(BuildGuard {
            ctx,
            name: name.to_string(),
        })
    }
}

impl Drop for BuildGuard<'_> {
    fn drop(&mut self) {
        self.ctx
            .building
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.name);
    }
}

/// Takes the build slot of `name` for a `background` build; false when a build of it runs.
pub(crate) fn claim_build(ctx: &Ctx, name: &str) -> bool {
    ctx.building
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(name.to_string())
}

/// True while a build of `name` runs.
pub(crate) fn is_building(ctx: &Ctx, name: &str) -> bool {
    ctx.building
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(name)
}

/// True when `name` has a masking file its current generation was not built with (or that
/// cannot be read): only a full build may make its next generation.
fn mask_stale(ctx: &Ctx, name: &str) -> Result<bool> {
    let Some((t, cur)) = ctx.store().current(name)? else {
        return Ok(false);
    };
    Ok(t.mask_path
        .is_some_and(|p| MaskFile::hash(&p).ok() != cur.mask_hash))
}

/// A scheduled or stale-triggered build of `name` in `mode`. The caller took the build slot with
/// `claim_build`; it is released when this returns. Never clears `paused`. The outcome is stored
/// before the slot is released, so no new attempt can start without seeing it: `last_attempt_at`
/// is set to the start; a failure stores its scrubbed message in `last_error`, a success clears it.
pub(crate) async fn background(ctx: &Ctx, name: &str, mode: RefreshMode) {
    // Adopts the slot the caller claimed.
    let _guard = BuildGuard {
        ctx,
        name: name.to_string(),
    };
    let started = now();
    // The base follows only its own branch: a layered build from another checkout would pollute it.
    if mode == RefreshMode::Layered {
        match off_base_branch(ctx, name).await {
            Ok(Some((base_branch, current))) => {
                return emit_skipped(ctx, name, &base_branch, &current);
            }
            Ok(None) => {}
            Err(e) => crate::lifecycle::log(ctx, &format!("template {name}: branch check: {e}")),
        }
    }
    // A layered build would be refused while the masking file is newer than the current
    // generation; an automatic build does the full build that re-bases it instead.
    let mode = match mode {
        RefreshMode::Layered if mask_stale(ctx, name).unwrap_or(false) => RefreshMode::Full,
        m => m,
    };
    let res = match mode {
        RefreshMode::Full => match refresh_args(ctx, name, None) {
            Ok(args) => build_held(ctx, args, false).await,
            Err(e) => Err(e),
        },
        RefreshMode::Layered => layered_held(ctx, name, None, false).await,
    };
    let store = ctx.store();
    let mut t = match store.get_template(name) {
        Ok(Some(t)) => t,
        Ok(None) => return,
        Err(e) => {
            drop(store);
            return crate::lifecycle::log(
                ctx,
                &format!("template {name}: read after refresh: {e}"),
            );
        }
    };
    t.last_attempt_at = Some(started);
    t.last_error = res.as_ref().err().map(|e| {
        // The first line names the failed step; the hint points at the full log.
        let first = e.message.lines().next().unwrap_or_default();
        let msg = match &e.hint {
            Some(h) => format!("{first} ({h})"),
            None => first.to_string(),
        };
        scrub(&msg, &source_secrets(&t.source))
    });
    let saved = store.update_template_settings(&t);
    drop(store);
    let mode = serde_json::to_value(mode)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default();
    let outcome = match (&res, &t.last_error) {
        (Ok(i), _) => format!("done, current generation {}", i.current_gen),
        (Err(_), Some(m)) => format!("failed: {m}"),
        (Err(_), None) => "failed".into(),
    };
    crate::lifecycle::log(
        ctx,
        &format!("background {mode} refresh of template {name}: {outcome}"),
    );
    if let Err(e) = saved {
        crate::lifecycle::log(ctx, &format!("template {name}: store refresh result: {e}"));
    }
}

type StepResult<T> = std::result::Result<T, (&'static str, MitosisError)>;

fn at(step: &'static str) -> impl FnOnce(MitosisError) -> (&'static str, MitosisError) {
    move |e| (step, e)
}

async fn run_psql(
    install: &PgInstall,
    opts: &StartOptions,
    db: &str,
    sql: String,
) -> Result<String> {
    let (i, s, p, d) = (
        install.clone(),
        opts.socket_dir.clone(),
        opts.port,
        db.to_string(),
    );
    blocking(move || psql(&i, &s, p, &d, &sql)).await
}

type Groups = Arc<Mutex<HashSet<i32>>>;

/// Lists a build tool's process group in `Ctx::build_groups` until dropped, so daemon shutdown
/// can kill it; `kill` ends whatever the tool left running in its group.
struct GroupGuard {
    groups: Groups,
    pgid: i32,
}

impl GroupGuard {
    fn new(groups: &Groups, pgid: i32) -> GroupGuard {
        groups
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(pgid);
        GroupGuard {
            groups: groups.clone(),
            pgid,
        }
    }

    fn kill(&self) {
        let _ = killpg(Pid::from_raw(self.pgid), Signal::SIGKILL);
    }
}

impl Drop for GroupGuard {
    fn drop(&mut self) {
        self.groups
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.pgid);
    }
}

/// Kills every running seed command and dump/restore; the daemon calls it as it shuts down.
pub(crate) fn kill_build_groups(ctx: &Ctx) {
    let groups: Vec<i32> = ctx
        .build_groups
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .drain()
        .collect();
    for pgid in groups.into_iter().filter(|p| *p > 1) {
        let _ = killpg(Pid::from_raw(pgid), Signal::SIGKILL);
    }
}

/// `pg_dump -Fc <url> | pg_restore` into the build server, both stderrs to the log.
/// The password goes in `PGPASSWORD`, never in argv, so neither `ps` nor libpq errors show it.
/// Without one in the URL, pg_dump reads `~/.pgpass` or `PGPASSWORD` from the daemon's environment.
fn dump_restore(
    install: &PgInstall,
    url: &str,
    opts: &StartOptions,
    db: &str,
    groups: &Groups,
) -> Result<()> {
    let split = split_password(url)?;
    let log = || OpenOptions::new().append(true).open(&opts.log_file);
    let (dump_err, restore_out, restore_err) = (log()?, log()?, log()?);
    let mut dump = Command::new(install.bin("pg_dump"));
    if let Some(pw) = &split.password {
        dump.env("PGPASSWORD", pw);
    }
    let mut dump = dump
        .args(["-Fc", "--no-owner", "--no-acl"])
        .arg(&split.url)
        .env("PGCONNECT_TIMEOUT", "10")
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(dump_err)
        .spawn()
        .map_err(|e| internal(format!("could not run pg_dump: {e}")))?;
    let _dump_group = GroupGuard::new(groups, dump.id() as i32);
    let dump_out = dump.stdout.take().expect("pg_dump stdout is piped");
    // If pg_restore cannot start, the pipe closes and pg_dump exits, so the wait below cannot hang.
    let restore = Command::new(install.bin("pg_restore"))
        .args(["--no-owner", "--no-acl", "-h"])
        .arg(&opts.socket_dir)
        .args(["-p", &opts.port.to_string(), "-U", "postgres", "-d", db])
        .process_group(0)
        .stdin(dump_out)
        .stdout(restore_out)
        .stderr(restore_err)
        .spawn()
        .map_err(|e| internal(format!("could not run pg_restore: {e}")));
    let restore = restore.and_then(|mut child| {
        let _restore_group = GroupGuard::new(groups, child.id() as i32);
        child
            .wait()
            .map_err(|e| internal(format!("waiting for pg_restore: {e}")))
    });
    let dump_status = dump
        .wait()
        .map_err(|e| internal(format!("waiting for pg_dump: {e}")))?;
    let restore_status = restore?;
    let mut failed = Vec::new();
    if !dump_status.success() {
        failed.push(format!("pg_dump failed ({dump_status})"));
    }
    if !restore_status.success() {
        failed.push(format!("pg_restore failed ({restore_status})"));
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(internal(failed.join("; ")))
    }
}

/// Runs a seed or migrate command (`what`) against the build server over TCP, in its own
/// process group.
#[allow(clippy::too_many_arguments)]
async fn run_command(
    install: &PgInstall,
    opts: &StartOptions,
    db: &str,
    command: &str,
    cwd: Option<&Path>,
    path: Option<&str>,
    groups: &Groups,
    what: &str,
) -> Result<()> {
    // initdb sets host auth to scram-sha-256, so TCP needs a password: a throwaway one, cleared afterwards.
    // log_min_error_statement=panic keeps a failing statement (with the password) out of the log.
    let password = random_hex(16);
    let sql = format!(
        "SET log_min_error_statement = panic; SET password_encryption = 'scram-sha-256'; \
         ALTER ROLE postgres PASSWORD '{password}'"
    );
    run_psql(install, opts, "postgres", sql).await?;
    let url = format!(
        "postgres://postgres:{password}@127.0.0.1:{}/{}",
        opts.port,
        url_encode(db)
    );
    let log = || OpenOptions::new().append(true).open(&opts.log_file);
    // An absolute shell, so a caller's PATH without /bin still runs it.
    let mut cmd = tokio::process::Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(command)
        .env("DATABASE_URL", &url)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(log()?)
        .stderr(log()?);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    if let Some(p) = path {
        cmd.env("PATH", p);
    }
    let spawn_err = |e: std::io::Error| match cwd {
        Some(dir) => internal(format!(
            "could not run the {what} command in {}: {e}",
            dir.display()
        )),
        None => internal(format!("could not run the {what} command: {e}")),
    };
    let mut child = cmd.spawn().map_err(spawn_err)?;
    let group = child.id().map(|pid| GroupGuard::new(groups, pid as i32));
    let status = child.wait().await.map_err(spawn_err)?;
    if !status.success() {
        // Background jobs the seed left behind would otherwise outlive the build.
        if let Some(g) = &group {
            g.kill();
        }
        return Err(internal(format!("{what} command failed ({status})")));
    }
    drop(group);
    run_psql(
        install,
        opts,
        "postgres",
        "ALTER ROLE postgres PASSWORD NULL".into(),
    )
    .await?;
    Ok(())
}

/// Steps 3-6: initdb, start, create database, restore or seed, extensions, stop.
// Named by port, not template name, to stay under the Unix socket path limit.
fn build_socket_dir(ctx: &Ctx, port: u16) -> PathBuf {
    ctx.home.sockets_dir().join(format!("build-{port}"))
}

/// True for the names `build_socket_dir` makes; gc touches no other non-clone name in `sockets/`.
pub(crate) fn is_build_socket_name(name: &str) -> bool {
    name.strip_prefix("build-").is_some_and(|p| {
        !p.is_empty() && p.parse::<u16>().is_ok() && p.bytes().all(|b| b.is_ascii_digit())
    })
}

/// A leftover folder `build` makes in `templates/`: `.<name>.building-<ts>` or `.<name>.old-<ts>`.
/// The string is the template's `folder_stem`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Leftover {
    Building(String),
    Old(String, i64),
}

pub(crate) fn build_dir_name(name: &str, ts: i64) -> String {
    format!(".{}.building-{ts}", folder_stem(name))
}

fn old_dir_name(name: &str, ts: i64) -> String {
    format!(".{}.old-{ts}", folder_stem(name))
}

/// The build log of template `name`; a branch template's name has `/`-bearing parts, so the
/// file is named by its folder stem.
pub(crate) fn build_log_path(home: &Home, name: &str) -> PathBuf {
    home.logs_dir()
        .join(format!("template-{}.log", folder_stem(name)))
}

/// The folder of generation `gen` of template `name`, in `templates/`.
fn gen_dir_name(name: &str, gen: u32) -> String {
    format!("{}@{gen}", folder_stem(name))
}

/// The name of the template built for git branch `branch` of template `base`.
pub fn branch_template_name(base: &str, branch: &str) -> String {
    format!("{base}~{branch}")
}

/// What stands for a branch template in folder names: `base`, `~`, the branch as a lowercase
/// ASCII slug (any other character becomes `-`, at most 40) and `-` plus 8 hex of its SHA-256.
pub fn branch_folder_slug(base: &str, branch: &str) -> String {
    let slug: String = branch
        .chars()
        .map(|c| match c.to_ascii_lowercase() {
            c @ ('a'..='z' | '0'..='9' | '-') => c,
            _ => '-',
        })
        .take(40)
        .collect();
    let hash: String = Sha256::digest(branch.as_bytes())
        .iter()
        .take(4)
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{base}~{slug}-{hash}")
}

/// What stands for template `name` in folder and log names: the name, or a branch template's slug.
pub(crate) fn folder_stem(name: &str) -> String {
    match name.split_once('~') {
        Some((base, branch)) => branch_folder_slug(base, branch),
        None => name.to_string(),
    }
}

/// True for the names `folder_stem` makes.
fn valid_stem(stem: &str) -> bool {
    match stem.split_once('~') {
        Some((base, slug)) => {
            validate_name("template", base).is_ok()
                && (10..=49).contains(&slug.len())
                && slug
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        }
        None => validate_name("template", stem).is_ok(),
    }
}

/// A template name a read request may name: a plain name, or a branch template `<base>~<branch>`.
pub fn validate_template_ref(name: &str) -> Result<()> {
    match name.split_once('~') {
        Some((base, branch)) => {
            validate_name("template", base)?;
            check_branch_name(branch)
        }
        None => validate_name("template", name),
    }
}

/// A git branch name `clone --branch` accepts.
fn check_branch_name(branch: &str) -> Result<()> {
    if branch.is_empty()
        || branch.len() > 255
        || branch.contains('~')
        || branch.chars().any(char::is_control)
    {
        return Err(invalid(format!("invalid branch name {branch:?}")));
    }
    Ok(())
}

/// Parses the names `gen_dir_name` makes; anything else is `None`.
pub(crate) fn parse_gen_dir(file_name: &str) -> Option<(String, u32)> {
    let (name, gen) = file_name.split_once('@')?;
    valid_stem(name).then_some(())?;
    if gen.is_empty() || !gen.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((name.into(), gen.parse().ok()?))
}

/// Parses the names `build_dir_name` and `old_dir_name` make; anything else is `None`.
pub(crate) fn parse_leftover(file_name: &str) -> Option<Leftover> {
    let (name, suffix) = file_name.strip_prefix('.')?.split_once('.')?;
    valid_stem(name).then_some(())?;
    let ts = |s: &str| {
        s.bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| s.parse::<i64>().ok())
            .flatten()
    };
    if let Some(t) = suffix.strip_prefix("building-") {
        ts(t).map(|_| Leftover::Building(name.into()))
    } else {
        ts(suffix.strip_prefix("old-")?).map(|t| Leftover::Old(name.into(), t))
    }
}

async fn run_steps(
    ctx: &Ctx,
    install: &PgInstall,
    data_dir: &Path,
    opts: &mut StartOptions,
    database: &str,
    source: &TemplateSource,
    mask: Option<&Masking>,
) -> StepResult<Vec<String>> {
    let pg = if let TemplateSource::Basebackup { url } = source {
        basebackup_server(ctx, install, url, data_dir, opts, database).await?
    } else {
        let (i, d) = (install.clone(), data_dir.to_path_buf());
        blocking(move || initdb(&i, &d))
            .await
            .map_err(at("initdb"))?;
        let pg = start_build_server(ctx, install, data_dir, opts).await?;

        if database != "postgres" {
            let sql = format!("CREATE DATABASE {}", quote_ident(database));
            run_psql(install, opts, "postgres", sql)
                .await
                .map_err(at("create database"))?;
        }
        pg
    };

    match source {
        // Already in the copy.
        TemplateSource::Basebackup { .. } => {}
        TemplateSource::Url { url } => {
            let (i, u, o, db, g) = (
                install.clone(),
                url.clone(),
                opts.clone(),
                database.to_string(),
                ctx.build_groups.clone(),
            );
            blocking(move || dump_restore(&i, &u, &o, &db, &g))
                .await
                .map_err(at("restore"))?;
        }
        TemplateSource::Seed { command, cwd, path } => {
            run_command(
                install,
                opts,
                database,
                command,
                cwd.as_deref(),
                path.as_deref(),
                &ctx.build_groups,
                "seed",
            )
            .await
            .map_err(at("seed"))?;
        }
        TemplateSource::Promoted { .. } => {
            return Err((
                "seed",
                invalid("a promoted template has no source to rebuild"),
            ));
        }
    }
    if let Some(m) = mask {
        mask_and_dump(ctx, install, data_dir, opts, database, m).await?;
    }
    extensions_and_stop(ctx, install, data_dir, opts, database, pg).await
}

/// Step name used only for "the source has no such database"; `build_gen` turns that
/// `invalid_request` into a plain bad request instead of a failed build.
const MISSING_DATABASE_STEP: &str = "missing database";

/// The major version of the server at `url`, read with `psql` of any install.
fn source_major(install: &PgInstall, url: &str) -> Result<u32> {
    let split = split_password(url)?;
    let mut cmd = Command::new(install.bin("psql"));
    if let Some(pw) = &split.password {
        cmd.env("PGPASSWORD", pw);
    }
    let out = cmd
        .args(["-X", "-A", "-t", "-w", "-c", "show server_version_num"])
        .arg(&split.url)
        .env("PGCONNECT_TIMEOUT", "10")
        .stdin(Stdio::null())
        .output()
        .map_err(|e| internal(format!("could not run psql: {e}")))?;
    let secrets = source_secrets(&TemplateSource::Basebackup { url: url.into() });
    if !out.status.success() {
        let err = scrub(String::from_utf8_lossy(&out.stderr).trim(), &secrets);
        return Err(MitosisError::new(
            ErrorCode::TemplateBuildFailed,
            format!("could not read the version of the source server: {err}"),
        )
        .with_hint("check the URL and password, and that the server accepts this connection"));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    text.trim()
        .parse::<u32>()
        .map(|n| n / 10_000)
        .map_err(|_| internal(format!("unexpected server_version_num {:?}", text.trim())))
}

/// The install whose `pg_basebackup` matches the major of the source at `url`. Fails before any
/// copy when that major is not installed.
async fn basebackup_install(ctx: &Ctx, url: &str, pinned: Option<u32>) -> Result<PgInstall> {
    let probe = ctx.newest_install().await?;
    let u = url.to_string();
    let major = blocking(move || source_major(&probe, &u)).await?;
    if let Some(m) = pinned.filter(|m| *m != major) {
        return Err(invalid(format!(
            "the source runs Postgres {major}, so its physical copy is Postgres {major}, not {m}"
        ))
        .with_hint("drop --pg-version; to move to another major, upgrade the template"));
    }
    ctx.install_major(major).await.map_err(|e| {
        if e.code != ErrorCode::NoMatchingPostgres {
            return e;
        }
        MitosisError::new(
            ErrorCode::NoMatchingPostgres,
            format!("the source runs Postgres {major} and pg_basebackup needs the same major; no Postgres {major} found"),
        )
        .with_hint(format!(
            "brew install postgresql@{major}, or: mitosis pg install {major}"
        ))
    })
}

/// True when `pg_basebackup`'s error says the source refused a replication connection.
fn refused_replication(stderr: &str) -> bool {
    let s = stderr.to_ascii_lowercase();
    s.contains("replication") || s.contains("walsender") || s.contains("wal sender")
}

const REPLICATION_NOTE: &str =
    "mitosis: the source refused the physical copy. The role in the URL \
    needs the REPLICATION attribute (ALTER ROLE <role> REPLICATION), and the source's pg_hba.conf \
    needs a 'replication' line that allows it (host replication <role> <address> scram-sha-256).";

/// `pg_basebackup -D dir -X stream -c fast` of the server at `url`, stderr to the build log. The
/// password goes in `PGPASSWORD`, never in argv. The tool creates `dir`, which must not exist.
fn run_basebackup(
    install: &PgInstall,
    url: &str,
    dir: &Path,
    log: &Path,
    groups: &Groups,
) -> Result<()> {
    let split = split_password(url)?;
    let mut cmd = Command::new(install.bin("pg_basebackup"));
    if let Some(pw) = &split.password {
        cmd.env("PGPASSWORD", pw);
    }
    let child = cmd
        .args(["-X", "stream", "-c", "fast", "-w", "-D"])
        .arg(dir)
        .arg("-d")
        .arg(&split.url)
        .env("PGCONNECT_TIMEOUT", "10")
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| internal(format!("could not run pg_basebackup: {e}")))?;
    let _group = GroupGuard::new(groups, child.id() as i32);
    let out = child
        .wait_with_output()
        .map_err(|e| internal(format!("waiting for pg_basebackup: {e}")))?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    if !stderr.trim().is_empty() {
        append_log(log, stderr.trim());
    }
    if out.status.success() {
        return Ok(());
    }
    if refused_replication(&stderr) {
        append_log(log, REPLICATION_NOTE);
    }
    Err(internal(format!("pg_basebackup failed ({})", out.status)))
}

/// Ensures the `postgres` superuser, then starts the copied cluster. A `shared_preload_libraries`
/// entry this machine lacks is commented out once, with a warning in the build log.
async fn standalone_start(
    ctx: &Ctx,
    install: &PgInstall,
    data_dir: &Path,
    opts: &mut StartOptions,
) -> StepResult<RunningPg> {
    let mut retried = false;
    loop {
        let (i, d) = (install.clone(), data_dir.to_path_buf());
        let attempt = match blocking(move || ensure_postgres_superuser(&i, &d)).await {
            Ok(()) => start_build_server(ctx, install, data_dir, opts).await,
            Err(e) => Err(("standalone", e)),
        };
        let (step, e) = match attempt {
            Ok(pg) => return Ok(pg),
            Err(failed) => failed,
        };
        if retried || !is_missing_library(&e.message) {
            return Err((step, e));
        }
        let d = data_dir.to_path_buf();
        let off = blocking(move || disable_preload_libraries(&d))
            .await
            .map_err(at("standalone"))?;
        if off.is_empty() {
            return Err((step, e));
        }
        for line in off {
            append_log(
                &opts.log_file,
                &format!("mitosis: warning: the copy needs a library this Postgres lacks; disabled: {line}"),
            );
        }
        retried = true;
    }
}

/// The copy step of a `pg_basebackup` template: copies the source into `data_dir`, makes it a
/// standalone cluster, starts it, and checks that `database` is in it. The server is left running.
async fn basebackup_server(
    ctx: &Ctx,
    install: &PgInstall,
    url: &str,
    data_dir: &Path,
    opts: &mut StartOptions,
    database: &str,
) -> StepResult<RunningPg> {
    let (i, u, d, l, g) = (
        install.clone(),
        url.to_string(),
        data_dir.to_path_buf(),
        opts.log_file.clone(),
        ctx.build_groups.clone(),
    );
    blocking(move || run_basebackup(&i, &u, &d, &l, &g))
        .await
        .map_err(at("basebackup"))?;
    let d = data_dir.to_path_buf();
    let warnings = blocking(move || make_standalone(&d))
        .await
        .map_err(at("standalone"))?;
    for w in warnings {
        append_log(&opts.log_file, &format!("mitosis: warning: {w}"));
    }
    let pg = standalone_start(ctx, install, data_dir, opts).await?;
    // template1 always exists and accepts connections.
    let out = run_psql(
        install,
        opts,
        "template1",
        "SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY 1"
            .into(),
    )
    .await
    .map_err(at("list databases"))?;
    let have: Vec<&str> = out
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    if !have.contains(&database) {
        return Err((
            MISSING_DATABASE_STEP,
            invalid(format!(
                "database {database} does not exist in the source (it has: {})",
                have.join(", ")
            ))
            .with_hint("name one of them with --database"),
        ));
    }
    Ok(pg)
}

/// Starts the build server in `data_dir`, moving `opts` to another port when the picked one is taken.
async fn start_build_server(
    ctx: &Ctx,
    install: &PgInstall,
    data_dir: &Path,
    opts: &mut StartOptions,
) -> StepResult<RunningPg> {
    let mut tried = HashSet::new();
    Ok(loop {
        let pg = start(install, data_dir, opts).map_err(at("start"))?;
        match wait_ready(install, &pg, opts, ctx.config().ready_timeout).await {
            Ok(()) => break pg,
            // Another process can take the picked port before postgres binds it.
            Err(e) if e.code == ErrorCode::PortInUse && tried.len() + 1 < START_TRIES => {
                let (i, d, t) = (
                    install.clone(),
                    data_dir.to_path_buf(),
                    ctx.config().stop_timeout,
                );
                blocking(move || stop(&i, &d, Some(pg), t))
                    .await
                    .map_err(at("start"))?;
                let _ = std::fs::remove_dir(&opts.socket_dir);
                tried.insert(opts.port);
                opts.port = build_port(ctx, &tried).map_err(at("start"))?;
                opts.socket_dir = build_socket_dir(ctx, opts.port);
            }
            Err(e) => return Err(("start", e)),
        }
    })
}

/// Lists the extensions of `database`, then stops the build server.
async fn extensions_and_stop(
    ctx: &Ctx,
    install: &PgInstall,
    data_dir: &Path,
    opts: &StartOptions,
    database: &str,
    pg: RunningPg,
) -> StepResult<Vec<String>> {
    let out = run_psql(
        install,
        opts,
        database,
        "SELECT extname FROM pg_extension ORDER BY extname".into(),
    )
    .await
    .map_err(at("extensions"))?;
    let extensions = out
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect();

    let (i, d, t) = (
        install.clone(),
        data_dir.to_path_buf(),
        ctx.config().stop_timeout,
    );
    blocking(move || stop(&i, &d, Some(pg), t))
        .await
        .map_err(at("stop"))?;
    Ok(extensions)
}

/// Stops any server in `dir` and deletes the folder. A server that would not stop keeps its
/// folder; gc deals with it later.
fn stop_and_remove(install: &PgInstall, dir: &Path, timeout: Duration) -> Result<()> {
    stop(install, dir, None, timeout)?;
    match std::fs::remove_dir_all(dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

/// Stops the build server, deletes the build folder, and turns `e` into `TemplateBuildFailed`.
async fn fail(
    ctx: &Ctx,
    install: &PgInstall,
    build_dir: &Path,
    log: &Path,
    secrets: &[String],
    step: &str,
    e: MitosisError,
) -> MitosisError {
    append_log(
        log,
        &scrub(&format!("mitosis: {step} failed: {e}"), secrets),
    );
    // Read the tail before stopping, so shutdown lines cannot push the cause out of it.
    let tail = scrub(&log_tail(log, 20), secrets);
    let (i, d, t) = (
        install.clone(),
        build_dir.to_path_buf(),
        ctx.config().stop_timeout,
    );
    let cleanup = blocking(move || stop_and_remove(&i, &d, t)).await;
    if let Err(ce) = cleanup {
        append_log(
            log,
            &scrub(
                &format!("mitosis: cleanup of {} failed: {ce}", build_dir.display()),
                secrets,
            ),
        );
    }
    MitosisError::new(
        ErrorCode::TemplateBuildFailed,
        format!("template build failed at {step}\n{tail}"),
    )
    .with_hint(format!("full log: {}", log.display()))
}

/// A manual build of `args.name`: takes its build slot first.
async fn build(ctx: &Ctx, args: TemplateInitArgs) -> Result<TemplateInfo> {
    let _guard = BuildGuard::take(ctx, &args.name)?;
    build_held(ctx, args, true).await
}

/// `build` for a caller that holds the build slot. Only a `manual` build clears `paused`.
async fn build_held(ctx: &Ctx, args: TemplateInitArgs, manual: bool) -> Result<TemplateInfo> {
    let name = args.name.clone();
    let database = database_for(&args)?;
    let install = match (&args.source, args.pg_major) {
        // A physical copy keeps the major of the server it is copied from.
        (TemplateSource::Basebackup { url }, pinned) => {
            basebackup_install(ctx, url, pinned).await?
        }
        (_, Some(major)) => ctx.install_major(major).await?,
        (_, None) => ctx.newest_install().await?,
    };
    let migrations_hash = match args.migrations.as_deref() {
        Some(m) if m.is_dir() => Some(hash_dir(m)?),
        _ => None,
    };
    // The masking file this build uses: the one given now, else the recorded one.
    let mask_path = match (&args.settings.mask, args.settings.mask_off) {
        (_, true) => None,
        (Some(p), false) => Some(p.clone()),
        (None, false) => ctx.store().get_template(&name)?.and_then(|t| t.mask_path),
    };
    let mask_file = match mask_path.as_deref() {
        Some(p) => Some((MaskFile::load(p)?, MaskFile::hash(p)?)),
        None => None,
    };
    // The commit expects the pointer read here; a new template starts at 0 (no generation).
    let (created, expected, gen, masking) = {
        let store = ctx.store();
        let (created, expected) = match store.get_template(&name)? {
            Some(t) => (false, t.current_gen),
            None => {
                store.insert_template(&TemplateRow::new(
                    &name,
                    &database,
                    args.source.clone(),
                    args.migrations.clone(),
                    args.max_age_secs
                        .unwrap_or(ctx.config().default_max_age_secs),
                ))?;
                (true, 0)
            }
        };
        let masking = match mask_file {
            Some((file, hash)) => Some(Masking {
                file,
                hash,
                salt: store.ensure_mask_salt(&name, &random_hex(32))?,
            }),
            None => None,
        };
        (created, expected, store.alloc_gen(&name)?, masking)
    };
    let built = build_gen(
        ctx,
        &install,
        &args,
        &database,
        migrations_hash,
        masking.as_ref(),
        gen,
        expected,
    )
    .await;
    if let Err(e) = &built {
        events::emit_template_failed(ctx, &name, gen, "build", e);
    }
    let store = ctx.store();
    if let Err(e) = built {
        // A failed first build leaves no template behind.
        if created && matches!(store.list_generations(&name), Ok(g) if g.is_empty()) {
            let _ = store.delete_template(&name);
        }
        return Err(e);
    }
    // Re-read under the store lock; only what this build owns is written. `template set` may
    // have changed any setting meanwhile; the max age changes here only when the caller gave one.
    let mut t = store.get_template(&name)?.ok_or_else(|| not_found(&name))?;
    t.database = database;
    t.source = args.source;
    if t.migrations != args.migrations {
        clear_repo_paths(&mut t);
    }
    t.migrations = args.migrations;
    if let Some(m) = args.max_age_secs {
        t.max_age_secs = m;
    }
    apply_settings(&mut t, None, &args.settings)?;
    if manual {
        resume_if_current(&mut t, gen);
    }
    store.update_template_settings(&t)?;
    drop(store);
    template_info(ctx, &name)
}

/// A successful build that is still current clears `paused` and `last_error`. One that a
/// rollback overtook leaves them, so the rollback sticks.
pub(crate) fn resume_if_current(t: &mut TemplateRow, gen: u32) {
    if t.current_gen == gen {
        t.paused = false;
        t.last_error = None;
    }
}

/// Copies the current generation of `name`, runs only its migrate command on the copy and makes
/// the result a `Layered` generation. `path`, when given, replaces the command's recorded `PATH`.
/// Refused while the template's recorded checkout is on another branch than its `base_branch`.
pub async fn layered(ctx: &Ctx, name: &str, path: Option<String>) -> Result<TemplateInfo> {
    validate_name("template", name)?;
    if let Some((base_branch, current)) = off_base_branch(ctx, name).await? {
        return Err(invalid(format!(
            "the checkout of template {name} is on {}, not on its base branch {base_branch}",
            branch_label(&current)
        ))
        .with_hint(format!(
            "for this branch use: mitosis clone --branch; to refresh {name}: git switch {base_branch}"
        )));
    }
    let _guard = BuildGuard::take(ctx, name)?;
    layered_held(ctx, name, path, true).await
}

/// `branch X` for a branch name, `a detached HEAD` for the empty one.
fn branch_label(branch: &str) -> String {
    if branch.is_empty() {
        "a detached HEAD".into()
    } else {
        format!("branch {branch}")
    }
}

/// `(base_branch, checked out branch)` when `name` has a `base_branch` and its migrate folder's
/// checkout is on another branch (`""` for a detached HEAD); `None` otherwise, also when git
/// cannot tell.
async fn off_base_branch(ctx: &Ctx, name: &str) -> Result<Option<(String, String)>> {
    let Some(t) = ctx.store().get_template(name)? else {
        return Ok(None);
    };
    let (Some(base_branch), Some(m)) = (t.base_branch, t.migrate) else {
        return Ok(None);
    };
    let current = blocking(move || Ok(git(&m.cwd, &["branch", "--show-current"]))).await?;
    Ok(current
        .filter(|c| *c != base_branch)
        .map(|c| (base_branch, c)))
}

/// `template.skipped` for a layered build of `name` skipped because its checkout is on `current`;
/// not again while the newest event of `name` already says so.
fn emit_skipped(ctx: &Ctx, name: &str, base_branch: &str, current: &str) {
    let filter = EventFilter {
        template: Some(name.to_string()),
        limit: Some(1),
        ..Default::default()
    };
    let last = ctx.store().list_events(&filter).unwrap_or_default();
    if last.last().is_some_and(|e| {
        e.kind == events::TEMPLATE_SKIPPED
            && e.detail.get("branch").and_then(|b| b.as_str()) == Some(current)
    }) {
        return;
    }
    events::emit(
        ctx,
        events::TEMPLATE_SKIPPED,
        None,
        Some(name),
        serde_json::json!({
            "op": "layered",
            "reason": "checkout on another branch",
            "branch": current,
            "base_branch": base_branch,
        }),
    );
}

/// `layered` for a caller that holds the build slot. Only a `manual` build clears `paused`.
async fn layered_held(
    ctx: &Ctx,
    name: &str,
    path: Option<String>,
    manual: bool,
) -> Result<TemplateInfo> {
    let (t, cur) = ctx.store().current(name)?.ok_or_else(|| not_found(name))?;
    let Some(mut migrate) = t.migrate.clone() else {
        return Err(
            invalid(format!("template {name} has no migrate command")).with_hint(format!(
                "set one with: mitosis template set {name} --migrate \"…\""
            )),
        );
    };
    migrate.path = path.or(migrate.path);
    let gen = build_layer(
        ctx,
        Layer {
            target: name,
            src: &t,
            from: &cur,
            migrate,
            migrations: t.migrations.clone(),
            expected: cur.gen,
        },
    )
    .await?;
    if manual {
        let store = ctx.store();
        let mut t = store.get_template(name)?.ok_or_else(|| not_found(name))?;
        resume_if_current(&mut t, gen);
        store.update_template_settings(&t)?;
    }
    template_info(ctx, name)
}

/// A layered build: generation `from` of template `src` copied, `migrate` run on the copy (and
/// `src`'s masking applied again), committed as the next generation of `target` when its current
/// pointer is still `expected`. `target` is `src` itself, or a branch template of it.
struct Layer<'a> {
    target: &'a str,
    src: &'a TemplateRow,
    from: &'a GenerationRow,
    migrate: MigrateCmd,
    /// Hashed into the new generation, for freshness checks.
    migrations: Option<PathBuf>,
    expected: u32,
}

/// Runs `l`; returns the new generation's number. The caller holds the build slot of `l.target`.
async fn build_layer(ctx: &Ctx, l: Layer<'_>) -> Result<u32> {
    let (name, t, cur) = (l.target, l.src, l.from);
    // Masking in place covers only what the migration adds: the copied pages must already be
    // masked with this very file, or raw values of newly masked columns would stay in them.
    let masking = match t.mask_path.as_deref() {
        Some(p) => {
            let hash = MaskFile::hash(p)?;
            if cur.mask_hash.as_deref() != Some(hash.as_str()) {
                return Err(invalid(format!(
                    "the current generation of template {} was not built with the masking \
                     file {} as it is now",
                    t.name,
                    p.display()
                ))
                .with_hint(format!(
                    "run a full build first: mitosis template refresh {}",
                    t.name
                )));
            }
            // The source's salt, so a branch template masks values as its base does.
            Some(Masking {
                file: MaskFile::load(p)?,
                hash,
                salt: ctx.store().ensure_mask_salt(&t.name, &random_hex(32))?,
            })
        }
        None => None,
    };
    let install = ctx.install_major(cur.pg_major).await?;
    let migrations_hash = match l.migrations.as_deref() {
        Some(m) if m.is_dir() => Some(hash_dir(m)?),
        _ => None,
    };
    let gen = ctx.store().alloc_gen(name)?;

    let mut secrets = source_secrets(&t.source);
    secrets.extend(masking.as_ref().map(|m| m.salt.clone()));
    let ts = now();
    let side = ctx.home.templates_dir().join(build_dir_name(name, ts));
    let mut opts = build_start_options(ctx, name)?;
    let log = opts.log_file.clone();
    let result = layered_steps(
        ctx,
        &install,
        &cur.dir,
        &side,
        &mut opts,
        &t.database,
        &l.migrate,
        masking.as_ref(),
    )
    .await;
    let result = match result {
        Ok(ext) => Ok(ext),
        Err((step, e)) => Err(fail(ctx, &install, &side, &log, &secrets, step, e).await),
    };
    if let Err(e) = &result {
        events::emit_template_failed(ctx, name, gen, "layered", e);
    }
    let _ = std::fs::remove_dir(&opts.socket_dir); // empty once the server is stopped
    let row = GenerationRow {
        template: name.to_string(),
        gen,
        kind: GenKind::Layered,
        dir: side,
        pg_major: 0,
        bin_dir: install.bin_dir.clone(),
        extensions: result?,
        migrations_hash,
        built_at: ts,
        size_bytes: 0,
        from_clone: None,
        from_gen: Some(cur.gen),
        mask_hash: masking.map(|m| m.hash),
    };
    if let Err(e) = commit_built(ctx, &install, &log, &secrets, row, l.expected).await {
        events::emit_template_failed(ctx, name, gen, "layered", &e);
        return Err(e);
    }
    Ok(gen)
}

/// Copy of generation folder `from` into `dir`, started privately, migrated, masked again when
/// `mask` is set, stopped.
#[allow(clippy::too_many_arguments)]
async fn layered_steps(
    ctx: &Ctx,
    install: &PgInstall,
    from: &Path,
    dir: &Path,
    opts: &mut StartOptions,
    database: &str,
    migrate: &MigrateCmd,
    mask: Option<&Masking>,
) -> StepResult<Vec<String>> {
    {
        // A commit or prune moves generation folders only under the write guard.
        let _swap = ctx.template_swap.read().await;
        let (b, f, d) = (ctx.backend.clone(), from.to_path_buf(), dir.to_path_buf());
        blocking(move || b.clone_dir(&f, &d))
            .await
            .map_err(at("copy"))?;
    }
    let pg = start_build_server(ctx, install, dir, opts).await?;
    run_command(
        install,
        opts,
        database,
        &migrate.command,
        Some(&migrate.cwd),
        migrate.path.as_deref(),
        &ctx.build_groups,
        "migrate",
    )
    .await
    .map_err(at("migrate"))?;
    if let Some(m) = mask {
        apply_mask(install, opts, database, m, false)
            .await
            .map_err(at("mask"))?;
    }
    extensions_and_stop(ctx, install, dir, opts, database, pg).await
}

/// What `clone --branch` gets: the branch template to clone and how its freshness was handled.
#[derive(Debug)]
pub struct BranchUse {
    pub name: String,
    /// It was rebuilt before the clone.
    pub rebuilt: bool,
    /// It is out of date and served as it is (the base has `on_stale = refresh`).
    pub stale: bool,
    /// A rebuild of it runs in the background.
    pub refreshing: bool,
    /// For the clone's response.
    pub warnings: Vec<String>,
}

/// How long `clone --branch` waits for another create's build of the same branch template.
const BRANCH_BUILD_WAIT: Duration = Duration::from_secs(30 * 60);

/// Where a branch build runs: the caller's checkout joined with the base's repo-relative paths.
struct Checkout {
    cwd: PathBuf,
    migrations: Option<PathBuf>,
}

/// The branch template of `base` for the caller's checkout `b`: created on first use, and rebuilt
/// first when the base has another current generation than the one it was layered on, or the
/// checkout's migrations differ from the ones it was built with. With the base's
/// `on_stale = refresh`, an existing generation is served and rebuilt in the background instead.
pub async fn ensure_branch(ctx: &Arc<Ctx>, base: &str, b: &BranchArgs) -> Result<BranchUse> {
    validate_name("template", base)?;
    check_branch_name(&b.name)?;
    if !b.repo_root.is_absolute() {
        return Err(invalid(format!(
            "repository root {} is not an absolute path",
            b.repo_root.display()
        )));
    }
    let name = branch_template_name(base, &b.name);
    let mut warnings = Vec::new();
    loop {
        let (row, base_row, base_cur, checkout, warning) = branch_row(ctx, base, &name, b).await?;
        warnings.extend(warning);
        let used = |rebuilt, stale, refreshing, warnings| BranchUse {
            name: name.clone(),
            rebuilt,
            stale,
            refreshing,
            warnings,
        };
        let cur = ctx.store().current(&name)?.map(|(_, g)| g);
        let m = checkout.migrations;
        let hash =
            blocking(move || m.filter(|d| d.is_dir()).map(|d| hash_dir(&d)).transpose()).await?;
        let fresh = cur
            .as_ref()
            .is_some_and(|g| row.base_gen == Some(base_cur.gen) && g.migrations_hash == hash);
        if fresh {
            return Ok(used(false, false, false, warnings));
        }
        if cur.is_some() && base_row.on_stale == OnStale::Refresh {
            // A failed background build waits out the same back-off as a template's own.
            let start = crate::scheduler::may_retry(&row, now()) && claim_build(ctx, &name);
            if start {
                let (ctx, name, base, b) = (ctx.clone(), name.clone(), base.to_string(), b.clone());
                tokio::spawn(async move { background_branch(&ctx, &base, &name, &b).await });
            } else if let Some(e) = row
                .last_error
                .as_deref()
                .filter(|_| !is_building(ctx, &name))
            {
                warnings.push(format!(
                    "the last background rebuild of template {name} failed: {e}"
                ));
            }
            let refreshing = start || is_building(ctx, &name);
            return Ok(used(false, true, refreshing, warnings));
        }
        match BuildGuard::take(ctx, &name) {
            Ok(_guard) => {
                build_branch(ctx, base, &name, b).await?;
                return Ok(used(true, false, false, warnings));
            }
            // Another clone is building it: wait for that build, then look again.
            Err(_) => {
                let deadline = std::time::Instant::now() + BRANCH_BUILD_WAIT;
                while is_building(ctx, &name) {
                    if std::time::Instant::now() >= deadline {
                        let log = ctx
                            .home
                            .logs_dir()
                            .join(format!("template-{}.log", folder_stem(&name)));
                        return Err(invalid(format!(
                            "template {name} is still being built after {} minutes",
                            BRANCH_BUILD_WAIT.as_secs() / 60
                        ))
                        .with_hint(format!(
                            "try again later; the build log is {}",
                            log.display()
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}

/// The background rebuild `ensure_branch` started; the caller claimed the build slot of `name`.
/// Skipped when the caller's checkout is no longer on the branch. The outcome is stored in
/// `last_attempt_at` / `last_error`, which `may_retry` reads.
async fn background_branch(ctx: &Ctx, base: &str, name: &str, b: &BranchArgs) {
    // Adopts the slot claimed by the caller.
    let _guard = BuildGuard {
        ctx,
        name: name.to_string(),
    };
    let root = b.repo_root.clone();
    let current = blocking(move || Ok(git(&root, &["branch", "--show-current"])))
        .await
        .ok()
        .flatten();
    if current.as_deref() != Some(b.name.as_str()) {
        let current = current.unwrap_or_default();
        crate::lifecycle::log(
            ctx,
            &format!(
                "background build of template {name} skipped: the checkout in {} is on {}",
                b.repo_root.display(),
                branch_label(&current)
            ),
        );
        events::emit(
            ctx,
            events::TEMPLATE_SKIPPED,
            None,
            Some(name),
            serde_json::json!({
                "op": "branch",
                "reason": "checkout on another branch",
                "branch": current,
                "wanted": b.name,
            }),
        );
        return;
    }
    let started = now();
    let res = build_branch(ctx, base, name, b).await;
    let store = ctx.store();
    let Ok(Some(mut t)) = store.get_template(name) else {
        return;
    };
    t.last_attempt_at = Some(started);
    t.last_error = res.as_ref().err().map(|e| {
        let first = e.message.lines().next().unwrap_or_default();
        let msg = match &e.hint {
            Some(h) => format!("{first} ({h})"),
            None => first.to_string(),
        };
        scrub(&msg, &source_secrets(&t.source))
    });
    let saved = store.update_template_settings(&t);
    drop(store);
    if let Some(e) = &t.last_error {
        crate::lifecycle::log(
            ctx,
            &format!("background build of template {name} failed: {e}"),
        );
    }
    if let Err(e) = saved {
        crate::lifecycle::log(ctx, &format!("template {name}: store build result: {e}"));
    }
}

/// What `branch_row` returns: the branch row, the base row and its current generation, where the
/// build runs, and a warning for the clone's response.
type BranchRow = (
    TemplateRow,
    TemplateRow,
    GenerationRow,
    Checkout,
    Option<String>,
);

/// Reads the base and creates the branch template `name` when it has no row yet; marks it used.
/// Fails when the base has no migrate command or its migrate folder is not in a git work tree.
async fn branch_row(ctx: &Ctx, base: &str, name: &str, b: &BranchArgs) -> Result<BranchRow> {
    let (mut base_row, base_cur) = ctx.store().current(base)?.ok_or_else(|| not_found(base))?;
    let mut warning = None;
    let Some(migrate) = base_row.migrate.clone() else {
        return Err(invalid(format!(
            "template {base} has no migrate command, so it has no branch templates"
        ))
        .with_hint(format!(
            "set one from your git checkout: mitosis template set {base} --migrate \"…\""
        )));
    };
    if base_row.repo_rel_cwd.is_none() {
        // `template init` records the paths; a migrate command set later is looked up here.
        let (cwd, mig) = (migrate.cwd.clone(), base_row.migrations.clone());
        let paths = blocking(move || Ok(git_paths(Some(&cwd), mig.as_deref()))).await?;
        if paths.1.is_none() {
            return Err(invalid(format!(
                "the migrate command of template {base} runs in {}, which is not in a git work tree",
                migrate.cwd.display()
            ))
            .with_hint(format!(
                "--branch needs it inside your git checkout: mitosis template set {base} --migrate \"…\" from there"
            )));
        }
        // Only the paths: the branch checked out now is usually the caller's, not the base's.
        let store = ctx.store();
        if let Some(mut t) = store.get_template(base)? {
            (t.repo_rel_cwd, t.repo_rel_migrations) = (paths.1, paths.2);
            store.update_template_settings(&t)?;
            if t.base_branch.is_none() {
                warning = Some(format!(
                    "template {base} does not know its own branch; run mitosis template set \
                     {base} --base-branch <b> so it stops following other branches"
                ));
            }
            base_row = t;
        }
    }
    let checkout = Checkout {
        cwd: b
            .repo_root
            .join(base_row.repo_rel_cwd.as_deref().unwrap_or(Path::new(""))),
        migrations: base_row
            .repo_rel_migrations
            .as_ref()
            .map(|r| b.repo_root.join(r)),
    };
    if !checkout.cwd.is_dir() {
        return Err(invalid(format!(
            "{} does not exist in this checkout",
            checkout.cwd.display()
        ))
        .with_hint(format!(
            "run it in a checkout of the repository template {base} was made from"
        )));
    }
    let store = ctx.store();
    if store.get_template(name)?.is_none() {
        let mut t = TemplateRow::new(
            name,
            &base_row.database,
            base_row.source.clone(),
            checkout.migrations.clone(),
            base_row.max_age_secs,
        );
        (t.base, t.branch, t.keep) = (Some(base.into()), Some(b.name.clone()), base_row.keep);
        store.insert_template(&t)?;
    }
    let mut row = store.get_template(name)?.ok_or_else(|| not_found(name))?;
    (row.last_used, row.max_age_secs) = (Some(now()), base_row.max_age_secs);
    store.update_template_settings(&row)?;
    Ok((row, base_row, base_cur, checkout, warning))
}

/// Layers a new generation of branch template `name` on the base's current generation, running the
/// base's migrate command in the caller's checkout. A failed first build leaves no template. The
/// caller holds the build slot of `name`.
async fn build_branch(ctx: &Ctx, base: &str, name: &str, b: &BranchArgs) -> Result<()> {
    let (row, base_row, base_cur, checkout, _) = branch_row(ctx, base, name, b).await?;
    let Some(mut migrate) = base_row.migrate.clone() else {
        return Err(not_found(base));
    };
    migrate.cwd = checkout.cwd;
    migrate.path = b.path.clone().or(migrate.path);
    let built = build_layer(
        ctx,
        Layer {
            target: name,
            src: &base_row,
            from: &base_cur,
            migrate,
            migrations: checkout.migrations.clone(),
            expected: row.current_gen,
        },
    )
    .await;
    let store = ctx.store();
    let Some(mut t) = store.get_template(name)? else {
        return built.map(drop);
    };
    match built {
        Ok(gen) => {
            if t.current_gen == gen {
                (t.base_gen, t.last_error) = (Some(base_cur.gen), None);
            }
            t.migrations = checkout.migrations;
            store.update_template_settings(&t)
        }
        Err(e) => {
            if matches!(store.list_generations(name), Ok(g) if g.is_empty()) {
                let _ = store.delete_template(name);
            }
            Err(e)
        }
    }
}

/// A free port for a build server, skipping `skip`. Reserved ports too: a named clone's stable URL
/// must not lose its port to a build server.
fn build_port(ctx: &Ctx, skip: &HashSet<u16>) -> Result<u16> {
    let (leased, reserved) = {
        let store = ctx.store();
        (store.leased_ports()?, store.reserved_ports()?)
    };
    pick_port(ctx.config().port_range, &leased, skip, &reserved)
}

/// Options for a private build server of `name` on a free port. Truncates the build log, so an
/// error tail belongs to this build.
fn build_start_options(ctx: &Ctx, name: &str) -> Result<StartOptions> {
    let log = build_log_path(&ctx.home, name);
    std::fs::File::create(&log)?;
    let port = build_port(ctx, &HashSet::new())?;
    Ok(StartOptions {
        port,
        socket_dir: build_socket_dir(ctx, port),
        listen_tcp: true,
        durable: false,
        log_file: log,
    })
}

/// Builds generation `gen` in a side folder and commits it as a `Full` generation; through a
/// staging cluster when `masking` is set.
#[allow(clippy::too_many_arguments)]
async fn build_gen(
    ctx: &Ctx,
    install: &PgInstall,
    args: &TemplateInitArgs,
    database: &str,
    migrations_hash: Option<String>,
    masking: Option<&Masking>,
    gen: u32,
    expected: u32,
) -> Result<()> {
    let name = &args.name;
    let mut secrets = source_secrets(&args.source);
    secrets.extend(masking.map(|m| m.salt.clone()));
    let ts = now();
    let build_dir = ctx.home.templates_dir().join(build_dir_name(name, ts));
    let mut opts = build_start_options(ctx, name)?;
    let log = opts.log_file.clone();
    let staging = masking.map(|_| Staging::new(ctx, install, name, &build_dir));

    let result = match (masking, &staging) {
        (None, _) | (_, None) => {
            run_steps(
                ctx,
                install,
                &build_dir,
                &mut opts,
                database,
                &args.source,
                None,
            )
            .await
        }
        (Some(m), Some(st)) => {
            let source = &args.source;
            masked_steps(ctx, install, st, &build_dir, &mut opts, database, source, m).await
        }
    };
    let result = match result {
        Ok(ext) => Ok(ext),
        Err((step, e)) => {
            // The caller named a database the source does not have: a bad request, not a failed build.
            let bad_request = step == MISSING_DATABASE_STEP && e.code == ErrorCode::InvalidRequest;
            let failed = fail(ctx, install, &build_dir, &log, &secrets, step, e.clone()).await;
            Err(if bad_request { e } else { failed })
        }
    };
    // Removed only now, so `fail` read the log tail before the staging server's shutdown lines.
    if let Some(st) = staging {
        st.remove(&log).await;
    }
    let _ = std::fs::remove_dir(&opts.socket_dir); // empty once the server is stopped
    let row = GenerationRow {
        template: name.clone(),
        gen,
        kind: GenKind::Full,
        dir: build_dir,
        pg_major: 0,
        bin_dir: install.bin_dir.clone(),
        extensions: result?,
        migrations_hash,
        built_at: ts,
        size_bytes: 0,
        from_clone: None,
        from_gen: None,
        mask_hash: masking.map(|m| m.hash.clone()),
    };
    commit_built(ctx, install, &log, &secrets, row, expected).await
}

/// What a masked build needs: the masking file, its hash, and the template's salt.
struct Masking {
    file: MaskFile,
    hash: String,
    salt: String,
}

/// Every column of every user table, as JSON for `ColumnInfo`.
const COLUMNS_SQL: &str = "SELECT coalesce(json_agg(json_build_object('schema', n.nspname, \
    'table', c.relname, 'column', a.attname, 'data_type', format_type(a.atttypid, a.atttypmod), \
    'not_null', a.attnotnull)), '[]') \
    FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped \
    AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%'";

/// Every foreign key (partitions' inherited copies left out), as JSON for `ForeignKey`.
const FOREIGN_KEYS_SQL: &str = "SELECT coalesce(json_agg(json_build_object( \
    'child', format('%I.%I', cn.nspname, cc.relname), \
    'child_cols', (SELECT json_agg(quote_ident(a.attname) ORDER BY k.i) \
        FROM unnest(con.conkey) WITH ORDINALITY AS k(num, i) \
        JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.num), \
    'parent', format('%I.%I', pn.nspname, pc.relname), \
    'parent_cols', (SELECT json_agg(quote_ident(a.attname) ORDER BY k.i) \
        FROM unnest(con.confkey) WITH ORDINALITY AS k(num, i) \
        JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.num))), '[]') \
    FROM pg_constraint con \
    JOIN pg_class cc ON cc.oid = con.conrelid JOIN pg_namespace cn ON cn.oid = cc.relnamespace \
    JOIN pg_class pc ON pc.oid = con.confrelid JOIN pg_namespace pn ON pn.oid = pc.relnamespace \
    WHERE con.contype = 'f' AND con.conparentid = 0";

/// Files the staging stage leaves for the restore stage, inside the staging folder.
const GLOBALS_FILE: &str = "mitosis-globals.sql";
const DUMP_FILE: &str = "mitosis-masked.dump";

/// Runs the subset (when `subset`) and masking statements of `m` against `database`, in one
/// transaction with triggers and foreign-key checks off.
async fn apply_mask(
    install: &PgInstall,
    opts: &StartOptions,
    database: &str,
    m: &Masking,
    subset: bool,
) -> Result<()> {
    let columns: Vec<ColumnInfo> =
        serde_json::from_str(&run_psql(install, opts, database, COLUMNS_SQL.into()).await?)?;
    let mut statements = Vec::new();
    if subset && !m.file.subset.is_empty() {
        let fks: Vec<ForeignKey> = serde_json::from_str(
            &run_psql(install, opts, database, FOREIGN_KEYS_SQL.into()).await?,
        )?;
        statements = subset_statements(&m.file, &fks)?;
    }
    statements.extend(mask_statements(&m.file, &m.salt, &columns)?);
    if statements.is_empty() {
        return Ok(());
    }
    // Only the `SET LOCAL` of the salt holds it. Nothing may echo statement text: no statement
    // logging (whatever the cluster's settings say), no failing statement in the server log, no
    // `LINE` excerpt from psql, no utility tracking by pg_stat_statements.
    let sql = format!(
        "\\set VERBOSITY terse\nSET log_statement = 'none';\nSET log_min_duration_statement = -1;\n\
         SET log_min_error_statement = panic;\nSET pg_stat_statements.track_utility = off;\n\
         BEGIN;\nSET LOCAL session_replication_role = replica;\n{};\nCOMMIT;\n",
        statements.join(";\n")
    );
    run_psql(install, opts, database, sql).await.map(drop)
}

/// Runs a Postgres tool against the build server, output to the build log, in its own process group.
fn run_tool(
    install: &PgInstall,
    tool: &str,
    opts: &StartOptions,
    groups: &Groups,
    args: &[&OsStr],
) -> Result<()> {
    let log = || OpenOptions::new().append(true).open(&opts.log_file);
    let mut child = Command::new(install.bin(tool))
        .arg("-h")
        .arg(&opts.socket_dir)
        .args(["-p", &opts.port.to_string(), "-U", "postgres"])
        .args(args)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(log()?)
        .stderr(log()?)
        .spawn()
        .map_err(|e| internal(format!("could not run {tool}: {e}")))?;
    let _group = GroupGuard::new(groups, child.id() as i32);
    let status = child
        .wait()
        .map_err(|e| internal(format!("waiting for {tool}: {e}")))?;
    if !status.success() {
        return Err(internal(format!("{tool} failed ({status})")));
    }
    Ok(())
}

/// In the running staging server: subsets and masking, then the roles (without passwords) and
/// the database dumped into files in the staging folder.
async fn mask_and_dump(
    ctx: &Ctx,
    install: &PgInstall,
    staging: &Path,
    opts: &StartOptions,
    database: &str,
    m: &Masking,
) -> StepResult<()> {
    apply_mask(install, opts, database, m, true)
        .await
        .map_err(at("mask"))?;
    let (i, o, db, dir, g) = (
        install.clone(),
        opts.clone(),
        database.to_string(),
        staging.to_path_buf(),
        ctx.build_groups.clone(),
    );
    blocking(move || {
        let (globals, dump) = (dir.join(GLOBALS_FILE), dir.join(DUMP_FILE));
        let globals_args = ["--globals-only", "--no-role-passwords", "-f"].map(OsStr::new);
        run_tool(
            &i,
            "pg_dumpall",
            &o,
            &g,
            &[&globals_args[..], &[globals.as_os_str()]].concat(),
        )?;
        // --create: the database's owner, settings and grants come along. No statistics: they
        // are computed from the raw data (pg_dump 18 is the first to know the flag).
        let mut dump_args = vec!["-Fc", "--create"];
        if i.major >= 18 {
            dump_args.push("--no-statistics");
        }
        dump_args.extend(["-d", &db, "-f"]);
        let mut dump_args: Vec<&OsStr> = dump_args.into_iter().map(OsStr::new).collect();
        dump_args.push(dump.as_os_str());
        run_tool(&i, "pg_dump", &o, &g, &dump_args)
    })
    .await
    .map_err(at("dump"))
}

/// Stage two of a masked build: a fresh cluster in `dir` gets the staging roles, then the
/// staging dump, and is stopped.
async fn restore_steps(
    ctx: &Ctx,
    install: &PgInstall,
    dir: &Path,
    opts: &mut StartOptions,
    database: &str,
    staging: &Path,
) -> StepResult<()> {
    let (i, d) = (install.clone(), dir.to_path_buf());
    blocking(move || initdb(&i, &d))
        .await
        .map_err(at("initdb"))?;
    let pg = start_build_server(ctx, install, dir, opts).await?;
    let globals = std::fs::read_to_string(staging.join(GLOBALS_FILE))
        .map_err(|e| ("roles", MitosisError::from(e)))?;
    // initdb made the postgres role already, and psql stops at the first error.
    let globals: String = globals
        .lines()
        .filter(|l| *l != "CREATE ROLE postgres;")
        .map(|l| format!("{l}\n"))
        .collect();
    run_psql(install, opts, "postgres", globals)
        .await
        .map_err(at("roles"))?;
    let (i, o, f, g) = (
        install.clone(),
        opts.clone(),
        staging.join(DUMP_FILE),
        ctx.build_groups.clone(),
    );
    // -C makes the database from the dump (owner, settings, grants); `postgres` exists already.
    let create = database != "postgres";
    blocking(move || {
        let mut args = vec![OsStr::new("-d"), OsStr::new("postgres")];
        if create {
            args.push(OsStr::new("-C"));
        }
        args.push(f.as_os_str());
        run_tool(&i, "pg_restore", &o, &g, &args)
    })
    .await
    .map_err(at("restore"))?;
    let (i, d, t) = (
        install.clone(),
        dir.to_path_buf(),
        ctx.config().stop_timeout,
    );
    blocking(move || stop(&i, &d, Some(pg), t))
        .await
        .map_err(at("stop"))
}

/// The staging cluster of a masked build. It holds raw source values, so it is stopped and
/// deleted on every path: by `remove`, or from a thread if the build is dropped midway.
struct Staging {
    install: PgInstall,
    dir: PathBuf,
    timeout: Duration,
    removed: bool,
}

impl Staging {
    /// A staging folder of template `name` next to its build folder `build_dir`.
    fn new(ctx: &Ctx, install: &PgInstall, name: &str, build_dir: &Path) -> Staging {
        let mut ts = now();
        let dir = loop {
            let d = ctx.home.templates_dir().join(build_dir_name(name, ts));
            if d != build_dir && !d.exists() {
                break d;
            }
            ts += 1;
        };
        Staging {
            install: install.clone(),
            dir,
            timeout: ctx.config().stop_timeout,
            removed: false,
        }
    }

    async fn remove(mut self, log: &Path) {
        self.removed = true;
        let (i, d, t) = (self.install.clone(), self.dir.clone(), self.timeout);
        if let Err(e) = blocking(move || stop_and_remove(&i, &d, t)).await {
            append_log(
                log,
                &format!(
                    "mitosis: cleanup of staging {} failed: {e}",
                    self.dir.display()
                ),
            );
        }
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if !self.removed {
            let (i, d, t) = (self.install.clone(), self.dir.clone(), self.timeout);
            std::thread::spawn(move || stop_and_remove(&i, &d, t));
        }
    }
}

/// The two-stage build: the source is built into a staging cluster, subset and masked there,
/// and only a dump of the result is restored into a fresh cluster in `build_dir`, so the
/// generation's data files and WAL never hold raw values.
/// The caller removes `staging` afterwards, on every path.
#[allow(clippy::too_many_arguments)]
async fn masked_steps(
    ctx: &Ctx,
    install: &PgInstall,
    staging: &Staging,
    build_dir: &Path,
    opts: &mut StartOptions,
    database: &str,
    source: &TemplateSource,
    m: &Masking,
) -> StepResult<Vec<String>> {
    let extensions = run_steps(ctx, install, &staging.dir, opts, database, source, Some(m)).await?;
    restore_steps(ctx, install, build_dir, opts, database, &staging.dir).await?;
    Ok(extensions)
}

/// Step 7: measures the built side folder `row.dir` (filling `pg_major` and `size_bytes`), then
/// commits it. On failure the folder is removed and the error has the build-failure shape.
pub(crate) async fn commit_built(
    ctx: &Ctx,
    install: &PgInstall,
    log: &Path,
    secrets: &[String],
    mut row: GenerationRow,
    expected: u32,
) -> Result<()> {
    let dir = row.dir.clone();
    let b = dir.clone();
    let measured = blocking(move || Ok((read_pg_version(&b)?, apparent_size(&b)?))).await;
    (row.pg_major, row.size_bytes) = match measured {
        Ok(v) => v,
        Err(e) => return Err(fail(ctx, install, &dir, log, secrets, "measure", e).await),
    };
    if let Err(e) = commit_generation(ctx, row, Some(expected)).await {
        return Err(fail(ctx, install, &dir, log, secrets, "commit", e).await);
    }
    Ok(())
}

/// Moves the built folder `gen.dir` to `templates/<name>@<gen>`, records the generation and, when
/// the current pointer still equals `expected_current`, makes it current; then prunes.
/// True when it became current; otherwise it is kept as a non-current generation.
pub async fn commit_generation(
    ctx: &Ctx,
    mut gen: GenerationRow,
    expected_current: Option<u32>,
) -> Result<bool> {
    let side = gen.dir.clone();
    let final_dir = ctx
        .home
        .templates_dir()
        .join(gen_dir_name(&gen.template, gen.gen));
    let current = {
        // Under the write guard, gc and clone creation never see the folder without its row.
        let _swap = ctx.template_swap.write().await;
        let (s, f) = (side.clone(), final_dir.clone());
        blocking(move || Ok(std::fs::rename(&s, &f)?)).await?;
        gen.dir = final_dir;
        let store = ctx.store();
        if let Err(e) = store.insert_generation(&gen) {
            // Back to the side folder, which the failed build then removes.
            let _ = std::fs::rename(&gen.dir, &side);
            return Err(e);
        }
        store.set_current(&gen.template, gen.gen, expected_current)?
    };
    if !current {
        crate::lifecycle::log(
            ctx,
            &format!(
                "template {}: generation {} kept as non-current: the current generation changed during the build",
                gen.template, gen.gen
            ),
        );
    }
    let kind = match gen.kind {
        GenKind::Promoted => events::TEMPLATE_PROMOTED,
        GenKind::Upgraded => events::TEMPLATE_UPGRADED,
        GenKind::Full | GenKind::Layered => events::TEMPLATE_BUILT,
    };
    events::emit(
        ctx,
        kind,
        None,
        Some(&gen.template),
        serde_json::json!({
            "gen": gen.gen,
            "kind": gen.kind,
            "current": current,
            "pg_major": gen.pg_major,
            "size_bytes": gen.size_bytes,
            "from_clone": gen.from_clone,
            "from_gen": gen.from_gen,
        }),
    );
    prune(ctx, &gen.template).await;
    Ok(current)
}

/// Removes generations beyond the newest `keep`, never the current one: rows first, then the
/// folders, renamed aside under the write guard (no clonefile reads them meanwhile) and deleted
/// outside it. Failures are logged; gc removes what is left.
pub(crate) async fn prune(ctx: &Ctx, name: &str) {
    let (gens, doomed): (Vec<u32>, Vec<PathBuf>) = match prune_rows(ctx, name) {
        Ok(d) => d.into_iter().unzip(),
        Err(e) => {
            return crate::lifecycle::log(ctx, &format!("prune of template {name}: {e}"));
        }
    };
    if doomed.is_empty() {
        return;
    }
    events::emit(
        ctx,
        events::TEMPLATE_PRUNED,
        None,
        Some(name),
        serde_json::json!({ "gens": gens }),
    );
    let mut old = Vec::new();
    {
        let _swap = ctx.template_swap.write().await;
        for dir in doomed {
            let mut ts = now();
            let to = loop {
                let to = ctx.home.templates_dir().join(old_dir_name(name, ts));
                if !to.exists() {
                    break to;
                }
                ts += 1;
            };
            match std::fs::rename(&dir, &to) {
                Ok(()) => old.push(to),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    crate::lifecycle::log(ctx, &format!("prune: move {} aside: {e}", dir.display()))
                }
            }
        }
    }
    let _ = blocking(move || {
        for dir in old {
            let _ = std::fs::remove_dir_all(&dir);
        }
        Ok(())
    })
    .await;
}

/// Deletes the rows of the generations to prune; returns their numbers and folders.
fn prune_rows(ctx: &Ctx, name: &str) -> Result<Vec<(u32, PathBuf)>> {
    let store = ctx.store();
    let Some(t) = store.get_template(name)? else {
        return Ok(Vec::new());
    };
    // A branch template keeps as many generations as its base.
    let keep = match &t.base {
        Some(base) => store.get_template(base)?.map_or(t.keep, |b| b.keep),
        None => t.keep,
    };
    let gens = store.list_generations(name)?;
    let cut = gens.len().saturating_sub(keep as usize);
    let mut dirs = Vec::new();
    for g in gens[..cut].iter().filter(|g| g.gen != t.current_gen) {
        store.delete_generation(name, g.gen)?;
        dirs.push((g.gen, g.dir.clone()));
    }
    Ok(dirs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_port_skips_reserved_ports() {
        let tmp = tempfile::Builder::new()
            .prefix("mt")
            .tempdir_in("/tmp")
            .unwrap();
        // A random 20-port slice of 55000-59999, so parallel tests rarely meet.
        let lo = 55_000 + u16::from_str_radix(&random_hex(2), 16).unwrap() % 250 * 20;
        let config = crate::ctx::Config {
            port_range: lo..=lo + 19,
            ..crate::ctx::Config::default()
        };
        let ctx = Ctx::open(crate::paths::Home::at(tmp.path().to_path_buf()), config).unwrap();
        let lowest = build_port(&ctx, &HashSet::new()).unwrap();
        ctx.store()
            .upsert_reservation(&crate::state::ReservationRow {
                name: "x".into(),
                port: lowest,
                password: "pw".into(),
                template: "main".into(),
                last_used: 0,
            })
            .unwrap();
        assert_ne!(build_port(&ctx, &HashSet::new()).unwrap(), lowest);
    }

    // Built at runtime so no `scheme://user:password@host` literal sits in the source.
    fn url(scheme: &str, user: &str, pass: &str, rest: &str) -> String {
        format!("{scheme}://{user}:{pass}@{rest}")
    }

    #[test]
    fn database_names() {
        assert_eq!(
            db_from_url(&url("postgres", "u", "p", "h:5432/shop?sslmode=require")),
            "shop"
        );
        assert_eq!(db_from_url("postgres://h:5432"), "postgres");
        assert_eq!(db_from_url("postgres://h/"), "postgres");
        let args = |source, database: Option<&str>| TemplateInitArgs {
            name: "main".into(),
            source,
            database: database.map(String::from),
            migrations: None,
            max_age_secs: None,
            pg_major: None,
            settings: TemplateSettings::default(),
        };
        let seed = TemplateSource::Seed {
            command: "true".into(),
            cwd: None,
            path: None,
        };
        assert_eq!(database_for(&args(seed.clone(), None)).unwrap(), "app");
        assert_eq!(database_for(&args(seed.clone(), Some("x"))).unwrap(), "x");
        assert_eq!(
            database_for(&args(seed, Some("a=b"))).unwrap_err().code,
            ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_ident(r#"we"ird"#), r#""we""ird""#);
        assert_eq!(url_encode("my db/x"), "my%20db%2Fx");
    }

    #[test]
    fn redacts_urls_inside_text() {
        assert_eq!(
            redact_url(&format!(
                "psql '{}' && psql {}",
                url("postgres", "a", "pw", "h/db"),
                url("postgresql", "b", "pw2", "h2:1/x")
            )),
            "psql 'postgres://a:***@h/db' && psql postgresql://b:***@h2:1/x"
        );
        assert_eq!(redact_url("postgres://u@h/db"), "postgres://u@h/db");
    }

    #[test]
    fn redacts_password_env_assignments() {
        assert_eq!(
            redact_url("PGPASSWORD=s3cret psql -c 'select 1'"),
            "PGPASSWORD=*** psql -c 'select 1'"
        );
        assert_eq!(
            redact_url("DB_PASSWORD='a b' PGPASSWORD=\"x y\" run"),
            "DB_PASSWORD=*** PGPASSWORD=*** run"
        );
        assert_eq!(redact_url("echo PASSWORD= done"), "echo PASSWORD= done");
    }

    #[test]
    fn migrate_cwd_must_be_a_folder() {
        let tmp = tempfile::tempdir().unwrap();
        let migrate = |cwd: PathBuf| TemplateSettings {
            migrate: Some(crate::protocol::MigrateCmd {
                command: "make".into(),
                cwd,
                path: None,
            }),
            ..Default::default()
        };
        let mut t = TemplateRow::new("m", "app", TemplateSource::Url { url: "x".into() }, None, 1);
        apply_settings(&mut t, None, &migrate(tmp.path().to_path_buf())).unwrap();
        let e = apply_settings(&mut t, None, &migrate(tmp.path().join("gone"))).unwrap_err();
        assert!(e.message.contains("not a folder"), "{}", e.message);
    }

    #[test]
    fn build_log_name_is_safe_for_branch_templates() {
        let home = Home::at(PathBuf::from("/h"));
        assert_eq!(
            build_log_path(&home, "main"),
            home.logs_dir().join("template-main.log")
        );
        let log = build_log_path(&home, &branch_template_name("main", "feature/x y"));
        let file = log.file_name().unwrap().to_str().unwrap();
        assert_eq!(log.parent().unwrap(), home.logs_dir());
        assert!(file.starts_with("template-main~feature-x-y-"), "{file}");
    }

    #[test]
    fn password_is_split_out_of_the_pg_dump_url() {
        let s = split_password(&url(
            "postgres",
            "u",
            "s%40cr%3Aet",
            "h:5432/db?sslmode=require",
        ))
        .unwrap();
        assert_eq!(s.url, "postgres://u@h:5432/db?sslmode=require");
        assert_eq!(s.password.as_deref(), Some("s@cr:et"));
        assert_eq!(s.raw_password.as_deref(), Some("s%40cr%3Aet"));
        assert!(!s.url.contains("s%40cr") && !s.url.contains("s@cr"));

        let none = split_password("postgres://u@h/db").unwrap();
        assert_eq!(
            (none.url.as_str(), none.password),
            ("postgres://u@h/db", None)
        );
        assert_eq!(split_password("postgres://h/db").unwrap().password, None);

        let e = split_password(&url("postgres", "u", "pa%zz", "h/db")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidRequest);
        assert!(!e.to_string().contains("pa%zz"), "{e}");
        assert!(split_password(&url("postgres", "u", "pa%4", "h/db")).is_err()); // truncated escape
        assert!(split_password(&url("postgres", "u", "pa%+1", "h/db")).is_err());
        // sign is not a hex digit
    }

    #[test]
    fn query_password_is_split_out_and_redacted() {
        let s = split_password("postgres://u@h/db?sslmode=require&password=s%40cr&x=1").unwrap();
        assert_eq!(s.url, "postgres://u@h/db?sslmode=require&x=1");
        assert_eq!(s.password.as_deref(), Some("s@cr"));
        assert_eq!(s.raw_password.as_deref(), Some("s%40cr"));

        let only = split_password("postgres://u@h/db?password=pw").unwrap();
        assert_eq!(only.url, "postgres://u@h/db");

        // The query password wins over the userinfo one.
        let both = split_password(&url("postgres", "u", "old", "h/db?password=new")).unwrap();
        assert_eq!(
            (both.url.as_str(), both.password.as_deref()),
            ("postgres://u@h/db", Some("new"))
        );

        assert!(split_password("postgres://u@h/db?password=pa%zz").is_err());

        assert_eq!(
            redact_url("pg_dump 'postgres://u@h/db?sslmode=x&password=pw#f' ok"),
            "pg_dump 'postgres://u@h/db?sslmode=x&password=***#f' ok"
        );
        assert_eq!(
            redact_url("postgres://u@h/db?sslmode=x"),
            "postgres://u@h/db?sslmode=x"
        );
    }

    #[test]
    fn invalid_password_encoding_rejected_by_init_without_echo() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = Ctx::open(
            crate::paths::Home::at(tmp.path().join("m")),
            Default::default(),
        )
        .unwrap();
        let args = TemplateInitArgs {
            name: "main".into(),
            source: TemplateSource::Url {
                url: url("postgres", "u", "pa%zz", "h/db"),
            },
            database: None,
            migrations: None,
            max_age_secs: None,
            pg_major: None,
            settings: TemplateSettings::default(),
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let e = rt.block_on(init(&ctx, args)).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidRequest);
        assert!(!e.to_string().contains("pa%zz"), "{e}");
    }

    #[test]
    fn secrets_are_scrubbed_from_text() {
        let src = TemplateSource::Url {
            url: url("postgres", "u", "p%40ss", "h/db"),
        };
        let secrets = source_secrets(&src);
        assert_eq!(secrets, vec!["p%40ss".to_string(), "p@ss".to_string()]);
        let text = format!(
            "invalid token: \"p%40ss\"; also p@ss and {}",
            url("postgres", "x", "y", "h")
        );
        assert_eq!(
            scrub(&text, &secrets),
            "invalid token: \"***\"; also *** and postgres://x:***@h"
        );
        assert!(source_secrets(&TemplateSource::Seed {
            command: "x".into(),
            cwd: None,
            path: None
        })
        .is_empty());
    }

    #[test]
    fn leftover_names_round_trip() {
        assert_eq!(
            parse_leftover(&build_dir_name("main", 17)),
            Some(Leftover::Building("main".into()))
        );
        assert_eq!(
            parse_leftover(&old_dir_name("my-db", 42)),
            Some(Leftover::Old("my-db".into(), 42))
        );
        for bad in [
            "main",
            ".DS_Store",
            ".main.old-",
            ".main.old-1x",
            ".main.building-",
            ".Main.old-1",
            ".main.backup-1",
            ".git",
        ] {
            assert_eq!(parse_leftover(bad), None, "{bad}");
        }
        assert!(is_build_socket_name("build-54321"));
        for bad in ["build-", "build-x", "build-99999", "build-+1", "cabc123"] {
            assert!(!is_build_socket_name(bad), "{bad}");
        }
        assert_eq!(
            parse_gen_dir(&gen_dir_name("my-db", 12)),
            Some(("my-db".into(), 12))
        );
        for bad in [
            "main", "main@", "main@x", "main@+1", "Main@1", "@1", "main@1@2", ".main@1",
        ] {
            assert_eq!(parse_gen_dir(bad), None, "{bad}");
        }
    }

    #[test]
    fn build_guard_is_exclusive_per_name() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = Ctx::open(
            crate::paths::Home::at(tmp.path().join("m")),
            Default::default(),
        )
        .unwrap();
        let g = BuildGuard::take(&ctx, "main").unwrap();
        let e = BuildGuard::take(&ctx, "main").err().unwrap();
        assert_eq!(e.code, ErrorCode::InvalidRequest);
        assert_eq!(e.message, "template main is already being built");
        assert!(BuildGuard::take(&ctx, "other").is_ok());
        drop(g);
        assert!(BuildGuard::take(&ctx, "main").is_ok());
    }

    // The daemon spawns these futures on a multi-thread runtime, so they must be Send.
    #[allow(dead_code)]
    fn futures_are_send(ctx: &'static Ctx, args: TemplateInitArgs) {
        fn send<T: Send>(_: T) {}
        send(init(ctx, args));
        send(refresh(ctx, "main", None));
        send(rollback(ctx, "main", None));
        send(layered(ctx, "main", None));
        send(background(ctx, "main", RefreshMode::Layered));
    }
}
