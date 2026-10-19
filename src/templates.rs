use crate::clone_backend::apparent_size;
use crate::clones::START_TRIES;
use crate::ctx::Ctx;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::names::{random_hex, validate_name};
use crate::pg::{
    initdb, psql, read_pg_version, start, stop, wait_ready, PgInstall, RunningPg, StartOptions,
};
use crate::ports::pick_port;
use crate::protocol::{
    GenKind, GenerationInfo, MigrateCmd, RefreshMode, TemplateInfo, TemplateInitArgs,
    TemplateSetArgs, TemplateSettings, TemplateSource,
};
use crate::state::{GenerationRow, TemplateRow};
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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
    if let TemplateSource::Url { url } = &args.source {
        if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
            return Err(invalid(
                "source URL must start with postgres:// or postgresql://",
            ));
        }
        split_password(url)?;
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
    apply_settings(&mut preview, None, &args.settings)?;
    build(ctx, args).await
}

fn not_found(name: &str) -> MitosisError {
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
        max_age_secs: Some(row.max_age_secs),
        pg_major: Some(cur.pg_major),
        settings: TemplateSettings::default(),
    })
}

/// Changes the settings of `name`; `resume` clears `paused`.
pub fn set(ctx: &Ctx, args: TemplateSetArgs) -> Result<TemplateInfo> {
    validate_name("template", &args.name)?;
    {
        let store = ctx.store();
        let mut t = store
            .get_template(&args.name)?
            .ok_or_else(|| not_found(&args.name))?;
        apply_settings(&mut t, args.max_age_secs, &args.settings)?;
        if args.resume {
            t.paused = false;
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
    if let Some(a) = s.auto_migrate {
        t.auto_migrate = a;
    }
    if let Some(o) = s.on_stale {
        t.on_stale = o;
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
fn template_info(ctx: &Ctx, name: &str) -> Result<TemplateInfo> {
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
        warnings.push(format!(
            "template is {} days old; run: mitosis template refresh {}",
            age_secs / 86_400,
            row.name
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
    let source = match &row.source {
        TemplateSource::Url { url } => TemplateSource::Url {
            url: redact_url(url),
        },
        TemplateSource::Seed { command, cwd, path } => TemplateSource::Seed {
            command: redact_url(command),
            cwd: cwd.clone(),
            path: path.clone(),
        },
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
    }
}

pub fn history(ctx: &Ctx, name: &str) -> Result<Vec<GenerationInfo>> {
    validate_name("template", name)?;
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
    let TemplateSource::Url { url } = source else {
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

fn fnv1a(h: &mut u64, bytes: &[u8]) {
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
        TemplateSource::Seed { .. } => args
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
struct BuildGuard<'a> {
    ctx: &'a Ctx,
    name: String,
}

impl<'a> BuildGuard<'a> {
    fn take(ctx: &'a Ctx, name: &str) -> Result<BuildGuard<'a>> {
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
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Leftover {
    Building(String),
    Old(String, i64),
}

fn build_dir_name(name: &str, ts: i64) -> String {
    format!(".{name}.building-{ts}")
}

fn old_dir_name(name: &str, ts: i64) -> String {
    format!(".{name}.old-{ts}")
}

/// The folder of generation `gen` of template `name`, in `templates/`.
fn gen_dir_name(name: &str, gen: u32) -> String {
    format!("{name}@{gen}")
}

/// Parses the names `gen_dir_name` makes; anything else is `None`.
pub(crate) fn parse_gen_dir(file_name: &str) -> Option<(String, u32)> {
    let (name, gen) = file_name.split_once('@')?;
    validate_name("template", name).ok()?;
    if gen.is_empty() || !gen.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((name.into(), gen.parse().ok()?))
}

/// Parses the names `build_dir_name` and `old_dir_name` make; anything else is `None`.
pub(crate) fn parse_leftover(file_name: &str) -> Option<Leftover> {
    let (name, suffix) = file_name.strip_prefix('.')?.split_once('.')?;
    validate_name("template", name).ok()?;
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
) -> StepResult<Vec<String>> {
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

    match source {
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
    }
    extensions_and_stop(ctx, install, data_dir, opts, database, pg).await
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
                let leased = ctx.store().leased_ports().map_err(at("start"))?;
                opts.port =
                    pick_port(ctx.config().port_range, &leased, &tried).map_err(at("start"))?;
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
    let cleanup = blocking(move || {
        // A server that would not stop keeps its folder; gc deals with it later.
        stop(&i, &d, None, t)?;
        match std::fs::remove_dir_all(&d) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
    })
    .await;
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
    let install = match args.pg_major {
        Some(major) => ctx.install_major(major).await?,
        None => ctx.newest_install().await?,
    };
    let migrations_hash = match args.migrations.as_deref() {
        Some(m) if m.is_dir() => Some(hash_dir(m)?),
        _ => None,
    };
    let max_age_secs = args
        .max_age_secs
        .unwrap_or(ctx.config().default_max_age_secs);
    // The commit expects the pointer read here; a new template starts at 0 (no generation).
    let (created, expected, gen) = {
        let store = ctx.store();
        let (created, expected) = match store.get_template(&name)? {
            Some(t) => (false, t.current_gen),
            None => {
                store.insert_template(&TemplateRow::new(
                    &name,
                    &database,
                    args.source.clone(),
                    args.migrations.clone(),
                    max_age_secs,
                ))?;
                (true, 0)
            }
        };
        (created, expected, store.alloc_gen(&name)?)
    };
    let built = build_gen(
        ctx,
        &install,
        &args,
        &database,
        migrations_hash,
        gen,
        expected,
    )
    .await;
    let store = ctx.store();
    if let Err(e) = built {
        // A failed first build leaves no template behind.
        if created && matches!(store.list_generations(&name), Ok(g) if g.is_empty()) {
            let _ = store.delete_template(&name);
        }
        return Err(e);
    }
    let mut t = store.get_template(&name)?.ok_or_else(|| not_found(&name))?;
    t.database = database;
    t.source = args.source;
    t.migrations = args.migrations;
    t.max_age_secs = max_age_secs;
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
fn resume_if_current(t: &mut TemplateRow, gen: u32) {
    if t.current_gen == gen {
        t.paused = false;
        t.last_error = None;
    }
}

/// Copies the current generation of `name`, runs only its migrate command on the copy and makes
/// the result a `Layered` generation. `path`, when given, replaces the command's recorded `PATH`.
pub async fn layered(ctx: &Ctx, name: &str, path: Option<String>) -> Result<TemplateInfo> {
    validate_name("template", name)?;
    let _guard = BuildGuard::take(ctx, name)?;
    layered_held(ctx, name, path, true).await
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
    let install = ctx.install_major(cur.pg_major).await?;
    let migrations_hash = match t.migrations.as_deref() {
        Some(m) if m.is_dir() => Some(hash_dir(m)?),
        _ => None,
    };
    let gen = ctx.store().alloc_gen(name)?;

    let secrets = source_secrets(&t.source);
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
        &migrate,
    )
    .await;
    let result = match result {
        Ok(ext) => Ok(ext),
        Err((step, e)) => Err(fail(ctx, &install, &side, &log, &secrets, step, e).await),
    };
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
    };
    commit_built(ctx, &install, &log, &secrets, row, cur.gen).await?;
    if manual {
        let store = ctx.store();
        let mut t = store.get_template(name)?.ok_or_else(|| not_found(name))?;
        resume_if_current(&mut t, gen);
        store.update_template_settings(&t)?;
    }
    template_info(ctx, name)
}

/// Copy of generation folder `from` into `dir`, started privately, migrated, stopped.
async fn layered_steps(
    ctx: &Ctx,
    install: &PgInstall,
    from: &Path,
    dir: &Path,
    opts: &mut StartOptions,
    database: &str,
    migrate: &MigrateCmd,
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
    extensions_and_stop(ctx, install, dir, opts, database, pg).await
}

/// Options for a private build server of `name` on a free port. Truncates the build log, so an
/// error tail belongs to this build.
fn build_start_options(ctx: &Ctx, name: &str) -> Result<StartOptions> {
    let log = ctx.home.logs_dir().join(format!("template-{name}.log"));
    std::fs::File::create(&log)?;
    let leased = ctx.store().leased_ports()?;
    let port = pick_port(ctx.config().port_range, &leased, &HashSet::new())?;
    Ok(StartOptions {
        port,
        socket_dir: build_socket_dir(ctx, port),
        listen_tcp: true,
        durable: false,
        log_file: log,
    })
}

/// Builds generation `gen` in a side folder and commits it as a `Full` generation.
async fn build_gen(
    ctx: &Ctx,
    install: &PgInstall,
    args: &TemplateInitArgs,
    database: &str,
    migrations_hash: Option<String>,
    gen: u32,
    expected: u32,
) -> Result<()> {
    let name = &args.name;
    let secrets = source_secrets(&args.source);
    let ts = now();
    let build_dir = ctx.home.templates_dir().join(build_dir_name(name, ts));
    let mut opts = build_start_options(ctx, name)?;
    let log = opts.log_file.clone();

    let result = run_steps(ctx, install, &build_dir, &mut opts, database, &args.source).await;
    let result = match result {
        Ok(ext) => Ok(ext),
        Err((step, e)) => Err(fail(ctx, install, &build_dir, &log, &secrets, step, e).await),
    };
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
    };
    commit_built(ctx, install, &log, &secrets, row, expected).await
}

/// Step 7: measures the built side folder `row.dir` (filling `pg_major` and `size_bytes`), then
/// commits it. On failure the folder is removed and the error has the build-failure shape.
async fn commit_built(
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
    prune(ctx, &gen.template).await;
    Ok(current)
}

/// Removes generations beyond the newest `keep`, never the current one: rows first, then the
/// folders, renamed aside under the write guard (no clonefile reads them meanwhile) and deleted
/// outside it. Failures are logged; gc removes what is left.
pub(crate) async fn prune(ctx: &Ctx, name: &str) {
    let doomed = match prune_rows(ctx, name) {
        Ok(d) => d,
        Err(e) => {
            return crate::lifecycle::log(ctx, &format!("prune of template {name}: {e}"));
        }
    };
    if doomed.is_empty() {
        return;
    }
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

/// Deletes the rows of the generations to prune; returns their folders.
fn prune_rows(ctx: &Ctx, name: &str) -> Result<Vec<PathBuf>> {
    let store = ctx.store();
    let Some(t) = store.get_template(name)? else {
        return Ok(Vec::new());
    };
    let gens = store.list_generations(name)?;
    let cut = gens.len().saturating_sub(t.keep as usize);
    let mut dirs = Vec::new();
    for g in gens[..cut].iter().filter(|g| g.gen != t.current_gen) {
        store.delete_generation(name, g.gen)?;
        dirs.push(g.dir.clone());
    }
    Ok(dirs)
}

#[cfg(test)]
mod tests {
    use super::*;

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
