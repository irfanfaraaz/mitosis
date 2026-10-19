use crate::clone_backend::apparent_size;
use crate::clones::START_TRIES;
use crate::ctx::Ctx;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::names::{random_hex, validate_name};
use crate::pg::{
    find_major, initdb, newest, psql, read_pg_version, start, stop, wait_ready, PgInstall,
    StartOptions,
};
use crate::ports::pick_port;
use crate::protocol::{TemplateInfo, TemplateInitArgs, TemplateSource};
use crate::state::TemplateRow;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_SEED_DATABASE: &str = "app";

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

pub async fn init(ctx: &Ctx, args: TemplateInitArgs) -> Result<TemplateInfo> {
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
    build(ctx, args).await
}

pub async fn refresh(ctx: &Ctx, name: &str) -> Result<TemplateInfo> {
    validate_name("template", name)?;
    let row = ctx.store().get_template(name)?.ok_or_else(|| {
        MitosisError::new(
            ErrorCode::TemplateNotFound,
            format!("template {name} not found"),
        )
        .with_hint("see existing templates with: mitosis template list")
    })?;
    let args = TemplateInitArgs {
        name: row.name,
        source: row.source,
        database: Some(row.database),
        migrations: row.migrations,
        max_age_secs: Some(row.max_age_secs),
        pg_major: Some(row.pg_major),
    };
    build(ctx, args).await
}

pub fn list(ctx: &Ctx) -> Result<Vec<TemplateInfo>> {
    let rows = ctx.store().list_templates()?;
    let now = now();
    Ok(rows.iter().map(|r| info(r, now)).collect())
}

pub fn info(row: &TemplateRow, now: i64) -> TemplateInfo {
    let age_secs = now.saturating_sub(row.built_at).max(0) as u64;
    let mut warnings = Vec::new();
    if age_secs > row.max_age_secs {
        warnings.push(format!(
            "template is {} days old; run: mitosis template refresh {}",
            age_secs / 86_400,
            row.name
        ));
    }
    if let Some(dir) = row.migrations.as_deref().filter(|d| d.is_dir()) {
        if hash_dir(dir).ok() != row.migrations_hash {
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
        TemplateSource::Seed { command, cwd } => TemplateSource::Seed {
            command: redact_url(command),
            cwd: cwd.clone(),
        },
    };
    TemplateInfo {
        name: row.name.clone(),
        pg_major: row.pg_major,
        source,
        database: row.database.clone(),
        size_bytes: row.size_bytes,
        built_at: row.built_at,
        age_secs,
        warnings,
    }
}

/// Masks the password of every `scheme://user:password@host` in `text` as `***`.
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
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
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

/// Splits the password out of `url`. Bad percent-encoding is an error that never echoes the password.
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
    let Some(at) = authority.rfind('@') else {
        return Ok(plain());
    };
    let Some(colon) = authority[..at].find(':') else {
        return Ok(plain());
    };
    let raw = &authority[colon + 1..at];
    let password = percent_decode(raw)
        .ok_or_else(|| invalid("the password in the source URL has invalid percent-encoding"))?;
    Ok(SplitUrl {
        url: format!("{head}{}{}", &authority[..colon], &tail[at..]),
        password: Some(password),
        raw_password: Some(raw.to_string()),
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

fn url_encode(s: &str) -> String {
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

/// `pg_dump -Fc <url> | pg_restore` into the build server, both stderrs to the log.
/// The password goes in `PGPASSWORD`, never in argv, so neither `ps` nor libpq errors show it.
fn dump_restore(install: &PgInstall, url: &str, opts: &StartOptions, db: &str) -> Result<()> {
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
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(dump_err)
        .spawn()
        .map_err(|e| internal(format!("could not run pg_dump: {e}")))?;
    let dump_out = dump.stdout.take().expect("pg_dump stdout is piped");
    // If pg_restore cannot start, the pipe closes and pg_dump exits, so the wait below cannot hang.
    let restore = Command::new(install.bin("pg_restore"))
        .args(["--no-owner", "--no-acl", "-h"])
        .arg(&opts.socket_dir)
        .args(["-p", &opts.port.to_string(), "-U", "postgres", "-d", db])
        .stdin(dump_out)
        .stdout(restore_out)
        .stderr(restore_err)
        .status()
        .map_err(|e| internal(format!("could not run pg_restore: {e}")));
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

/// Runs the seed command against the build server over TCP.
async fn run_seed(
    install: &PgInstall,
    opts: &StartOptions,
    db: &str,
    command: &str,
    cwd: Option<&Path>,
) -> Result<()> {
    // initdb sets host auth to scram-sha-256, so TCP needs a password: a throwaway one, cleared afterwards.
    let password = random_hex(16);
    let sql = format!(
        "SET password_encryption = 'scram-sha-256'; ALTER ROLE postgres PASSWORD '{password}'"
    );
    run_psql(install, opts, "postgres", sql).await?;
    let url = format!(
        "postgres://postgres:{password}@127.0.0.1:{}/{}",
        opts.port,
        url_encode(db)
    );
    let log = || OpenOptions::new().append(true).open(&opts.log_file);
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c")
        .arg(command)
        .env("DATABASE_URL", &url)
        .stdin(Stdio::null())
        .stdout(log()?)
        .stderr(log()?);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let status = cmd.status().await.map_err(|e| match cwd {
        Some(dir) => internal(format!(
            "could not run the seed command in {}: {e}",
            dir.display()
        )),
        None => internal(format!("could not run the seed command: {e}")),
    })?;
    if !status.success() {
        return Err(internal(format!("seed command failed ({status})")));
    }
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

    let mut tried = HashSet::new();
    loop {
        let pg = start(install, data_dir, opts).map_err(at("start"))?;
        match wait_ready(install, &pg, opts, ctx.config.ready_timeout).await {
            Ok(()) => break,
            // Another process can take the picked port before postgres binds it.
            Err(e) if e.code == ErrorCode::PortInUse && tried.len() + 1 < START_TRIES => {
                let (i, d, t) = (
                    install.clone(),
                    data_dir.to_path_buf(),
                    ctx.config.stop_timeout,
                );
                blocking(move || stop(&i, &d, t))
                    .await
                    .map_err(at("start"))?;
                let _ = std::fs::remove_dir(&opts.socket_dir);
                tried.insert(opts.port);
                let leased = ctx.store().leased_ports().map_err(at("start"))?;
                opts.port = pick_port(ctx.config.port_range.clone(), &leased, &tried)
                    .map_err(at("start"))?;
                opts.socket_dir = build_socket_dir(ctx, opts.port);
            }
            Err(e) => return Err(("start", e)),
        }
    }

    if database != "postgres" {
        let sql = format!("CREATE DATABASE {}", quote_ident(database));
        run_psql(install, opts, "postgres", sql)
            .await
            .map_err(at("create database"))?;
    }

    match source {
        TemplateSource::Url { url } => {
            let (i, u, o, db) = (
                install.clone(),
                url.clone(),
                opts.clone(),
                database.to_string(),
            );
            blocking(move || dump_restore(&i, &u, &o, &db))
                .await
                .map_err(at("restore"))?;
        }
        TemplateSource::Seed { command, cwd } => {
            run_seed(install, opts, database, command, cwd.as_deref())
                .await
                .map_err(at("seed"))?;
        }
    }

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
        ctx.config.stop_timeout,
    );
    blocking(move || stop(&i, &d, t))
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
        ctx.config.stop_timeout,
    );
    let cleanup = blocking(move || {
        // A server that would not stop keeps its folder; gc deals with it later.
        stop(&i, &d, t)?;
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

/// Puts `build_dir` at `final_dir`, keeping the previous template if the rename fails.
fn swap(build_dir: &Path, final_dir: &Path, old_dir: &Path, log: &Path) -> Result<()> {
    let had_old = final_dir.exists();
    if had_old {
        std::fs::rename(final_dir, old_dir)?;
    }
    if let Err(e) = std::fs::rename(build_dir, final_dir) {
        if had_old {
            if let Err(re) = std::fs::rename(old_dir, final_dir) {
                append_log(
                    log,
                    &format!(
                        "mitosis: could not move the previous template back from {} to {}: {re}",
                        old_dir.display(),
                        final_dir.display()
                    ),
                );
            }
        }
        return Err(e.into());
    }
    if had_old {
        let _ = std::fs::remove_dir_all(old_dir); // a leftover .old- folder is removed by gc
    }
    Ok(())
}

async fn build(ctx: &Ctx, args: TemplateInitArgs) -> Result<TemplateInfo> {
    let name = args.name.clone();
    let database = database_for(&args)?;
    let _guard = BuildGuard::take(ctx, &name)?;
    let install = match args.pg_major {
        Some(major) => find_major(&ctx.installs, major)?,
        None => newest(&ctx.installs)?,
    }
    .clone();
    let migrations_hash = match args.migrations.as_deref() {
        Some(m) if m.is_dir() => Some(hash_dir(m)?),
        _ => None,
    };
    let max_age_secs = args.max_age_secs.unwrap_or(ctx.config.default_max_age_secs);
    let secrets = source_secrets(&args.source);

    let ts = now();
    let templates_dir = ctx.home.templates_dir();
    let build_dir = templates_dir.join(format!(".{name}.building-{ts}"));
    let log = ctx.home.logs_dir().join(format!("template-{name}.log"));
    std::fs::File::create(&log)?; // truncate, so the error tail belongs to this build
    let leased = ctx.store().leased_ports()?;
    let port = pick_port(ctx.config.port_range.clone(), &leased, &HashSet::new())?;
    let mut opts = StartOptions {
        port,
        socket_dir: build_socket_dir(ctx, port),
        listen_tcp: true,
        durable: false,
        log_file: log.clone(),
    };

    let result = run_steps(
        ctx,
        &install,
        &build_dir,
        &mut opts,
        &database,
        &args.source,
    )
    .await;
    let result = match result {
        Ok(ext) => Ok(ext),
        Err((step, e)) => Err(fail(ctx, &install, &build_dir, &log, &secrets, step, e).await),
    };
    let _ = std::fs::remove_dir(&opts.socket_dir); // empty once the server is stopped
    let extensions = result?;

    // Step 7: measure, swap, record.
    let final_dir = templates_dir.join(&name);
    let old_dir = templates_dir.join(format!(".{name}.old-{ts}"));
    let (b, f, o, l) = (build_dir.clone(), final_dir.clone(), old_dir, log.clone());
    let swapped = blocking(move || {
        let pg_major = read_pg_version(&b)?;
        let size_bytes = apparent_size(&b)?;
        swap(&b, &f, &o, &l)?;
        Ok((pg_major, size_bytes))
    })
    .await;
    let (pg_major, size_bytes) = match swapped {
        Ok(v) => v,
        Err(e) => return Err(fail(ctx, &install, &build_dir, &log, &secrets, "swap", e).await),
    };
    let row = TemplateRow {
        name,
        dir: final_dir,
        pg_major,
        bin_dir: install.bin_dir.clone(),
        extensions,
        database,
        source: args.source,
        migrations: args.migrations,
        migrations_hash,
        max_age_secs,
        built_at: ts,
        size_bytes,
    };
    let upserted = ctx.store().upsert_template(&row);
    if let Err(e) = upserted {
        return Err(fail(ctx, &install, &build_dir, &log, &secrets, "swap", e).await);
    }
    Ok(info(&row, now()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_names() {
        assert_eq!(
            db_from_url("postgres://u:p@h:5432/shop?sslmode=require"),
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
        };
        let seed = TemplateSource::Seed {
            command: "true".into(),
            cwd: None,
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
            redact_url("psql 'postgres://a:pw@h/db' && psql postgresql://b:pw2@h2:1/x"),
            "psql 'postgres://a:***@h/db' && psql postgresql://b:***@h2:1/x"
        );
        assert_eq!(redact_url("postgres://u@h/db"), "postgres://u@h/db");
    }

    #[test]
    fn password_is_split_out_of_the_pg_dump_url() {
        let s = split_password("postgres://u:s%40cr%3Aet@h:5432/db?sslmode=require").unwrap();
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

        let e = split_password("postgres://u:pa%zz@h/db").unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidRequest);
        assert!(!e.to_string().contains("pa%zz"), "{e}");
        assert!(split_password("postgres://u:pa%4@h/db").is_err()); // truncated escape
        assert!(split_password("postgres://u:pa%+1@h/db").is_err()); // sign is not a hex digit
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
                url: "postgres://u:pa%zz@h/db".into(),
            },
            database: None,
            migrations: None,
            max_age_secs: None,
            pg_major: None,
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
            url: "postgres://u:p%40ss@h/db".into(),
        };
        let secrets = source_secrets(&src);
        assert_eq!(secrets, vec!["p%40ss".to_string(), "p@ss".to_string()]);
        let text = "invalid token: \"p%40ss\"; also p@ss and postgres://x:y@h";
        assert_eq!(
            scrub(text, &secrets),
            "invalid token: \"***\"; also *** and postgres://x:***@h"
        );
        assert!(source_secrets(&TemplateSource::Seed {
            command: "x".into(),
            cwd: None
        })
        .is_empty());
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
        send(refresh(ctx, "main"));
    }
}
