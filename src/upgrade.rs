//! Template upgrade: moves a template's current generation to a newer Postgres major with
//! `pg_upgrade --clone`. The current generation is copied (clonefile) into side folder A, a new
//! cluster B is made with the target's `initdb` (same encoding, locale and checksums as A), and
//! `pg_upgrade` fills B from A. B becomes an `Upgraded` generation of the same template, or
//! generation 1 of a new one (`--as`). The source generation is never touched.

use crate::ctx::Ctx;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::names::validate_name;
use crate::pg::{check_extensions, psql, start, stop, wait_ready, PgInstall, StartOptions};
use crate::ports::pick_port;
use crate::protocol::{GenKind, TemplateInfo};
use crate::state::{GenerationRow, TemplateRow};
use crate::templates::{
    build_dir_name, commit_built, not_found, now, redact_url, resume_if_current, template_info,
    BuildGuard,
};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

fn invalid(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::InvalidRequest, msg)
}

fn internal(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, msg)
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

/// Upgrades template `name` to Postgres `to`: a new generation of `name`, or with `as_name` the
/// first generation of a new template with the same database and settings (schedule off).
/// Holds the build slot of `name` (and of `as_name`) for the whole operation.
pub async fn upgrade(
    ctx: &Ctx,
    name: &str,
    to: u32,
    as_name: Option<&str>,
) -> Result<TemplateInfo> {
    validate_name("template", name)?;
    if let Some(n) = as_name {
        validate_name("template", n)?;
        if n == name {
            return Err(exists(n));
        }
    }
    let _guard = BuildGuard::take(ctx, name)?;
    let _new_guard = as_name.map(|n| BuildGuard::take(ctx, n)).transpose()?;

    let (row, cur) = ctx.store().current(name)?.ok_or_else(|| not_found(name))?;
    if to <= cur.pg_major {
        return Err(invalid(format!(
            "template {name} is on Postgres {}; --to must be a newer major",
            cur.pg_major
        ))
        .with_hint(format!("pick a major above {}", cur.pg_major)));
    }
    let target = ctx.install_major(to).await?;
    if !target.pg_upgrade().is_file() {
        return Err(MitosisError::new(
            ErrorCode::NoMatchingPostgres,
            format!(
                "Postgres {to} in {} has no pg_upgrade",
                target.bin_dir.display()
            ),
        )
        .with_hint(format!("brew reinstall postgresql@{to}")));
    }
    let (t, ext) = (target.clone(), cur.extensions.clone());
    blocking(move || check_extensions(&t, &ext)).await?;
    let old = ctx.install_major(cur.pg_major).await?;

    let new_name = as_name.unwrap_or(name);
    let (created, expected, gen) = reserve(ctx, &row, new_name, as_name.is_some(), cur.gen)?;
    let res = run(ctx, &old, &target, &cur, new_name, gen, expected, created).await;
    if let Err(e) = &res {
        crate::events::emit_template_failed(ctx, new_name, gen, "upgrade", e);
    }
    if res.is_err() && created {
        // A failed upgrade leaves no new template behind.
        let store = ctx.store();
        if matches!(store.list_generations(new_name), Ok(g) if g.is_empty()) {
            let _ = store.delete_template(new_name);
        }
    }
    res
}

fn exists(name: &str) -> MitosisError {
    invalid(format!("template {name} already exists"))
        .with_hint("pick another --as name, or upgrade in place without --as")
}

/// Under one store lock: creates the `--as` template (copy of `row`, schedule off) or reads the
/// pointer to expect, then allocates the generation. Returns `(created, expected, gen)`.
fn reserve(
    ctx: &Ctx,
    row: &TemplateRow,
    new_name: &str,
    create: bool,
    cur_gen: u32,
) -> Result<(bool, u32, u32)> {
    let store = ctx.store();
    if !create {
        return Ok((false, cur_gen, store.alloc_gen(new_name)?));
    }
    if store.get_template(new_name)?.is_some() {
        return Err(exists(new_name));
    }
    let mut t = TemplateRow::new(
        new_name,
        &row.database,
        row.source.clone(),
        row.migrations.clone(),
        row.max_age_secs,
    );
    t.keep = row.keep;
    t.every_mode = row.every_mode;
    t.migrate = row.migrate.clone();
    t.auto_migrate = row.auto_migrate;
    t.on_stale = row.on_stale;
    store.insert_template(&t)?;
    match store.alloc_gen(new_name) {
        Ok(gen) => Ok((true, 0, gen)),
        Err(e) => {
            let _ = store.delete_template(new_name);
            Err(e)
        }
    }
}

/// Paths one upgrade works in; all removed on failure.
struct Work {
    a: PathBuf,
    b: PathBuf,
    tmp: PathBuf,
    socket_dir: PathBuf,
    log: PathBuf,
}

#[allow(clippy::too_many_arguments)]
async fn run(
    ctx: &Ctx,
    old: &PgInstall,
    target: &PgInstall,
    cur: &GenerationRow,
    name: &str,
    gen: u32,
    expected: u32,
    created: bool,
) -> Result<TemplateInfo> {
    let mut ts = now();
    let dir = ctx.home.templates_dir();
    // Two side folders named like build folders, so gc removes them after a crash.
    while dir.join(build_dir_name(name, ts)).exists()
        || dir.join(build_dir_name(name, ts + 1)).exists()
    {
        ts += 2;
    }
    // Held until `run` ends: the servers listen on a socket only, so without the listeners a
    // concurrent build could pick the same port and share `sockets/build-<port>`.
    let (old_port, _old_hold) = hold_port(ctx, &HashSet::new())?;
    let (new_port, _new_hold) = hold_port(ctx, &HashSet::from([old_port]))?;
    let work = Work {
        a: dir.join(build_dir_name(name, ts)),
        b: dir.join(build_dir_name(name, ts + 1)),
        tmp: ctx.home.logs_dir().join(format!("upgrade-{name}-{ts}")),
        // Named like a build socket folder (by port, short), so gc knows it.
        socket_dir: ctx.home.sockets_dir().join(format!("build-{old_port}")),
        log: ctx.home.logs_dir().join(format!("template-{name}.log")),
    };
    std::fs::File::create(&work.log)?;
    let ports = (old_port, new_port);

    if let Err((step, e)) = steps(ctx, old, target, cur, &work, ports).await {
        return Err(fail(ctx, old, target, &work, step, e).await);
    }
    let cleaned = {
        let (a, tmp, s) = (work.a.clone(), work.tmp.clone(), work.socket_dir.clone());
        blocking(move || {
            remove(&tmp)?;
            let _ = std::fs::remove_dir(&s);
            remove(&a)
        })
        .await
    };
    if let Err(e) = cleaned {
        return Err(fail(ctx, old, target, &work, "cleanup", e).await);
    }

    let row = GenerationRow {
        template: name.to_string(),
        gen,
        kind: GenKind::Upgraded,
        dir: work.b.clone(),
        pg_major: 0,
        bin_dir: target.bin_dir.clone(),
        extensions: cur.extensions.clone(),
        migrations_hash: cur.migrations_hash.clone(),
        built_at: ts,
        size_bytes: 0,
        from_clone: None,
        // Only a generation of the same template names its parent.
        from_gen: (!created).then_some(cur.gen),
    };
    commit_built(ctx, target, &work.log, &[], row, expected).await?;
    if !created {
        // An upgrade is a manual build: it lifts a pause and clears the last error when current.
        let store = ctx.store();
        let mut t = store.get_template(name)?.ok_or_else(|| not_found(name))?;
        resume_if_current(&mut t, gen);
        store.update_template_settings(&t)?;
    }
    template_info(ctx, name)
}

/// A free port from the configured range, bound on 127.0.0.1 so no other build or clone takes it
/// while the listener lives. Retries when another process binds the picked port first.
fn hold_port(ctx: &Ctx, skip: &HashSet<u16>) -> Result<(u16, TcpListener)> {
    let (leased, reserved) = {
        let store = ctx.store();
        (store.leased_ports()?, store.reserved_ports()?)
    };
    let mut skip = skip.clone();
    loop {
        let port = pick_port(ctx.config().port_range, &leased, &skip, &reserved)?;
        match TcpListener::bind(("127.0.0.1", port)) {
            Ok(l) => return Ok((port, l)),
            // Lost the race for it; pick_port fails with ports_exhausted once none is left.
            Err(_) => {
                skip.insert(port);
            }
        }
    }
}

type StepResult<T> = std::result::Result<T, (&'static str, MitosisError)>;

fn at(step: &'static str) -> impl FnOnce(MitosisError) -> (&'static str, MitosisError) {
    move |e| (step, e)
}

/// Copy, read the cluster's settings, initdb, pg_upgrade (retried with `--copy` when cloning is
/// not supported).
async fn steps(
    ctx: &Ctx,
    old: &PgInstall,
    target: &PgInstall,
    cur: &GenerationRow,
    work: &Work,
    (old_port, new_port): (u16, u16),
) -> StepResult<()> {
    {
        // A commit or prune moves generation folders only under the write guard.
        let _swap = ctx.template_swap.read().await;
        let (b, f, d) = (ctx.backend.clone(), cur.dir.clone(), work.a.clone());
        blocking(move || b.clone_dir(&f, &d))
            .await
            .map_err(at("copy"))?;
    }
    let settings = read_settings(ctx, old, work, old_port)
        .await
        .map_err(at("read settings"))?;
    std::fs::create_dir_all(&work.tmp).map_err(|e| ("pg_upgrade", e.into()))?;
    for mode in ["--clone", "--copy"] {
        let (t, b, s) = (target.clone(), work.b.clone(), settings.clone());
        blocking(move || {
            remove(&b)?;
            initdb_like(&t, &b, &s)
        })
        .await
        .map_err(at("initdb"))?;
        let (o, t, a, b, tmp, sock, groups) = (
            old.clone(),
            target.clone(),
            work.a.clone(),
            work.b.clone(),
            work.tmp.clone(),
            work.socket_dir.clone(),
            ctx.build_groups.clone(),
        );
        let out = blocking(move || {
            pg_upgrade(
                &o,
                &t,
                &a,
                &b,
                &tmp,
                &sock,
                (old_port, new_port),
                mode,
                &groups,
            )
        })
        .await
        .map_err(at("pg_upgrade"))?;
        append_log(&work.log, &out.text);
        if out.ok {
            // pg_upgrade leaves the new server's last command line behind.
            return match std::fs::remove_file(work.b.join("postmaster.opts")) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(("pg_upgrade", e.into())),
                _ => Ok(()),
            };
        }
        let lower = out.text.to_lowercase();
        if mode == "--clone" && lower.contains("clon") && lower.contains("not supported") {
            append_log(
                &work.log,
                "mitosis: file cloning not supported, retrying with --copy",
            );
            continue;
        }
        return Err(("pg_upgrade", internal("pg_upgrade failed")));
    }
    Err(("pg_upgrade", internal("pg_upgrade failed")))
}

/// What `initdb` of the new cluster must match for `pg_upgrade` to accept it.
#[derive(Clone, Debug, PartialEq)]
struct ClusterSettings {
    encoding: String,
    collate: String,
    ctype: String,
    /// `c` libc, `i` icu, `b` builtin (`datlocprovider`).
    provider: String,
    /// ICU or builtin locale, when the provider has one.
    locale: Option<String>,
    checksums: bool,
}

const SETTINGS_SQL: &str = "SELECT current_setting('data_checksums'), \
     pg_encoding_to_char(d.encoding), row_to_json(d) FROM pg_database d \
     WHERE datname = 'template0'";

/// Parses one `SETTINGS_SQL` output line. `row_to_json` keeps it working across majors:
/// `datlocprovider` exists from 15, `daticulocale` in 15-16, `datlocale` from 17.
fn parse_settings(line: &str) -> Result<ClusterSettings> {
    let bad = || internal(format!("unexpected cluster settings: {line}"));
    let mut parts = line.splitn(3, '|');
    let (Some(checksums), Some(encoding), Some(json)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(bad());
    };
    let row: serde_json::Value = serde_json::from_str(json).map_err(|_| bad())?;
    let text = |k: &str| row.get(k).and_then(|v| v.as_str()).map(String::from);
    Ok(ClusterSettings {
        encoding: encoding.to_string(),
        collate: text("datcollate").ok_or_else(bad)?,
        ctype: text("datctype").ok_or_else(bad)?,
        provider: text("datlocprovider").unwrap_or_else(|| "c".into()),
        locale: text("datlocale").or_else(|| text("daticulocale")),
        checksums: checksums == "on",
    })
}

/// Starts copy A privately on the old major, reads its settings, stops it cleanly.
async fn read_settings(
    ctx: &Ctx,
    old: &PgInstall,
    work: &Work,
    port: u16,
) -> Result<ClusterSettings> {
    let opts = StartOptions {
        port,
        socket_dir: work.socket_dir.clone(),
        listen_tcp: false,
        durable: false,
        log_file: work.log.clone(),
    };
    // No PortInUse retry: the port is held by `hold_port` and the server opens no TCP listener.
    let pg = start(old, &work.a, &opts)?;
    let ready = wait_ready(old, &pg, &opts, ctx.config().ready_timeout).await;
    let read = match ready {
        Ok(()) => {
            let (i, s) = (old.clone(), opts.socket_dir.clone());
            blocking(move || psql(&i, &s, port, "postgres", SETTINGS_SQL)).await
        }
        Err(e) => Err(e),
    };
    let (i, d, t) = (old.clone(), work.a.clone(), ctx.config().stop_timeout);
    blocking(move || stop(&i, &d, Some(pg), t)).await?;
    parse_settings(read?.lines().next().unwrap_or_default())
}

/// The `initdb` arguments that recreate `s` (cluster.rs `initdb` plus encoding/locale/checksums).
fn initdb_args(s: &ClusterSettings, target_major: u32) -> Vec<String> {
    let mut args: Vec<String> = [
        "-U",
        "postgres",
        "--auth-local=trust",
        "--auth-host=scram-sha-256",
    ]
    .map(String::from)
    .to_vec();
    args.push(format!("--encoding={}", s.encoding));
    args.push(format!("--lc-collate={}", s.collate));
    args.push(format!("--lc-ctype={}", s.ctype));
    match (s.provider.as_str(), &s.locale) {
        ("i", Some(l)) => {
            args.push("--locale-provider=icu".into());
            args.push(format!("--icu-locale={l}"));
        }
        ("b", Some(l)) => {
            args.push("--locale-provider=builtin".into());
            args.push(format!("--builtin-locale={l}"));
        }
        _ if target_major >= 15 => args.push("--locale-provider=libc".into()),
        _ => {}
    }
    if s.checksums {
        args.push("--data-checksums".into());
    } else if target_major >= 18 {
        // Postgres 18 turns checksums on by default; pg_upgrade needs both sides equal.
        args.push("--no-data-checksums".into());
    }
    args
}

fn initdb_like(install: &PgInstall, data_dir: &Path, s: &ClusterSettings) -> Result<()> {
    let out = Command::new(install.bin("initdb"))
        .arg("-D")
        .arg(data_dir)
        .args(initdb_args(s, install.major))
        .output()?;
    if !out.status.success() {
        return Err(internal(format!(
            "initdb failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(())
}

struct UpgradeOutput {
    ok: bool,
    text: String,
}

/// Runs `pg_upgrade` in its own process group (killed on daemon shutdown), cwd `tmp`.
#[allow(clippy::too_many_arguments)]
fn pg_upgrade(
    old: &PgInstall,
    target: &PgInstall,
    a: &Path,
    b: &Path,
    tmp: &Path,
    socket_dir: &Path,
    (old_port, new_port): (u16, u16),
    mode: &str,
    groups: &Arc<Mutex<HashSet<i32>>>,
) -> Result<UpgradeOutput> {
    std::fs::create_dir_all(socket_dir)?;
    let child = Command::new(target.pg_upgrade())
        .arg("-b")
        .arg(&old.bin_dir)
        .arg("-B")
        .arg(&target.bin_dir)
        .arg("-d")
        .arg(a)
        .arg("-D")
        .arg(b)
        .args(["-U", "postgres", mode, "--socketdir"])
        .arg(socket_dir)
        .args(["-p", &old_port.to_string(), "-P", &new_port.to_string()])
        // macOS postgres aborts without a locale in the environment (see `pg::start`).
        .env("LC_ALL", "C")
        .current_dir(tmp)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| internal(format!("could not run pg_upgrade: {e}")))?;
    let pgid = child.id() as i32;
    let lock = || groups.lock().unwrap_or_else(|e| e.into_inner());
    lock().insert(pgid);
    let out = child.wait_with_output();
    lock().remove(&pgid);
    let out = out.map_err(|e| internal(format!("waiting for pg_upgrade: {e}")))?;
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    if !out.status.success() {
        text.push_str(&format!("pg_upgrade exited with {}\n", out.status));
        // Its detailed logs live in the new cluster (15+) or the cwd; both are removed next.
        for root in [b.join("pg_upgrade_output.d"), tmp.to_path_buf()] {
            log_files(&root, &mut text);
        }
    }
    Ok(UpgradeOutput {
        ok: out.status.success(),
        text,
    })
}

/// Appends the last lines of each `.log`/`.txt` file under `root` to `text`.
fn log_files(root: &Path, text: &mut String) {
    let Ok(rd) = std::fs::read_dir(root) else {
        return;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            log_files(&path, text);
        } else if path.extension().is_some_and(|e| e == "log" || e == "txt") {
            let body = std::fs::read(&path).unwrap_or_default();
            let body = String::from_utf8_lossy(&body);
            let lines: Vec<&str> = body.lines().collect();
            text.push_str(&format!("--- {}\n", path.display()));
            text.push_str(&lines[lines.len().saturating_sub(40)..].join("\n"));
            text.push('\n');
        }
    }
}

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

fn remove(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

/// Stops anything left running in A or B, removes A, B, the temp and socket folders, and turns
/// `e` into `TemplateBuildFailed`. A folder whose server would not stop is left for gc.
async fn fail(
    ctx: &Ctx,
    old: &PgInstall,
    target: &PgInstall,
    work: &Work,
    step: &str,
    e: MitosisError,
) -> MitosisError {
    append_log(&work.log, &format!("mitosis: {step} failed: {e}"));
    let tail = redact_url(&log_tail(&work.log, 20));
    let (o, t, timeout) = (old.clone(), target.clone(), ctx.config().stop_timeout);
    let (a, b, tmp, sock) = (
        work.a.clone(),
        work.b.clone(),
        work.tmp.clone(),
        work.socket_dir.clone(),
    );
    let cleanup = blocking(move || {
        let mut errors = Vec::new();
        for (install, dir) in [(&o, &a), (&t, &b)] {
            let res = stop(install, dir, None, timeout).and_then(|()| remove(dir));
            if let Err(e) = res {
                errors.push(format!("{}: {e}", dir.display()));
            }
        }
        if let Err(e) = remove(&tmp) {
            errors.push(format!("{}: {e}", tmp.display()));
        }
        let _ = std::fs::remove_dir(&sock);
        match errors.is_empty() {
            true => Ok(()),
            false => Err(internal(errors.join("; "))),
        }
    })
    .await;
    if let Err(ce) = cleanup {
        append_log(&work.log, &format!("mitosis: cleanup failed: {ce}"));
    }
    MitosisError::new(
        ErrorCode::TemplateBuildFailed,
        format!("template upgrade failed at {step}\n{tail}"),
    )
    .with_hint(format!("full log: {}", work.log.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(provider: &str, locale: Option<&str>, checksums: bool) -> ClusterSettings {
        ClusterSettings {
            encoding: "UTF8".into(),
            collate: "en_US.UTF-8".into(),
            ctype: "en_US.UTF-8".into(),
            provider: provider.into(),
            locale: locale.map(String::from),
            checksums,
        }
    }

    #[test]
    fn parses_settings_across_majors() {
        // 18: datlocale
        let s = parse_settings(
            r#"off|UTF8|{"datcollate":"en_US.UTF-8","datctype":"en_US.UTF-8","datlocprovider":"i","datlocale":"und"}"#,
        )
        .unwrap();
        assert_eq!(s, settings("i", Some("und"), false));
        // 16: daticulocale
        let s = parse_settings(
            r#"on|UTF8|{"datcollate":"en_US.UTF-8","datctype":"en_US.UTF-8","datlocprovider":"i","daticulocale":"und"}"#,
        )
        .unwrap();
        assert_eq!(s, settings("i", Some("und"), true));
        // 14: no provider column
        let s = parse_settings(r#"off|UTF8|{"datcollate":"en_US.UTF-8","datctype":"en_US.UTF-8"}"#)
            .unwrap();
        assert_eq!(s, settings("c", None, false));
        assert!(parse_settings("garbage").is_err());
    }

    #[test]
    fn initdb_args_match_the_old_cluster() {
        let a = initdb_args(&settings("c", None, false), 18);
        assert!(a.contains(&"--no-data-checksums".to_string()));
        assert!(a.contains(&"--locale-provider=libc".to_string()));
        assert!(a.contains(&"--lc-collate=en_US.UTF-8".to_string()));
        let a = initdb_args(&settings("i", Some("und"), true), 17);
        assert!(a.contains(&"--data-checksums".to_string()));
        assert!(a.contains(&"--icu-locale=und".to_string()));
        assert!(!a.iter().any(|x| x == "--no-data-checksums"));
        let a = initdb_args(&settings("b", Some("C.UTF-8"), false), 17);
        assert!(a.contains(&"--builtin-locale=C.UTF-8".to_string()));
    }

    // The daemon spawns this future on a multi-thread runtime, so it must be Send.
    #[allow(dead_code)]
    fn futures_are_send(ctx: &'static Arc<Ctx>) {
        fn send<T: Send>(_: T) {}
        send(upgrade(ctx, "x", 18, None));
    }
}
