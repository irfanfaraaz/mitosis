//! Promote: turn a running clone into a new generation of a template. The clone's Postgres is
//! stopped cleanly, its folder is copied with a copy-on-write clone, and the clone is started again on the same
//! port, socket and password, so its URL keeps working (open connections are dropped).

use crate::clones::{mark, stop_and_remove};
use crate::ctx::{Ctx, InFlightOp};
use crate::error::{ErrorCode, MitosisError, Result};
use crate::events::reason;
use crate::names::validate_name;
use crate::pg::{
    psql, read_pg_version, running_server, start, stop, wait_ready, PgInstall, RunningPg,
    StartOptions,
};
use crate::proc_util::process_start_time;
use crate::protocol::{GenKind, TemplateInfo, TemplateSource};
use crate::state::{CloneRow, CloneStatus, GenerationRow, TemplateRow};
use crate::templates::{
    build_dir_name, build_log_path, commit_built, not_found, now, resume_if_current, template_info,
    BuildGuard,
};

fn invalid(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::InvalidRequest, msg)
}

/// Runs blocking work (Postgres tools, big file operations) off the async runtime.
async fn blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(|e| {
        MitosisError::new(ErrorCode::Internal, format!("background task failed: {e}"))
    })?
}

/// Makes the running clone `id_or_name` the newest generation of `target` (default: the clone's
/// own template). A new `target` name creates a template with source `Promoted`.
/// The clone is marked in flight and `target` is held as being built for the whole operation.
pub async fn promote(ctx: &Ctx, id_or_name: &str, target: Option<&str>) -> Result<TemplateInfo> {
    if let Some(t) = target {
        validate_name("template", t)?;
    }
    let clone_gone = |id: &str| {
        MitosisError::new(ErrorCode::CloneNotFound, format!("clone {id} not found"))
            .with_hint("see existing clones with `mitosis list` (or the clone_list tool)")
    };
    let found = ctx
        .store()
        .get_clone(id_or_name)?
        .ok_or_else(|| clone_gone(id_or_name))?;
    // The same mark a create holds: reap, gc and destroy leave the clone alone until it drops.
    let _mark = mark(ctx, &found.id, InFlightOp::Create).map_err(|_| {
        invalid(format!(
            "clone {} has an operation running; try again in a moment",
            found.id
        ))
    })?;
    // A destroy may have finished between the lookup and the mark.
    let clone = ctx
        .store()
        .get_clone(&found.id)?
        .filter(|r| r.id == found.id)
        .ok_or_else(|| clone_gone(&found.id))?;
    if clone.status != CloneStatus::Ready {
        return Err(invalid(format!(
            "clone {} is not ready (it is {})",
            clone.id,
            clone.status.as_str()
        ))
        .with_hint(match clone.status {
            CloneStatus::Sleeping => "connect to it to wake it, then promote",
            _ => "wait for the clone to finish its current operation, then promote",
        }));
    }
    // Possibly corrupt data must not become what every new clone copies.
    if clone.suspect {
        let n = clone.name.as_deref().unwrap_or(&clone.id);
        return Err(invalid(format!(
            "clone {n} may hold lost or corrupt writes after an unclean shutdown"
        ))
        .with_hint(format!(
            "run `mitosis clone --name {n} --fresh` first, or destroy it"
        )));
    }
    let name = target.unwrap_or(&clone.template).to_string();
    // A branch template's generations are layers on its base's; a promoted one would match no
    // `base_gen`.
    if name.contains('~') {
        return Err(invalid(format!(
            "clone {} comes from branch template {name}, which cannot take a promoted generation",
            clone.name.as_deref().unwrap_or(&clone.id)
        ))
        .with_hint("promote into a plain template: use --template <template>"));
    }
    let _guard = BuildGuard::take(ctx, &name)?;

    let dir = clone.dir.clone();
    let major = blocking(move || read_pg_version(&dir)).await?;
    let install = ctx.install_major(major).await?;
    let (created, expected, gen, migrations_hash) = reserve(ctx, &clone, &name, major)?;
    let res = run(ctx, &install, &clone, &name, expected, gen, migrations_hash).await;
    if res.is_err() && created {
        // A failed first generation leaves no template behind.
        let store = ctx.store();
        if matches!(store.list_generations(&name), Ok(g) if g.is_empty()) {
            let _ = store.delete_template(&name);
        }
    }
    res
}

/// Checks the target and allocates the generation number, under one store lock. Returns
/// `(template was created, current gen to expect at commit, new gen, migrations hash to carry)`.
fn reserve(
    ctx: &Ctx,
    clone: &CloneRow,
    name: &str,
    major: u32,
) -> Result<(bool, u32, u32, Option<String>)> {
    let store = ctx.store();
    let (created, expected) = match store.get_template(name)? {
        Some(t) => {
            if t.database != clone.database {
                return Err(invalid(format!(
                    "template {name} uses database {}, the clone uses {}",
                    t.database, clone.database
                ))
                .with_hint(
                    "promote into a template on the same database, or use a new --template name",
                ));
            }
            if let Some((_, cur)) = store.current(name)? {
                if cur.pg_major != major {
                    return Err(invalid(format!(
                        "template {name} is on Postgres {}, the clone runs {major}",
                        cur.pg_major
                    ))
                    .with_hint(
                        "promote into a template on the same major, or use a new --template name",
                    ));
                }
            }
            (false, t.current_gen)
        }
        None => {
            store.insert_template(&TemplateRow::new(
                name,
                &clone.database,
                TemplateSource::Promoted {
                    clone_id: clone.id.clone(),
                },
                None,
                ctx.config().default_max_age_secs,
            ))?;
            (true, 0)
        }
    };
    let gen = match store.alloc_gen(name) {
        Ok(g) => g,
        Err(e) => {
            if created {
                let _ = store.delete_template(name);
            }
            return Err(e);
        }
    };
    // Promoting into the clone's own template keeps the migrations state it was built from.
    let hash = match clone.template == name {
        true => store
            .get_generation(name, clone.generation)?
            .and_then(|g| g.migrations_hash),
        false => None,
    };
    Ok((created, expected, gen, hash))
}

async fn run(
    ctx: &Ctx,
    install: &PgInstall,
    clone: &CloneRow,
    name: &str,
    expected: u32,
    gen: u32,
    migrations_hash: Option<String>,
) -> Result<TemplateInfo> {
    let timeout = ctx.config().stop_timeout;
    let running = clone
        .pg_pid
        .zip(clone.pg_start_time)
        .map(|(pid, start_time)| RunningPg { pid, start_time });

    // Read while the server runs; also proves it does, so a crashed clone is never copied.
    let (i, s, p, db) = (
        install.clone(),
        clone.socket_dir.clone(),
        clone.port,
        clone.database.clone(),
    );
    let out = blocking(move || {
        psql(
            &i,
            &s,
            p,
            &db,
            "SELECT extname FROM pg_extension ORDER BY extname",
        )
    })
    .await?;
    let extensions: Vec<String> = out
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect();

    let log = build_log_path(&ctx.home, name);
    std::fs::File::create(&log)?;
    let (i, d) = (install.clone(), clone.dir.clone());
    blocking(move || {
        if running_server(&d, running)?.is_none() {
            return Err(invalid("the clone's Postgres is not running"));
        }
        stop(&i, &d, running, timeout)
    })
    .await?;

    // From here the clone is down: every path below starts it again (or reconcile does).
    let built_at = now();
    let side = ctx
        .home
        .templates_dir()
        .join(build_dir_name(name, built_at));
    let (b, src, dst) = (ctx.backend.clone(), clone.dir.clone(), side.clone());
    let copied = blocking(move || {
        b.clone_dir(&src, &dst)?;
        for file in ["postmaster.pid", "postmaster.opts"] {
            match std::fs::remove_file(dst.join(file)) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
                _ => {}
            }
        }
        Ok(())
    })
    .await;
    if copied.is_err() {
        let (b, s) = (ctx.backend.clone(), side.clone());
        let _ = blocking(move || b.remove_dir(&s)).await;
    }

    // Committed before the clone restarts: a kill after this point cannot lose the promoted data,
    // whatever reconcile then does to the stopped clone.
    let committed = match copied {
        Ok(()) => {
            let row = GenerationRow {
                template: name.to_string(),
                gen,
                kind: GenKind::Promoted,
                dir: side,
                pg_major: 0,
                bin_dir: install.bin_dir.clone(),
                extensions,
                migrations_hash,
                built_at,
                size_bytes: 0,
                from_clone: Some(clone.id.clone()),
                from_gen: (clone.template == name).then_some(clone.generation),
                mask_hash: None,
            };
            commit_built(ctx, install, &log, &[], row, expected).await
        }
        Err(e) => Err(e),
    };
    // The clone that cannot restart is destroyed; the copy is already committed.
    let lost = match restart(ctx, install, clone).await {
        Ok(()) => None,
        Err(e) => {
            let row = ctx
                .store()
                .get_clone(&clone.id)
                .ok()
                .flatten()
                .filter(|r| r.id == clone.id)
                .unwrap_or_else(|| clone.clone());
            let removed = match stop_and_remove(ctx, &row, reason::FAILED).await {
                Ok(()) => "it was destroyed".to_string(),
                Err(re) => format!("it could not be removed ({re}); run `mitosis gc`"),
            };
            Some((
                e.code,
                format!(
                    "clone {} could not be restarted after the promote ({}) and {removed}",
                    clone.id, e.message
                ),
            ))
        }
    };

    match (committed, lost) {
        (Ok(()), lost) => {
            // A promote is a manual build: it lifts a pause and clears the last error when current.
            let resumed = (|| {
                let store = ctx.store();
                let mut t = store.get_template(name)?.ok_or_else(|| not_found(name))?;
                resume_if_current(&mut t, gen);
                store.update_template_settings(&t)
            })();
            match lost {
                None => {
                    // The generation is in; a failed resume only leaves the pause as it was.
                    if let Err(e) = resumed {
                        crate::lifecycle::log(ctx, &format!("promote {name}: resume: {e}"));
                    }
                    template_info(ctx, name)
                }
                Some((code, msg)) => Err(MitosisError::new(
                    code,
                    format!("{msg}; template {name} generation {gen} was committed"),
                )),
            }
        }
        (Err(e), None) => Err(e),
        (Err(mut e), Some((_, msg))) => {
            e.message = format!("{}\n{msg}", e.message);
            Err(e)
        }
    }
}

/// Starts the clone's Postgres again on its port and socket and records the new process.
async fn restart(ctx: &Ctx, install: &PgInstall, clone: &CloneRow) -> Result<()> {
    let opts = StartOptions {
        port: clone.port,
        socket_dir: clone.socket_dir.clone(),
        listen_tcp: true,
        durable: clone.durable,
        log_file: ctx.home.logs_dir().join(format!("{}.log", clone.id)),
    };
    let pg = start(install, &clone.dir, &opts)?;
    // Recorded before the wait, so a restart that never gets ready stops exactly this server.
    {
        let store = ctx.store();
        let mut row = store
            .get_clone(&clone.id)?
            .filter(|r| r.id == clone.id)
            .ok_or_else(|| {
                MitosisError::new(
                    ErrorCode::CloneNotFound,
                    format!("clone {} no longer exists", clone.id),
                )
            })?;
        row.pg_pid = Some(pg.pid);
        row.pg_start_time = Some(process_start_time(pg.pid).unwrap_or(pg.start_time));
        store.update_clone(&row)?;
    }
    wait_ready(install, &pg, &opts, ctx.config().ready_timeout).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // The daemon spawns this future on a multi-thread runtime, so it must be Send.
    #[allow(dead_code)]
    fn futures_are_send(ctx: &'static Arc<Ctx>) {
        fn send<T: Send>(_: T) {}
        send(promote(ctx, "x", None));
    }

    #[tokio::test]
    async fn promote_names_the_status_of_a_clone_that_is_not_ready() {
        let tmp = tempfile::tempdir().unwrap();
        let home = crate::paths::Home::at(tmp.path().to_path_buf());
        let ctx = Ctx::open(home, crate::ctx::Config::default()).unwrap();
        let row = CloneRow {
            id: "c0a1b2".into(),
            name: None,
            template: "main".into(),
            dir: ctx.home.clones_dir().join("c0a1b2"),
            port: 54321,
            socket_dir: ctx.home.sockets_dir().join("c0a1b2"),
            password: "pw".into(),
            database: "app".into(),
            pg_pid: None,
            pg_start_time: None,
            parent_pid: None,
            parent_start_time: None,
            created_at: 0,
            expires_at: i64::MAX,
            durable: false,
            status: CloneStatus::Sleeping,
            generation: 1,
            protected: false,
            suspect: false,
        };
        ctx.store().insert_clone(&row).unwrap();
        let e = promote(&ctx, "c0a1b2", None).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidRequest);
        assert!(e.message.contains("it is sleeping"), "{}", e.message);
        assert!(e.hint.unwrap_or_default().contains("wake"));
    }
}
