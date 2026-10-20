//! Automatic template builds: interval schedules, auto-migrate, and the background refresh a clone
//! of a stale template asks for. Every build runs on its own task and holds the template's build
//! slot, so there is at most one build per template at a time.

use crate::ctx::Ctx;
use crate::error::Result;
use crate::protocol::{RefreshMode, TemplateSource};
use crate::state::TemplateRow;
use crate::templates::{self, hash_dir, now};
use std::sync::Arc;
use tokio::sync::watch;

/// After a failed background build, how long auto-migrate and stale refreshes wait before the next
/// try when the template has no `--every` interval.
const RETRY_SECS: u64 = 3_600;

/// Events older than this are dropped by the scheduler.
const EVENT_KEEP_SECS: i64 = 30 * 24 * 3600;
/// The event log keeps at most this many rows, the newest.
const EVENT_KEEP_ROWS: u32 = 50_000;

fn secs(s: u64) -> i64 {
    i64::try_from(s).unwrap_or(i64::MAX)
}

/// The mode of the interval refresh `row` needs at `now`, if any. Counts from `newest_built_at`
/// (the newest generation, current or not); after an attempt, waits a whole interval again.
pub fn due(row: &TemplateRow, newest_built_at: i64, now: i64) -> Option<RefreshMode> {
    if row.paused || full_refresh_refused(row) {
        return None;
    }
    let every = secs(row.every_secs?);
    let ready = now >= newest_built_at.saturating_add(every)
        && row
            .last_attempt_at
            .is_none_or(|a| now >= a.saturating_add(every));
    ready.then_some(row.every_mode)
}

/// A promoted template has no source, so its interval refresh and its stale refresh must be
/// layered; a full one would fail at every interval.
pub(crate) fn full_refresh_refused(row: &TemplateRow) -> bool {
    matches!(row.source, TemplateSource::Promoted { .. }) && row.every_mode == RefreshMode::Full
}

/// True when auto-migrate should run a layered refresh: the migrations folder no longer matches
/// the newest generation.
pub fn auto_migrate_due(
    row: &TemplateRow,
    newest_hash: Option<&str>,
    folder_hash: Option<&str>,
) -> bool {
    row.auto_migrate && !row.paused && folder_hash != newest_hash
}

/// False while a failed background build is younger than the retry interval (`every_secs`, else
/// one hour), so a broken source is not rebuilt on every tick or clone.
pub(crate) fn may_retry(row: &TemplateRow, now: i64) -> bool {
    let wait = secs(row.every_secs.unwrap_or(RETRY_SECS));
    row.last_error.is_none()
        || row
            .last_attempt_at
            .is_none_or(|a| now >= a.saturating_add(wait))
}

/// Starts a background build of `name` in `mode` on its own task; false when a build of it runs.
pub(crate) fn start(ctx: &Arc<Ctx>, name: &str, mode: RefreshMode) -> bool {
    if !templates::claim_build(ctx, name) {
        return false;
    }
    let (ctx, name) = (ctx.clone(), name.to_string());
    tokio::spawn(async move { templates::background(&ctx, &name, mode).await });
    true
}

/// The builds due at `now`, at most one per template: an interval refresh in its mode, else an
/// auto-migrate layered refresh. Templates with a build running are left out.
fn plan(ctx: &Ctx, now: i64) -> Result<Vec<(String, RefreshMode)>> {
    let rows = {
        let store = ctx.store();
        let mut rows = Vec::new();
        for t in store.list_templates()? {
            let newest = store
                .list_generations(&t.name)?
                .into_iter()
                .max_by_key(|g| g.built_at);
            if let Some(g) = newest {
                rows.push((t, g));
            }
        }
        rows
    };
    let mut out = Vec::new();
    for (t, newest) in rows {
        if templates::is_building(ctx, &t.name) {
            continue;
        }
        if let Some(mode) = due(&t, newest.built_at, now) {
            out.push((t.name, mode));
            continue;
        }
        if !t.auto_migrate || t.paused || !may_retry(&t, now) {
            continue;
        }
        // A missing or unreadable folder is not a change.
        let Some(folder) = t
            .migrations
            .as_deref()
            .filter(|d| d.is_dir())
            .and_then(|d| hash_dir(d).ok())
        else {
            continue;
        };
        if auto_migrate_due(&t, newest.migrations_hash.as_deref(), Some(&folder)) {
            out.push((t.name, RefreshMode::Layered));
        }
    }
    Ok(out)
}

/// Starts every build due at `now` (each on its own task, skipping templates with a build
/// running), then prunes every template, so a lowered `--keep` applies without a new build.
pub async fn tick(ctx: &Arc<Ctx>, now: i64) {
    let pruned = ctx
        .store()
        .prune_events(now.saturating_sub(EVENT_KEEP_SECS), EVENT_KEEP_ROWS);
    if let Err(e) = pruned {
        crate::lifecycle::log(ctx, &format!("scheduler: prune events: {e}"));
    }
    let c = ctx.clone();
    // Hashing migration folders reads files: off the runtime.
    match tokio::task::spawn_blocking(move || plan(&c, now)).await {
        Ok(Ok(builds)) => {
            for (name, mode) in builds {
                start(ctx, &name, mode);
            }
        }
        Ok(Err(e)) => crate::lifecycle::log(ctx, &format!("scheduler: {e}")),
        Err(e) => crate::lifecycle::log(ctx, &format!("scheduler: background task failed: {e}")),
    }
    let names: Vec<String> = match ctx.store().list_templates() {
        Ok(ts) => ts.into_iter().map(|t| t.name).collect(),
        Err(e) => {
            return crate::lifecycle::log(ctx, &format!("scheduler: list templates: {e}"));
        }
    };
    for name in names {
        templates::prune(ctx, &name).await;
    }
}

/// Runs `tick` right away (so a refresh that came due while the daemon was down runs once), then
/// every `scheduler_tick`, until `shutdown` turns true or its sender is dropped.
pub fn run(ctx: Arc<Ctx>, mut shutdown: watch::Receiver<bool>) {
    tokio::spawn(async move {
        loop {
            if *shutdown.borrow() {
                return;
            }
            tick(&ctx, now()).await;
            let every = ctx.config().scheduler_tick;
            tokio::select! {
                // An error means the sender is gone: the daemon has stopped.
                changed = shutdown.changed() => if changed.is_err() { return },
                _ = tokio::time::sleep(every) => {}
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx::Config;
    use crate::paths::Home;
    use crate::protocol::{GenKind, TemplateSource};
    use crate::state::GenerationRow;
    use std::path::PathBuf;

    const NOW: i64 = 1_700_000_000;

    fn row() -> TemplateRow {
        TemplateRow::new(
            "main",
            "app",
            TemplateSource::Seed {
                command: "true".into(),
                cwd: None,
                path: None,
            },
            None,
            604_800,
        )
    }

    fn every(secs: u64, mode: RefreshMode) -> TemplateRow {
        TemplateRow {
            every_secs: Some(secs),
            every_mode: mode,
            ..row()
        }
    }

    fn gen(gen: u32, built_at: i64) -> GenerationRow {
        GenerationRow {
            template: "main".into(),
            gen,
            kind: GenKind::Full,
            dir: PathBuf::from(format!("/h/templates/main@{gen}")),
            pg_major: 16,
            bin_dir: PathBuf::from("/opt/pg/bin"),
            extensions: vec![],
            migrations_hash: None,
            built_at,
            size_bytes: 1,
            from_clone: None,
            from_gen: None,
            mask_hash: None,
        }
    }

    #[test]
    fn due_counts_from_newest_and_respects_pause_and_attempts() {
        let layered = every(60, RefreshMode::Layered);
        let failed = |at: i64| TemplateRow {
            last_attempt_at: Some(at),
            last_error: Some("boom".into()),
            ..every(60, RefreshMode::Full)
        };
        let cases: [(&str, TemplateRow, i64, Option<RefreshMode>); 7] = [
            ("no schedule", row(), NOW - 1_000, None),
            (
                "paused",
                TemplateRow {
                    paused: true,
                    ..layered.clone()
                },
                NOW - 1_000,
                None,
            ),
            ("not yet due", layered.clone(), NOW - 59, None),
            (
                "due exactly",
                layered.clone(),
                NOW - 60,
                Some(RefreshMode::Layered),
            ),
            (
                "overdue",
                layered.clone(),
                NOW - 1_000,
                Some(RefreshMode::Layered),
            ),
            ("retried too soon", failed(NOW - 30), NOW - 1_000, None),
            (
                "retry after an interval",
                failed(NOW - 60),
                NOW - 1_000,
                Some(RefreshMode::Full),
            ),
        ];
        for (what, r, newest, want) in cases {
            assert_eq!(due(&r, newest, NOW), want, "{what}");
        }

        // The plan counts from the newest generation, even when it is not the current one.
        let tmp = tempfile::tempdir().unwrap();
        let ctx = Ctx::open(Home::at(tmp.path().join("m")), Config::default()).unwrap();
        let mut t = every(60, RefreshMode::Full);
        t.current_gen = 1;
        t.next_gen = 3;
        let store = ctx.store();
        store.insert_template(&t).unwrap();
        store.insert_generation(&gen(1, NOW - 1_000)).unwrap();
        store.insert_generation(&gen(2, NOW - 10)).unwrap();
        drop(store);
        assert!(plan(&ctx, NOW).unwrap().is_empty());
        assert_eq!(
            plan(&ctx, NOW + 50).unwrap(),
            vec![("main".to_string(), RefreshMode::Full)]
        );
        // A running build is never joined by a second one.
        assert!(templates::claim_build(&ctx, "main"));
        assert!(plan(&ctx, NOW + 50).unwrap().is_empty());
    }

    #[test]
    fn auto_migrate_due_cases() {
        let on = TemplateRow {
            auto_migrate: true,
            ..row()
        };
        assert!(auto_migrate_due(&on, Some("a"), Some("b")));
        assert!(auto_migrate_due(&on, None, Some("b")));
        assert!(!auto_migrate_due(&on, Some("a"), Some("a")));
        assert!(!auto_migrate_due(&row(), Some("a"), Some("b")));
        let paused = TemplateRow {
            paused: true,
            ..on.clone()
        };
        assert!(!auto_migrate_due(&paused, Some("a"), Some("b")));
    }

    #[test]
    fn may_retry_waits_an_interval_after_a_failure() {
        assert!(may_retry(&row(), NOW));
        let failed = |every: Option<u64>| TemplateRow {
            every_secs: every,
            last_attempt_at: Some(NOW - 100),
            last_error: Some("boom".into()),
            ..row()
        };
        assert!(may_retry(&failed(Some(60)), NOW));
        assert!(!may_retry(&failed(Some(3_600)), NOW));
        assert!(!may_retry(&failed(None), NOW));
        assert!(may_retry(&failed(None), NOW + 3_600));
        // A success clears the error, so an old attempt never holds a build back.
        let ok = TemplateRow {
            last_error: None,
            ..failed(None)
        };
        assert!(may_retry(&ok, NOW));
    }
}
