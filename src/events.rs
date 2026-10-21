//! The event log: what the daemon did and why. Every action site calls `emit`; `mitosis events`
//! lists the rows and `--follow` streams them. An event never holds a password or a URL with
//! one: build `detail` from known fields only, never from free text, and run any URL through
//! `templates::redact_url`.

use crate::ctx::Ctx;
use crate::error::MitosisError;
use crate::protocol::{EventFilter, EventRow};
use crate::state::CloneRow;
use serde_json::{json, Value};

pub const CLONE_CREATED: &str = "clone.created";
pub const CLONE_DESTROYED: &str = "clone.destroyed";
pub const CLONE_SLEPT: &str = "clone.slept";
pub const CLONE_WOKE: &str = "clone.woke";
pub const CLONE_SUSPECT: &str = "clone.suspect";
pub const TEMPLATE_BUILT: &str = "template.built";
pub const TEMPLATE_FAILED: &str = "template.failed";
pub const TEMPLATE_ROLLED_BACK: &str = "template.rolled_back";
pub const TEMPLATE_PROMOTED: &str = "template.promoted";
pub const TEMPLATE_UPGRADED: &str = "template.upgraded";
pub const TEMPLATE_PRUNED: &str = "template.pruned";
/// A build that was not needed (emitted by the branch-template code).
pub const TEMPLATE_SKIPPED: &str = "template.skipped";
/// A generation (or clone) folder gc could not remove because clones of it still exist (ZFS);
/// `detail.reason` is `clones_depend`. gc tries again on its next run.
pub const GENERATION_KEPT: &str = "generation_kept";
pub const QUEUE_WAITED: &str = "queue.waited";
pub const QUEUE_TIMEOUT: &str = "queue.timeout";
pub const ROUTER_STARTED: &str = "router.started";
pub const ROUTER_FAILED: &str = "router.failed";
pub const GC: &str = "gc";
pub const RECOVERY: &str = "recovery";

/// Values of `detail.reason` on `clone.destroyed`.
pub mod reason {
    pub const USER: &str = "user";
    pub const TTL: &str = "ttl";
    pub const PARENT: &str = "parent";
    pub const IDLE: &str = "idle";
    pub const GC: &str = "gc";
    pub const RECOVERY: &str = "recovery";
    pub const FRESH: &str = "fresh";
    pub const FAILED: &str = "failed";
    /// `clone.slept` only: put to sleep to make room under `ram_budget` (`IDLE` is the other).
    pub const EVICTED: &str = "evicted";
}

/// Writes the event, then wakes the followers (they read the row from the store, so the order
/// of these wake-ups does not matter). A failure is logged and never reaches the caller. Do not
/// call it while holding the store lock.
pub fn emit(ctx: &Ctx, kind: &str, clone: Option<&str>, template: Option<&str>, detail: Value) {
    let written = ctx.store().add_event(kind, clone, template, &detail);
    match written {
        Ok(row) => {
            // The payload is only a wake-up. No follower is not an error.
            let _ = ctx.events_tx.send(row);
        }
        Err(e) => crate::lifecycle::log(ctx, &format!("event {kind} not recorded: {e}")),
    }
}

/// `emit` for a clone: the event names the clone by its name (else its id), and `detail` gains
/// the clone's `id` and the template it came from.
pub fn emit_clone(ctx: &Ctx, kind: &str, row: &CloneRow, mut detail: Value) {
    if let Value::Object(map) = &mut detail {
        map.insert("id".into(), json!(row.id));
    }
    let label = row.name.as_deref().unwrap_or(&row.id);
    emit(ctx, kind, Some(label), Some(&row.template), detail);
}

/// `template.failed` for a build error. Only the failed step is recorded: the step is the first
/// line of the error a build raises ("template build failed at <step>"), anything else is "other".
pub fn emit_template_failed(ctx: &Ctx, template: &str, gen: u32, op: &str, e: &MitosisError) {
    let step = e
        .message
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("template build failed at "))
        .filter(|s| {
            s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ' ')
        })
        .unwrap_or("other");
    emit(
        ctx,
        TEMPLATE_FAILED,
        None,
        Some(template),
        json!({ "gen": gen, "op": op, "step": step }),
    );
}

/// The one-line summary `mitosis events` prints for `detail`: `key=value` pairs of scalars.
pub fn short_detail(detail: &Value) -> String {
    let Value::Object(map) = detail else {
        return String::new();
    };
    map.iter()
        .filter_map(|(k, v)| match v {
            Value::String(s) => Some(format!("{k}={s}")),
            Value::Number(n) => Some(format!("{k}={n}")),
            Value::Bool(b) => Some(format!("{k}={b}")),
            Value::Array(a) => Some(format!(
                "{k}={}",
                a.iter()
                    .map(|x| x.to_string().trim_matches('"').to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// True when `e` passes `f`; the same rule `Store::list_events` applies in SQL (`limit` aside).
pub fn matches(f: &EventFilter, e: &EventRow) -> bool {
    let clone = f.clone.as_deref().is_none_or(|c| {
        e.clone.as_deref() == Some(c) || e.detail.get("id").and_then(Value::as_str) == Some(c)
    });
    let template = f
        .template
        .as_deref()
        .is_none_or(|t| e.template.as_deref() == Some(t));
    let kind = f.kind.as_deref().is_none_or(|k| {
        e.kind == k
            || e.kind
                .strip_prefix(k)
                .is_some_and(|rest| rest.starts_with('.'))
    });
    clone && template && kind && f.since.is_none_or(|t| e.ts >= t)
}

/// `ts` (unix seconds) as local time, `2026-10-20 14:03:09`; the number itself if the clock fails.
pub fn local_time(ts: i64) -> String {
    let t = ts as libc::time_t;
    let mut buf = [0u8; 32];
    // SAFETY: both calls write only into the `tm` and the buffer given to them.
    let n = unsafe {
        let mut tm: libc::tm = std::mem::zeroed();
        if libc::localtime_r(&t, &mut tm).is_null() {
            return ts.to_string();
        }
        libc::strftime(
            buf.as_mut_ptr().cast(),
            buf.len(),
            c"%Y-%m-%d %H:%M:%S".as_ptr(),
            &tm,
        )
    };
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// The label `mitosis events` prints for an event: its clone, else its template.
pub fn row_label(e: &EventRow) -> &str {
    e.clone.as_deref().or(e.template.as_deref()).unwrap_or("-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_detail_prints_scalars_and_lists() {
        let d = json!({ "reason": "ttl", "gen": 3, "ok": true, "gens": [1, 2], "skip": null });
        assert_eq!(short_detail(&d), "gen=3 gens=1,2 ok=true reason=ttl");
        assert_eq!(short_detail(&json!(null)), "");
    }

    #[test]
    fn matches_agrees_with_the_store_filter() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::state::Store::open(&dir.path().join("state.db")).unwrap();
        for (kind, clone, tpl, id) in [
            ("clone.created", Some("x"), Some("main"), "c1"),
            ("clone.destroyed", Some("x"), Some("main"), "c1"),
            ("template.built", None, Some("main"), ""),
            ("clone.created", Some("c2"), Some("other"), "c2"),
            ("gc", None, None, ""),
        ] {
            store
                .add_event(kind, clone, tpl, &json!({ "id": id }))
                .unwrap();
        }
        let f = |c: Option<&str>, t: Option<&str>, k: Option<&str>| EventFilter {
            clone: c.map(Into::into),
            template: t.map(Into::into),
            kind: k.map(Into::into),
            ..Default::default()
        };
        let all = store.list_events(&EventFilter::default()).unwrap();
        for filter in [
            f(None, None, None),
            f(Some("x"), None, None),
            f(Some("c1"), None, None),
            f(None, Some("main"), None),
            f(None, None, Some("clone")),
            f(None, None, Some("clone.created")),
            f(None, None, Some("clon")),
            f(Some("x"), Some("main"), Some("clone.destroyed")),
        ] {
            let want: Vec<&EventRow> = all.iter().filter(|e| matches(&filter, e)).collect();
            let got = store.list_events(&filter).unwrap();
            assert_eq!(got.iter().collect::<Vec<_>>(), want, "{filter:?}");
        }
    }

    #[test]
    fn failed_step_is_read_from_the_build_error() {
        let ctx_dir = tempfile::tempdir().unwrap();
        let ctx = Ctx::open(
            crate::paths::Home::at(ctx_dir.path().join("m")),
            crate::ctx::Config::default(),
        )
        .unwrap();
        let mut rx = ctx.events_tx.subscribe();
        let e = MitosisError::new(
            crate::error::ErrorCode::TemplateBuildFailed,
            "template build failed at seed\nconnection to postgres://u:secret@h/db refused",
        );
        emit_template_failed(&ctx, "main", 2, "build", &e);
        let row = rx.try_recv().unwrap();
        assert_eq!(row.kind, TEMPLATE_FAILED);
        assert_eq!(
            row.detail,
            json!({ "gen": 2, "op": "build", "step": "seed" })
        );
        assert!(!row.detail.to_string().contains("secret"));
        assert_eq!(
            ctx.store().list_events(&Default::default()).unwrap(),
            vec![row]
        );
    }
}
