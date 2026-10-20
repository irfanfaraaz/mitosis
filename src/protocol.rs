use crate::error::MitosisError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const PROTOCOL_VERSION: u32 = 3;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TemplateSource {
    Url {
        url: String,
    },
    /// A physical copy of a running server made with `pg_basebackup`.
    Basebackup {
        url: String,
    },
    Seed {
        command: String,
        /// Folder the command runs in; the daemon's own cwd when absent.
        #[serde(default)]
        cwd: Option<PathBuf>,
        /// The caller's `PATH`, so the command finds the same tools as in the caller's shell;
        /// the daemon's own `PATH` when absent.
        #[serde(default)]
        path: Option<String>,
    },
    /// Made by `promote` from a running clone; there is no source to rebuild it from.
    Promoted {
        clone_id: String,
    },
}

/// How a template generation was made.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GenKind {
    Full,
    Layered,
    Promoted,
    Upgraded,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RefreshMode {
    #[default]
    Full,
    Layered,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnStale {
    #[default]
    Warn,
    Refresh,
}

/// A migration command run against a template, in `cwd`, with the caller's `PATH`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct MigrateCmd {
    pub command: String,
    pub cwd: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct GenerationInfo {
    pub gen: u32,
    pub kind: GenKind,
    pub built_at: i64,
    pub pg_major: u32,
    pub size_bytes: u64,
    pub current: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_clone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_gen: Option<u32>,
}

/// Template settings a request may change; `None` / `false` leaves a setting as it is.
/// An `_off` flag turns its setting off and must not come with a value.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct TemplateSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every_secs: Option<u64>,
    #[serde(default)]
    pub every_off: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every_mode: Option<RefreshMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migrate: Option<MigrateCmd>,
    #[serde(default)]
    pub migrate_off: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_migrate: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_stale: Option<OnStale>,
    /// Masking file, as an absolute path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mask: Option<PathBuf>,
    #[serde(default)]
    pub mask_off: bool,
    /// The git branch the base template's recorded checkout follows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_branch: Option<String>,
    /// Forget the base branch: layered builds then run on any branch, as before v1.0.
    #[serde(default)]
    pub base_branch_off: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TemplateInitArgs {
    pub name: String,
    pub source: TemplateSource,
    pub database: Option<String>,
    pub migrations: Option<PathBuf>,
    pub max_age_secs: Option<u64>,
    pub pg_major: Option<u32>,
    /// Applied with the first generation.
    #[serde(flatten)]
    pub settings: TemplateSettings,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TemplateSetArgs {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_secs: Option<u64>,
    #[serde(flatten)]
    pub settings: TemplateSettings,
    /// Clears `paused`, so automatic builds run again.
    #[serde(default)]
    pub resume: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CloneCreateArgs {
    pub template: String,
    pub name: Option<String>,
    pub ttl_secs: u64,
    pub durable: bool,
    pub parent_pid: Option<i32>,
    /// Destroy a running clone of this name first (its reservation is kept), then create it.
    #[serde(default)]
    pub fresh: bool,
    /// Needs a name; no parent, no TTL, and `destroy` needs `force`.
    #[serde(default)]
    pub protect: bool,
    /// A `.env` entry the client will point at the clone; the daemon records it so a destroy can
    /// put `previous` back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<EnvTarget>,
    /// How long to wait for room under the limits; `None` uses the daemon's `queue_wait`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_secs: Option<u64>,
    /// Clone the template built for this git branch (`<template>~<branch>`) instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<BranchArgs>,
}

/// The caller's git checkout for `clone --branch`; the client fills it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct BranchArgs {
    /// The branch name as git shows it.
    pub name: String,
    /// The root of the caller's git work tree (`git rev-parse --show-toplevel`).
    pub repo_root: PathBuf,
    /// The caller's `PATH`, for the migrate command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// One variable in one `.env` file that holds a clone's URL.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EnvTarget {
    /// Absolute path.
    pub file: PathBuf,
    pub var: String,
    /// The value the variable had before (`None`: the line did not exist).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<String>,
}

/// A `.env` restore the daemon could not do (e.g. a read-only file); a client finishes it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PendingRestore {
    pub id: i64,
    pub file: PathBuf,
    pub var: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<String>,
    /// The clone URL the file must still hold for the restore to apply.
    pub url: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    TemplateInit(TemplateInitArgs),
    TemplateRefresh {
        name: String,
        /// The refreshing caller's `PATH`; for a seed template it replaces the recorded one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        /// Copy the current generation and run only the migrate command on it.
        #[serde(default)]
        layered: bool,
    },
    TemplateSet(TemplateSetArgs),
    TemplateList,
    TemplateHistory {
        name: String,
    },
    /// Points the template at generation `to`, or the newest one older than the current.
    TemplateRollback {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        to: Option<u32>,
    },
    /// Moves the template to Postgres `to` with pg_upgrade: a new generation, or with `as_name`
    /// a new template (same database and settings, schedule off).
    TemplateUpgrade {
        name: String,
        to: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_name: Option<String>,
    },
    CloneCreate(CloneCreateArgs),
    CloneList,
    CloneDestroy {
        id_or_name: String,
        /// Needed to destroy a protected clone.
        #[serde(default)]
        force: bool,
        /// Also drop the name's reserved port and password.
        #[serde(default)]
        forget: bool,
    },
    /// Makes a running clone the newest generation of its template, or of `template`.
    ClonePromote {
        id_or_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        template: Option<String>,
    },
    /// Starts a sleeping clone (a no-op for a running one); answered with `Response::Clone`.
    CloneWake {
        id_or_name: String,
    },
    /// What a clone changed against the template generation it came from; answered with
    /// `Response::Diff`. A `parent_pid` limits it to a clone of that MCP session.
    CloneDiff {
        id_or_name: String,
        /// A table (`name` or `schema.name`) to compare row by row.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        table: Option<String>,
        /// Rows shown per kind with `table`; default 20.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_pid: Option<i32>,
    },
    /// `.env` restores the daemon could not finish.
    PendingRestores,
    PendingRestoreDone {
        id: i64,
    },
    Gc,
    Status,
    /// Resource use of all clones against the limits.
    Usage,
    /// Re-read `config.toml` and the environment; later requests use the new settings.
    ConfigReload,
    /// Past events matching `filter`, oldest first. With `follow` the daemon keeps the connection
    /// open after that list and writes one `Response::Event` line per new event.
    Events {
        #[serde(default)]
        filter: EventFilter,
        #[serde(default)]
        follow: bool,
    },
    Shutdown,
}

/// One entry of the event log. `detail` never holds a password or a URL with one.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EventRow {
    pub id: i64,
    pub ts: i64,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    pub detail: serde_json::Value,
}

/// Which events to list; an unset field matches everything.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct EventFilter {
    /// A clone's name or id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// An exact kind (`clone.created`) or a group (`clone` matches every `clone.*`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Unix seconds; only events at or after it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<i64>,
    /// The newest `limit` matches (still listed oldest first).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TemplateInfo {
    pub name: String,
    pub pg_major: u32,
    pub source: TemplateSource,
    pub database: String,
    pub size_bytes: u64,
    pub built_at: i64,
    pub age_secs: u64,
    pub warnings: Vec<String>,
    #[serde(default)]
    pub current_gen: u32,
    /// How many generations the template has on disk.
    #[serde(default, rename = "gen_count")]
    pub generations: u32,
    #[serde(default)]
    pub paused: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every_secs: Option<u64>,
    #[serde(default)]
    pub every_mode: RefreshMode,
    /// The migrate command, passwords masked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migrate: Option<String>,
    #[serde(default)]
    pub auto_migrate: bool,
    #[serde(default)]
    pub on_stale: OnStale,
    #[serde(default)]
    pub keep: u32,
    /// The masking file, when the template has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mask: Option<PathBuf>,
    /// A branch template: its base template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// A branch template: its git branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// A base template: the branch its recorded checkout follows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_branch: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CloneInfo {
    pub id: String,
    pub name: Option<String>,
    pub template: String,
    pub url: String,
    pub port: u16,
    pub socket_dir: PathBuf,
    pub template_age_secs: u64,
    pub created_at: i64,
    pub expires_at: i64,
    pub durable: bool,
    pub parent_pid: Option<i32>,
    pub rss_bytes: Option<u64>,
    pub own_disk_bytes: Option<u64>,
    /// The template generation this clone was made from.
    #[serde(default, rename = "gen")]
    pub generation: u32,
    /// The template was older than its max age when this clone was made.
    #[serde(default)]
    pub stale: bool,
    /// A background refresh of the template was running (or was started) when this clone was made.
    #[serde(default)]
    pub refreshing: bool,
    /// A named clone that was already running was returned instead of a new one.
    #[serde(default)]
    pub existing: bool,
    /// Never expires and is not destroyed with a parent; `destroy` needs `--force`.
    #[serde(default)]
    pub protected: bool,
    #[serde(default)]
    pub warnings: Vec<String>,
    /// Wall time of the create request.
    #[serde(default)]
    pub duration_ms: u64,
    /// Time the create spent queued for room under the limits.
    #[serde(default)]
    pub waited_ms: u64,
    /// The clone's own disk use is past `clone_disk_warn`.
    #[serde(default)]
    pub disk_warn: bool,
    /// Restarted after an unclean shutdown with fsync off: recent writes may be lost or corrupt.
    #[serde(default)]
    pub suspect: bool,
    /// The URL straight to the clone's own port; `url` is the router URL while the router is on.
    #[serde(default)]
    pub direct_url: String,
    /// `ready`, `sleeping`, `starting` or `stopping`.
    #[serde(default)]
    pub status: String,
    /// Made from a branch template: its git branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// The branch template was rebuilt before this clone was made.
    #[serde(default)]
    pub rebuilt: bool,
}

/// What a clone changed against the template generation it was made from.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DiffReport {
    pub clone: String,
    pub template: String,
    #[serde(rename = "gen")]
    pub generation: u32,
    pub schema: Vec<SchemaChange>,
    /// Every table of either side; `public.` is left off names.
    pub tables: Vec<TableCount>,
    /// Only with `--table`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<RowDiff>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SchemaChange {
    /// table, column, index, constraint, view, function, sequence or extension.
    pub kind: String,
    pub name: String,
    /// added, removed or changed.
    pub change: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TableCount {
    pub table: String,
    /// None: the table is not there on that side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Count>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Count>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Count {
    pub n: u64,
    /// `n` is the planner's estimate: the exact count timed out.
    #[serde(default)]
    pub estimate: bool,
}

/// Row-level changes of one table. `pk` is the key value, or an array for a composite key.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RowDiff {
    pub table: String,
    pub inserted: Vec<serde_json::Value>,
    pub deleted: Vec<serde_json::Value>,
    pub updated: Vec<RowChange>,
    pub inserted_total: u64,
    pub deleted_total: u64,
    pub updated_total: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RowChange {
    pub pk: serde_json::Value,
    /// Columns whose values differ.
    pub columns: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GcReport {
    pub adopted: Vec<String>,
    pub destroyed: Vec<String>,
    pub orphan_folders_removed: Vec<PathBuf>,
    pub records_dropped: Vec<String>,
    /// Entries gc could not clean up; they are retried on the next gc.
    #[serde(default)]
    pub failed: Vec<GcFailure>,
    /// Things the caller should know, such as a restarted clone that is suspect.
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GcFailure {
    /// Clone id or folder path.
    pub item: String,
    pub error: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DaemonStatus {
    pub pid: u32,
    pub home: PathBuf,
    pub clones: usize,
    pub templates: usize,
    pub uptime_secs: u64,
    /// Crate version of the daemon binary.
    #[serde(default)]
    pub version: String,
    /// Started by the launchd login service.
    #[serde(default)]
    pub service: bool,
    /// The router's state; none when `router = off`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub router: Option<RouterStatus>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct RouterStatus {
    pub listening: bool,
    /// The address the router binds (`router_listen`); empty before the first bind attempt.
    #[serde(default)]
    pub listen: String,
    pub port: u16,
    /// Why the router is not listening (a taken port, say); it retries every minute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    TemplateBuilt(TemplateInfo),
    Templates {
        templates: Vec<TemplateInfo>,
    },
    Generations {
        template: String,
        generations: Vec<GenerationInfo>,
        /// Automatic builds are stopped (after a rollback).
        #[serde(default)]
        paused: bool,
    },
    CloneCreated(CloneInfo),
    /// One clone, e.g. after `CloneWake`.
    Clone(CloneInfo),
    Clones {
        clones: Vec<CloneInfo>,
    },
    Destroyed {
        id: String,
    },
    PendingRestores {
        items: Vec<PendingRestore>,
    },
    Gc(GcReport),
    Status(DaemonStatus),
    Usage(crate::budget::Usage),
    Events {
        events: Vec<EventRow>,
    },
    /// One new event on a following `Request::Events` connection.
    Event(EventRow),
    Diff(DiffReport),
    Ok,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RequestEnvelope {
    pub v: u32,
    pub request: Request,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResponseEnvelope {
    pub v: u32,
    pub result: std::result::Result<Response, MitosisError>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn gc_is_tagged_only() {
        assert_eq!(
            serde_json::to_string(&Request::Gc).unwrap(),
            r#"{"type":"gc"}"#
        );
    }

    #[test]
    fn request_envelope_round_trip() {
        let env = RequestEnvelope {
            v: PROTOCOL_VERSION,
            request: Request::CloneCreate(CloneCreateArgs {
                template: "main".into(),
                name: Some("feat".into()),
                ttl_secs: 7200,
                durable: false,
                parent_pid: Some(42),
                fresh: false,
                protect: false,
                env: None,
                wait_secs: None,
                branch: None,
            }),
        };
        let line = serde_json::to_string(&env).unwrap();
        assert!(!line.contains('\n'));
        let back: RequestEnvelope = serde_json::from_str(&line).unwrap();
        assert_eq!(back.v, PROTOCOL_VERSION);
        assert_eq!(PROTOCOL_VERSION, 3);
        match back.request {
            Request::CloneCreate(a) => {
                assert_eq!(a.template, "main");
                assert_eq!(a.name.as_deref(), Some("feat"));
                assert_eq!(a.ttl_secs, 7200);
                assert_eq!(a.parent_pid, Some(42));
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn err_response_round_trip() {
        let env = ResponseEnvelope {
            v: PROTOCOL_VERSION,
            result: Err(
                MitosisError::new(ErrorCode::CapReached, "too many").with_hint("destroy one")
            ),
        };
        let line = serde_json::to_string(&env).unwrap();
        let back: ResponseEnvelope = serde_json::from_str(&line).unwrap();
        let e = back.result.unwrap_err();
        assert_eq!(e.code, ErrorCode::CapReached);
        assert_eq!(e.hint.as_deref(), Some("destroy one"));
    }

    fn template_info() -> TemplateInfo {
        TemplateInfo {
            name: "main".into(),
            pg_major: 16,
            source: TemplateSource::Url {
                url: "postgres://x".into(),
            },
            database: "app".into(),
            size_bytes: 1024,
            built_at: 1_700_000_000,
            age_secs: 5,
            warnings: vec!["w".into()],
            current_gen: 2,
            generations: 2,
            paused: true,
            last_error: Some("boom".into()),
            every_secs: Some(86_400),
            every_mode: RefreshMode::Layered,
            migrate: Some("make migrate".into()),
            auto_migrate: true,
            on_stale: OnStale::Refresh,
            keep: 3,
            mask: None,
            base: None,
            branch: None,
            base_branch: None,
        }
    }

    fn clone_info() -> CloneInfo {
        CloneInfo {
            id: "abc123".into(),
            name: Some("feat".into()),
            template: "main".into(),
            url: "postgres://localhost:5500/app".into(),
            port: 5500,
            socket_dir: "/tmp/s".into(),
            template_age_secs: 5,
            created_at: 1,
            expires_at: 2,
            durable: false,
            parent_pid: Some(7),
            rss_bytes: None,
            own_disk_bytes: Some(9),
            generation: 2,
            stale: true,
            refreshing: true,
            existing: false,
            protected: false,
            warnings: Vec::new(),
            duration_ms: 0,
            waited_ms: 0,
            disk_warn: false,
            suspect: false,
            direct_url: "postgres://localhost:5500/app".into(),
            status: "ready".into(),
            branch: None,
            rebuilt: false,
        }
    }

    fn round_trip(r: Response) {
        let env = ResponseEnvelope {
            v: PROTOCOL_VERSION,
            result: Ok(r),
        };
        let line = serde_json::to_string(&env).unwrap();
        let back: ResponseEnvelope = serde_json::from_str(&line).unwrap();
        assert_eq!(
            serde_json::to_value(&env).unwrap(),
            serde_json::to_value(&back).unwrap()
        );
    }

    #[test]
    fn list_and_newtype_responses_round_trip() {
        round_trip(Response::Templates {
            templates: vec![template_info()],
        });
        round_trip(Response::Clones {
            clones: vec![clone_info()],
        });
        round_trip(Response::TemplateBuilt(template_info()));
        round_trip(Response::CloneCreated(clone_info()));
        round_trip(Response::Generations {
            template: "main".into(),
            generations: vec![GenerationInfo {
                gen: 1,
                kind: GenKind::Promoted,
                built_at: 1,
                pg_major: 16,
                size_bytes: 2,
                current: true,
                from_clone: Some("feat".into()),
                from_gen: Some(3),
            }],
            paused: true,
        });
    }

    #[test]
    fn v01_shapes_still_parse() {
        let t: TemplateInfo = serde_json::from_str(
            r#"{"name":"main","pg_major":16,"source":{"kind":"url","url":"postgres://h/db"},
                "database":"db","size_bytes":1,"built_at":1,"age_secs":1,"warnings":[]}"#,
        )
        .unwrap();
        assert_eq!((t.current_gen, t.paused, t.last_error), (0, false, None));
        let r: Request =
            serde_json::from_str(r#"{"type":"template_rollback","name":"main"}"#).unwrap();
        assert!(matches!(r, Request::TemplateRollback { to: None, .. }));
        assert_eq!(
            serde_json::to_string(&GenKind::Upgraded).unwrap(),
            r#""upgraded""#
        );
    }

    #[test]
    fn seed_cwd_is_optional() {
        let s: TemplateSource =
            serde_json::from_str(r#"{"kind":"seed","command":"make seed"}"#).unwrap();
        assert_eq!(
            s,
            TemplateSource::Seed {
                command: "make seed".into(),
                cwd: None,
                path: None
            }
        );
        let s = TemplateSource::Seed {
            command: "x".into(),
            cwd: Some("/w".into()),
            path: Some("/usr/bin:/bin".into()),
        };
        assert_eq!(
            serde_json::from_str::<TemplateSource>(&serde_json::to_string(&s).unwrap()).unwrap(),
            s
        );
    }

    #[test]
    fn refresh_path_is_optional() {
        let r: Request =
            serde_json::from_str(r#"{"type":"template_refresh","name":"main"}"#).unwrap();
        assert!(matches!(
            r,
            Request::TemplateRefresh {
                path: None,
                layered: false,
                ..
            }
        ));
        let r: GcReport = serde_json::from_str(
            r#"{"adopted":[],"destroyed":[],"orphan_folders_removed":[],"records_dropped":[]}"#,
        )
        .unwrap();
        assert!(r.failed.is_empty());
    }

    #[test]
    fn template_settings_are_flat_and_optional() {
        let r: Request = serde_json::from_str(
            r#"{"type":"template_init","name":"main","source":{"kind":"seed","command":"x"},
                "database":null,"migrations":null,"max_age_secs":null,"pg_major":null}"#,
        )
        .unwrap();
        let Request::TemplateInit(a) = r else {
            panic!("wrong variant")
        };
        assert_eq!(a.settings, TemplateSettings::default());

        let set = Request::TemplateSet(TemplateSetArgs {
            name: "main".into(),
            max_age_secs: Some(60),
            settings: TemplateSettings {
                keep: Some(2),
                every_secs: Some(3600),
                every_mode: Some(RefreshMode::Layered),
                migrate: Some(MigrateCmd {
                    command: "make migrate".into(),
                    cwd: "/w".into(),
                    path: None,
                }),
                auto_migrate: Some(true),
                on_stale: Some(OnStale::Refresh),
                ..Default::default()
            },
            resume: true,
        });
        let line = serde_json::to_string(&set).unwrap();
        assert!(line.contains(r#""keep":2"#), "{line}");
        let Request::TemplateSet(back) = serde_json::from_str(&line).unwrap() else {
            panic!("wrong variant")
        };
        assert_eq!(back.settings.keep, Some(2));
        assert_eq!(back.settings.every_mode, Some(RefreshMode::Layered));
        assert_eq!(back.settings.migrate.unwrap().command, "make migrate");
        assert!(back.resume && !back.settings.every_off);
        let r: Response =
            serde_json::from_str(r#"{"type":"generations","template":"main","generations":[]}"#)
                .unwrap();
        assert!(matches!(r, Response::Generations { paused: false, .. }));
    }

    #[test]
    fn events_round_trip_and_defaults() {
        let row = EventRow {
            id: 7,
            ts: 1_700_000_000,
            kind: "clone.created".into(),
            clone: Some("feat".into()),
            template: None,
            detail: serde_json::json!({ "id": "abc123" }),
        };
        round_trip(Response::Events {
            events: vec![row.clone()],
        });
        round_trip(Response::Event(row));
        // A request that names no filter and no follow still parses.
        let r: Request = serde_json::from_str(r#"{"type":"events"}"#).unwrap();
        let Request::Events { filter, follow } = r else {
            panic!("wrong variant")
        };
        assert_eq!((filter, follow), (EventFilter::default(), false));
        assert_eq!(
            serde_json::to_string(&EventFilter::default()).unwrap(),
            "{}"
        );
    }

    #[test]
    fn ok_response_round_trip() {
        let env = ResponseEnvelope {
            v: PROTOCOL_VERSION,
            result: Ok(Response::Ok),
        };
        let line = serde_json::to_string(&env).unwrap();
        let back: ResponseEnvelope = serde_json::from_str(&line).unwrap();
        assert!(matches!(back.result, Ok(Response::Ok)));
    }
}
