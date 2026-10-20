//! Contract tests: the 1.0 surface that COMPATIBILITY.md promises stays put until v2.
//!
//! Each test builds a representative value (every optional field set, every list non-empty) and
//! compares its field shape (keys and JSON types, recursively; values ignored) with a checked-in
//! file under `tests/contract/`. The error codes, config keys, enum values, MCP input schemas and
//! the CLI command tree are compared as they are. A change fails here on purpose: update the
//! snapshot and the CHANGELOG together.
//!
//! Create or update the snapshots with `MITOSIS_UPDATE_SNAPSHOTS=1 cargo test --test contract`.

use mitosis::budget::{CloneUsage, Usage};
use mitosis::config::{ConfigEntry, ConfigSource};
use mitosis::doctor::{Check, Level};
use mitosis::error::{ErrorCode, MitosisError};
use mitosis::events;
use mitosis::pg::managed::ManagedInstall;
use mitosis::protocol::*;
use mitosis::service::ServiceStatus;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;
use std::path::PathBuf;

// ---------------------------------------------------------------------------------------------
// Snapshot machinery
// ---------------------------------------------------------------------------------------------

fn snapshot_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/contract")
        .join(format!("{name}.json"))
}

/// Compares `actual` with `tests/contract/<name>.json`. With `MITOSIS_UPDATE_SNAPSHOTS=1` the file
/// is (re)written instead; without it a missing file fails and says how to create it.
fn snapshot(name: &str, actual: &Value) {
    let path = snapshot_path(name);
    if std::env::var_os("MITOSIS_UPDATE_SNAPSHOTS").is_some_and(|v| v == "1") {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let text = serde_json::to_string_pretty(actual).unwrap() + "\n";
        std::fs::write(&path, text).unwrap();
        return;
    }
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "contract snapshot {} is missing ({e}).\n\
             Create it with: MITOSIS_UPDATE_SNAPSHOTS=1 cargo test --test contract\n\
             then review and commit the new file.",
            path.display()
        )
    });
    let expected: Value = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("contract snapshot {} is not JSON: {e}", path.display()));
    if &expected != actual {
        panic!(
            "the public contract `{name}` changed (- snapshot, + now):\n{}\n\
             If the change is deliberate and allowed by COMPATIBILITY.md, run\n\
             MITOSIS_UPDATE_SNAPSHOTS=1 cargo test --test contract\n\
             and list it in CHANGELOG.md.",
            diff(&expected, actual)
        );
    }
}

/// Snapshot of a value's field shape.
fn shape_snapshot<T: Serialize>(name: &str, value: &T) {
    snapshot(name, &shape(&serde_json::to_value(value).unwrap()));
}

/// Keys and JSON types, recursively. An array's shape is the merge of its items' shapes.
fn shape(v: &Value) -> Value {
    match v {
        Value::Null => "null".into(),
        Value::Bool(_) => "boolean".into(),
        Value::Number(_) => "number".into(),
        Value::String(_) => "string".into(),
        Value::Array(items) => {
            Value::Array(items.iter().map(shape).reduce(merge).into_iter().collect())
        }
        Value::Object(m) => Value::Object(m.iter().map(|(k, v)| (k.clone(), shape(v))).collect()),
    }
}

/// Two shapes of items in one array: objects get the union of their keys. Different types under
/// one key fail, since a script cannot rely on such a field.
fn merge(a: Value, b: Value) -> Value {
    match (a, b) {
        (Value::Object(mut a), Value::Object(b)) => {
            for (k, v) in b {
                let merged = match a.remove(&k) {
                    Some(old) => merge(old, v),
                    None => v,
                };
                a.insert(k, merged);
            }
            Value::Object(a)
        }
        (Value::Array(a), Value::Array(b)) => {
            Value::Array(a.into_iter().chain(b).reduce(merge).into_iter().collect())
        }
        (a, b) if a == b => a,
        (a, b) => panic!("one field has two JSON types: {a} and {b}"),
    }
}

/// `path = value` lines of both sides, the ones only in `expected` with `-`, only in `actual`
/// with `+`.
fn diff(expected: &Value, actual: &Value) -> String {
    fn flatten(v: &Value, path: String, out: &mut BTreeSet<String>) {
        match v {
            Value::Object(m) if !m.is_empty() => {
                for (k, v) in m {
                    flatten(v, format!("{path}.{k}"), out);
                }
            }
            Value::Array(a) if !a.is_empty() => {
                for (i, v) in a.iter().enumerate() {
                    flatten(v, format!("{path}[{i}]"), out);
                }
            }
            leaf => {
                out.insert(format!("{path} = {leaf}"));
            }
        }
    }
    let (mut e, mut a) = (BTreeSet::new(), BTreeSet::new());
    flatten(expected, String::new(), &mut e);
    flatten(actual, String::new(), &mut a);
    let mut lines: Vec<String> = e.difference(&a).map(|l| format!("- {l}")).collect();
    lines.extend(a.difference(&e).map(|l| format!("+ {l}")));
    lines.join("\n")
}

#[test]
fn shape_merges_array_items_and_ignores_values() {
    let v = json!([{"a": 1, "b": null}, {"a": 2, "c": [true]}]);
    assert_eq!(
        shape(&v),
        json!([{"a": "number", "b": "null", "c": ["boolean"]}])
    );
    assert_eq!(shape(&json!([])), json!([]));
    let d = diff(
        &json!({"a": "string"}),
        &json!({"a": "number", "b": "string"}),
    );
    assert_eq!(d, "- .a = \"string\"\n+ .a = \"number\"\n+ .b = \"string\"");
}

// ---------------------------------------------------------------------------------------------
// Representative values: every Option is Some, every Vec has an item
// ---------------------------------------------------------------------------------------------

fn clone_info() -> CloneInfo {
    CloneInfo {
        id: "a1b2c3d4".into(),
        name: Some("feat".into()),
        template: "main".into(),
        url: "postgres://postgres:***@127.0.0.1:54300/feat".into(),
        port: 54321,
        socket_dir: "/home/sockets/a1b2c3d4".into(),
        template_age_secs: 60,
        created_at: 1_700_000_000,
        expires_at: 1_700_007_200,
        durable: false,
        parent_pid: Some(42),
        rss_bytes: Some(1),
        own_disk_bytes: Some(1),
        generation: 2,
        stale: false,
        refreshing: false,
        existing: false,
        protected: false,
        warnings: vec!["w".into()],
        duration_ms: 900,
        waited_ms: 0,
        disk_warn: false,
        suspect: false,
        direct_url: "postgres://postgres:***@127.0.0.1:54321/app".into(),
        status: "ready".into(),
        branch: Some("feature".into()),
        rebuilt: false,
    }
}

fn template_info() -> TemplateInfo {
    TemplateInfo {
        name: "main".into(),
        pg_major: 17,
        source: TemplateSource::Url {
            url: "postgres://u:***@h/db".into(),
        },
        database: "app".into(),
        size_bytes: 1,
        built_at: 1_700_000_000,
        age_secs: 1,
        warnings: vec!["w".into()],
        current_gen: 2,
        generations: 2,
        paused: false,
        last_error: Some("e".into()),
        every_secs: Some(86_400),
        every_mode: RefreshMode::Full,
        migrate: Some("make migrate".into()),
        auto_migrate: false,
        on_stale: OnStale::Warn,
        keep: 3,
        mask: Some("/w/mask.toml".into()),
        base: Some("main".into()),
        branch: Some("feature".into()),
        base_branch: Some("main".into()),
    }
}

fn generation_info() -> GenerationInfo {
    GenerationInfo {
        gen: 2,
        kind: GenKind::Promoted,
        built_at: 1_700_000_000,
        pg_major: 17,
        size_bytes: 1,
        current: true,
        from_clone: Some("a1b2c3d4".into()),
        from_gen: Some(1),
    }
}

fn event_row() -> EventRow {
    EventRow {
        id: 1,
        ts: 1_700_000_000,
        kind: events::CLONE_DESTROYED.into(),
        clone: Some("feat".into()),
        template: Some("main".into()),
        // Per-kind detail keys are listed in COMPATIBILITY.md, not pinned here.
        detail: json!({}),
    }
}

fn diff_report() -> DiffReport {
    DiffReport {
        clone: "feat".into(),
        template: "main".into(),
        generation: 2,
        schema: vec![SchemaChange {
            kind: "column".into(),
            name: "users.email".into(),
            change: "changed".into(),
            before: Some("text".into()),
            after: Some("varchar(200)".into()),
        }],
        tables: vec![TableCount {
            table: "users".into(),
            before: Some(Count {
                n: 1,
                estimate: false,
            }),
            after: Some(Count {
                n: 2,
                estimate: true,
            }),
        }],
        rows: Some(RowDiff {
            table: "users".into(),
            inserted: vec![json!({"id": 2})],
            deleted: vec![json!({"id": 1})],
            updated: vec![RowChange {
                pk: json!(3),
                columns: vec!["email".into()],
            }],
            inserted_total: 1,
            deleted_total: 1,
            updated_total: 1,
        }),
    }
}

fn settings() -> TemplateSettings {
    TemplateSettings {
        keep: Some(3),
        every_secs: Some(86_400),
        every_off: false,
        every_mode: Some(RefreshMode::Layered),
        migrate: Some(migrate_cmd()),
        migrate_off: false,
        auto_migrate: Some(true),
        on_stale: Some(OnStale::Refresh),
        mask: Some("/w/mask.toml".into()),
        mask_off: false,
        base_branch: Some("main".into()),
        base_branch_off: false,
    }
}

fn migrate_cmd() -> MigrateCmd {
    MigrateCmd {
        command: "make migrate".into(),
        cwd: "/w".into(),
        path: Some("/usr/bin".into()),
    }
}

fn clone_usage() -> CloneUsage {
    CloneUsage {
        id: "a1b2c3d4".into(),
        name: Some("feat".into()),
        template: "main".into(),
        rss_bytes: Some(1),
        own_disk_bytes: Some(1),
        created_at: 1,
        expires_at: 2,
        protected: false,
        disk_warn: false,
        status: "ready".into(),
    }
}

// ---------------------------------------------------------------------------------------------
// CLI `--json` outputs
// ---------------------------------------------------------------------------------------------

#[test]
fn cli_json_outputs() {
    // mitosis clone --json
    shape_snapshot("cli_clone", &clone_info());
    // mitosis list --json
    shape_snapshot("cli_list", &vec![clone_info()]);
    // mitosis template list --json
    shape_snapshot("cli_template_list", &vec![template_info()]);
    // mitosis template history --json
    shape_snapshot(
        "cli_template_history",
        &mitosis::cli::history_json("main", true, &[generation_info()]),
    );
    // mitosis events --json: an array; with --follow one object per line.
    shape_snapshot("cli_events", &vec![event_row()]);
    shape_snapshot("cli_events_follow_line", &event_row());
    // mitosis diff --json
    shape_snapshot("cli_diff", &diff_report());
    // mitosis doctor --json
    shape_snapshot(
        "cli_doctor",
        &vec![Check {
            name: "daemon",
            level: Level::Warn,
            detail: "d".into(),
            fix: Some("f".into()),
        }],
    );
    // mitosis pg list --json
    shape_snapshot(
        "cli_pg_list",
        &vec![ManagedInstall {
            version: "17.9".into(),
            major: 17,
            dir: "/home/pg/17.9".into(),
            used: true,
        }],
    );
    // mitosis service status --json
    shape_snapshot(
        "cli_service_status",
        &ServiceStatus {
            installed: true,
            loaded: true,
            running: true,
            binary: Some("/usr/local/bin/mitosis".into()),
            binary_exists: true,
        },
    );
}

#[test]
fn cli_config_json() {
    let entries = vec![
        ConfigEntry {
            key: "max_clones".into(),
            value: "8".into(),
            source: ConfigSource::Default,
        },
        ConfigEntry {
            key: "ram_budget".into(),
            value: "4GB".into(),
            source: ConfigSource::File,
        },
        ConfigEntry {
            key: "queue_wait".into(),
            value: "2m".into(),
            source: ConfigSource::Env("MITOSIS_QUEUE_WAIT".into()),
        },
    ];
    // `source` is always a string; `env_var` only comes with `env`.
    assert_eq!(
        serde_json::to_value(&entries).unwrap(),
        json!([
            {"key": "max_clones", "value": "8", "source": "default"},
            {"key": "ram_budget", "value": "4GB", "source": "file"},
            {"key": "queue_wait", "value": "2m", "source": "env", "env_var": "MITOSIS_QUEUE_WAIT"},
        ])
    );
    shape_snapshot("cli_config", &entries);
}

/// Values that appear in `--json` fields and keep their meaning until v2.
#[test]
fn enum_values() {
    fn names<T: Serialize>(all: &[T]) -> Value {
        all.iter()
            .map(|v| serde_json::to_value(v).unwrap())
            .collect()
    }
    // Each match fails to compile when a variant is added: add it to the list too.
    let gen_kinds = [
        GenKind::Full,
        GenKind::Layered,
        GenKind::Promoted,
        GenKind::Upgraded,
    ];
    for k in gen_kinds {
        match k {
            GenKind::Full | GenKind::Layered | GenKind::Promoted | GenKind::Upgraded => {}
        }
    }
    let modes = [RefreshMode::Full, RefreshMode::Layered];
    for m in modes {
        match m {
            RefreshMode::Full | RefreshMode::Layered => {}
        }
    }
    let stale = [OnStale::Warn, OnStale::Refresh];
    for s in stale {
        match s {
            OnStale::Warn | OnStale::Refresh => {}
        }
    }
    let levels = [Level::Ok, Level::Warn, Level::Fail];
    for l in levels {
        match l {
            Level::Ok | Level::Warn | Level::Fail => {}
        }
    }
    let sources = [
        TemplateSource::Url { url: "u".into() },
        TemplateSource::Basebackup { url: "u".into() },
        TemplateSource::Seed {
            command: "c".into(),
            cwd: Some("/w".into()),
            path: Some("/usr/bin".into()),
        },
        TemplateSource::Promoted {
            clone_id: "a1b2c3d4".into(),
        },
    ];
    for s in &sources {
        match s {
            TemplateSource::Url { .. }
            | TemplateSource::Basebackup { .. }
            | TemplateSource::Seed { .. }
            | TemplateSource::Promoted { .. } => {}
        }
    }
    let source_shapes: Map<String, Value> = sources
        .iter()
        .map(|s| {
            let v = serde_json::to_value(s).unwrap();
            (v["kind"].as_str().unwrap().to_string(), shape(&v))
        })
        .collect();
    snapshot(
        "enums",
        &json!({
            "template.source.kind": source_shapes,
            "generation.kind": names(&gen_kinds),
            "template.every_mode": names(&modes),
            "template.on_stale": names(&stale),
            "doctor.level": names(&levels),
            "clone.status": ["ready", "sleeping", "starting", "stopping"],
            "diff.schema.kind": [
                "table", "column", "index", "constraint", "view", "function", "sequence", "extension"
            ],
            "diff.schema.change": ["added", "removed", "changed"],
            "event.kind": [
                events::CLONE_CREATED,
                events::CLONE_DESTROYED,
                events::CLONE_SLEPT,
                events::CLONE_WOKE,
                events::CLONE_SUSPECT,
                events::TEMPLATE_BUILT,
                events::TEMPLATE_FAILED,
                events::TEMPLATE_ROLLED_BACK,
                events::TEMPLATE_PROMOTED,
                events::TEMPLATE_UPGRADED,
                events::TEMPLATE_PRUNED,
                events::TEMPLATE_SKIPPED,
                events::QUEUE_WAITED,
                events::QUEUE_TIMEOUT,
                events::ROUTER_STARTED,
                events::ROUTER_FAILED,
                events::GC,
                events::RECOVERY,
            ],
            "event.detail.reason": [
                events::reason::USER,
                events::reason::TTL,
                events::reason::PARENT,
                events::reason::IDLE,
                events::reason::GC,
                events::reason::RECOVERY,
                events::reason::FRESH,
                events::reason::FAILED,
                events::reason::EVICTED,
            ],
        }),
    );
}

// ---------------------------------------------------------------------------------------------
// MCP tools
// ---------------------------------------------------------------------------------------------

/// A JSON schema without its prose (`description`, `title`, `$schema`); property names stay.
fn schema_without_prose(v: &Value, in_properties: bool) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .filter(|(k, _)| {
                    in_properties || !matches!(k.as_str(), "description" | "title" | "$schema")
                })
                .map(|(k, v)| (k.clone(), schema_without_prose(v, k == "properties")))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(|v| schema_without_prose(v, false)).collect()),
        other => other.clone(),
    }
}

#[test]
fn mcp_tool_input_schemas() {
    let tools: Map<String, Value> = mitosis::mcp::Mitosis::tool_router()
        .list_all()
        .into_iter()
        .map(|t| {
            let schema = Value::Object((*t.input_schema).clone());
            let mut entry = json!({ "input": schema_without_prose(&schema, false) });
            if let Some(out) = &t.output_schema {
                entry["output"] = schema_without_prose(&Value::Object((**out).clone()), false);
            }
            (t.name.to_string(), entry)
        })
        .collect();
    snapshot("mcp_tools", &Value::Object(tools));
}

#[test]
fn mcp_tool_outputs() {
    shape_snapshot(
        "mcp_clone_create",
        &mitosis::mcp::create_output(&clone_info(), vec!["w".into()]).unwrap(),
    );
    shape_snapshot("mcp_clone_list", &vec![clone_info()]);
    shape_snapshot(
        "mcp_clone_destroy",
        &mitosis::mcp::destroy_output("a1b2c3d4"),
    );
    shape_snapshot("mcp_clone_diff", &diff_report());
}

// ---------------------------------------------------------------------------------------------
// Error codes, exit codes, config keys
// ---------------------------------------------------------------------------------------------

#[test]
fn error_codes() {
    use ErrorCode::*;
    let all = [
        NotApfs,
        CrossVolume,
        ForeignHome,
        NoMatchingPostgres,
        MissingExtension,
        TemplateNotFound,
        TemplateBuildFailed,
        CloneNotFound,
        CapReached,
        BudgetFull,
        LowDisk,
        PortsExhausted,
        PortInUse,
        PostgresStartFailed,
        AlreadyRunning,
        DaemonUnavailable,
        ProtocolMismatch,
        InvalidRequest,
        Internal,
    ];
    // Fails to compile when a code is added: add it to the list above.
    for c in all {
        match c {
            NotApfs | CrossVolume | ForeignHome | NoMatchingPostgres | MissingExtension
            | TemplateNotFound | TemplateBuildFailed | CloneNotFound | CapReached | BudgetFull
            | LowDisk | PortsExhausted | PortInUse | PostgresStartFailed | AlreadyRunning
            | DaemonUnavailable | ProtocolMismatch | InvalidRequest | Internal => {}
        }
    }
    let mut codes: Vec<String> = all
        .iter()
        .map(|c| {
            serde_json::to_value(c)
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    codes.sort();
    snapshot("error_codes", &json!(codes));
    // An error as clients receive it.
    shape_snapshot(
        "error",
        &MitosisError::new(InvalidRequest, "m").with_hint("h"),
    );
}

#[test]
fn exit_codes() {
    // 0 success, 1 failure (also `doctor` with a failed check and `daemon status` with no daemon),
    // 2 bad command line (clap), 3 `mitosis daemon` when one already runs.
    assert_eq!(mitosis::client::EXIT_ALREADY_RUNNING, 3);
    let failed = Check {
        name: "x",
        level: Level::Fail,
        detail: String::new(),
        fix: None,
    };
    let warned = Check {
        level: Level::Warn,
        ..failed.clone()
    };
    assert_eq!(mitosis::doctor::exit_code(std::slice::from_ref(&warned)), 0);
    assert_eq!(mitosis::doctor::exit_code(&[warned, failed]), 1);
    let usage = mitosis::cli::command()
        .try_get_matches_from(["mitosis", "no-such-command"])
        .unwrap_err();
    assert_eq!(usage.exit_code(), 2);
}

#[test]
fn config_keys() {
    let keys: Map<String, Value> = mitosis::config::keys()
        .map(|(key, env)| (key.to_string(), json!({ "env": env })))
        .collect();
    snapshot("config_keys", &Value::Object(keys));
}

// ---------------------------------------------------------------------------------------------
// CLI command tree
// ---------------------------------------------------------------------------------------------

/// Every subcommand with its flags and positionals: whether each takes a value, whether the
/// value is optional, required, its choices and its default. Help text is left out.
fn cli_tree(cmd: &clap::Command) -> Value {
    let mut args = Map::new();
    for a in cmd.get_arguments() {
        let id = a.get_id().as_str();
        // Only clap's generated flags: a user-defined `--version` (as on `pg install`) is pinned.
        if matches!(
            a.get_action(),
            clap::ArgAction::Help
                | clap::ArgAction::HelpShort
                | clap::ArgAction::HelpLong
                | clap::ArgAction::Version
        ) {
            continue;
        }
        let key = match a.get_long() {
            Some(l) => format!("--{l}"),
            None => format!("<{id}>"),
        };
        let mut d = Map::new();
        let takes_value = a.get_action().takes_values();
        d.insert("takes_value".into(), takes_value.into());
        if takes_value && a.get_num_args().is_some_and(|n| n.min_values() == 0) {
            d.insert("value_optional".into(), true.into());
        }
        if a.is_required_set() {
            d.insert("required".into(), true.into());
        }
        if let Some(i) = a.get_index() {
            d.insert("index".into(), i.into());
        }
        let values: Vec<String> = a
            .get_possible_values()
            .iter()
            .map(|v| v.get_name().to_string())
            .collect();
        if takes_value && !values.is_empty() {
            d.insert("values".into(), values.into());
        }
        let defaults: Vec<String> = a
            .get_default_values()
            .iter()
            .map(|v| v.to_string_lossy().into_owned())
            .collect();
        if takes_value && !defaults.is_empty() {
            d.insert("default".into(), defaults.into());
        }
        let aliases: Vec<String> = a
            .get_long_and_visible_aliases()
            .into_iter()
            .flatten()
            .filter(|l| Some(*l) != a.get_long())
            .map(String::from)
            .collect();
        if !aliases.is_empty() {
            d.insert("aliases".into(), aliases.into());
        }
        args.insert(key, Value::Object(d));
    }
    let subcommands: Map<String, Value> = cmd
        .get_subcommands()
        .filter(|s| s.get_name() != "help")
        .map(|s| (s.get_name().to_string(), cli_tree(s)))
        .collect();
    let mut out = Map::new();
    if !args.is_empty() {
        out.insert("args".into(), Value::Object(args));
    }
    if !subcommands.is_empty() {
        out.insert("subcommands".into(), Value::Object(subcommands));
    }
    Value::Object(out)
}

#[test]
fn cli_command_tree() {
    let mut cmd = mitosis::cli::command();
    cmd.build();
    snapshot("cli", &cli_tree(&cmd));
}

// ---------------------------------------------------------------------------------------------
// Daemon protocol: new requests and fields only
// ---------------------------------------------------------------------------------------------

fn requests() -> Vec<Request> {
    let all = vec![
        Request::TemplateInit(TemplateInitArgs {
            name: "main".into(),
            source: TemplateSource::Url { url: "u".into() },
            database: Some("app".into()),
            migrations: Some("/w/migrations".into()),
            max_age_secs: Some(1),
            pg_major: Some(17),
            settings: settings(),
        }),
        Request::TemplateRefresh {
            name: "main".into(),
            path: Some("/usr/bin".into()),
            layered: true,
        },
        Request::TemplateSet(TemplateSetArgs {
            name: "main".into(),
            max_age_secs: Some(1),
            settings: settings(),
            resume: true,
        }),
        Request::TemplateList,
        Request::TemplateHistory {
            name: "main".into(),
        },
        Request::TemplateRollback {
            name: "main".into(),
            to: Some(1),
        },
        Request::TemplateUpgrade {
            name: "main".into(),
            to: 18,
            as_name: Some("main18".into()),
        },
        Request::CloneCreate(CloneCreateArgs {
            template: "main".into(),
            name: Some("feat".into()),
            ttl_secs: 7200,
            durable: false,
            parent_pid: Some(42),
            fresh: false,
            protect: false,
            env: Some(EnvTarget {
                file: "/w/.env".into(),
                var: "DATABASE_URL".into(),
                previous: Some("x".into()),
            }),
            wait_secs: Some(0),
            branch: Some(BranchArgs {
                name: "feature".into(),
                repo_root: "/w".into(),
                path: Some("/usr/bin".into()),
            }),
        }),
        Request::CloneList,
        Request::CloneDestroy {
            id_or_name: "feat".into(),
            force: false,
            forget: false,
        },
        Request::ClonePromote {
            id_or_name: "feat".into(),
            template: Some("qa".into()),
        },
        Request::CloneWake {
            id_or_name: "feat".into(),
        },
        Request::CloneDiff {
            id_or_name: "feat".into(),
            table: Some("users".into()),
            limit: Some(20),
            parent_pid: Some(42),
        },
        Request::PendingRestores,
        Request::PendingRestoreDone { id: 1 },
        Request::Gc,
        Request::Status,
        Request::Usage,
        Request::ConfigReload,
        Request::Events {
            filter: EventFilter {
                clone: Some("feat".into()),
                template: Some("main".into()),
                kind: Some("clone".into()),
                since: Some(1),
                limit: Some(10),
            },
            follow: true,
        },
        Request::Shutdown,
    ];
    // Fails to compile when a request is added: add it to the list above.
    for r in &all {
        match r {
            Request::TemplateInit(_)
            | Request::TemplateRefresh { .. }
            | Request::TemplateSet(_)
            | Request::TemplateList
            | Request::TemplateHistory { .. }
            | Request::TemplateRollback { .. }
            | Request::TemplateUpgrade { .. }
            | Request::CloneCreate(_)
            | Request::CloneList
            | Request::CloneDestroy { .. }
            | Request::ClonePromote { .. }
            | Request::CloneWake { .. }
            | Request::CloneDiff { .. }
            | Request::PendingRestores
            | Request::PendingRestoreDone { .. }
            | Request::Gc
            | Request::Status
            | Request::Usage
            | Request::ConfigReload
            | Request::Events { .. }
            | Request::Shutdown => {}
        }
    }
    all
}

fn responses() -> Vec<Response> {
    let all = vec![
        Response::TemplateBuilt(template_info()),
        Response::Templates {
            templates: vec![template_info()],
        },
        Response::Generations {
            template: "main".into(),
            generations: vec![generation_info()],
            paused: false,
        },
        Response::CloneCreated(clone_info()),
        Response::Clone(clone_info()),
        Response::Clones {
            clones: vec![clone_info()],
        },
        Response::Destroyed {
            id: "a1b2c3d4".into(),
        },
        Response::PendingRestores {
            items: vec![PendingRestore {
                id: 1,
                file: "/w/.env".into(),
                var: "DATABASE_URL".into(),
                previous: Some("x".into()),
                url: "postgres://postgres:***@127.0.0.1:54300/feat".into(),
            }],
        },
        Response::Gc(GcReport {
            adopted: vec!["a".into()],
            destroyed: vec!["b".into()],
            orphan_folders_removed: vec!["/home/clones/c".into()],
            records_dropped: vec!["d".into()],
            failed: vec![GcFailure {
                item: "e".into(),
                error: "f".into(),
            }],
            warnings: vec!["w".into()],
        }),
        Response::Status(DaemonStatus {
            pid: 1,
            home: "/home".into(),
            clones: 1,
            templates: 1,
            uptime_secs: 1,
            version: "1.0.0".into(),
            service: false,
            router: Some(RouterStatus {
                listening: false,
                listen: "127.0.0.1".into(),
                port: 54300,
                error: Some("taken".into()),
            }),
        }),
        Response::Usage(Usage {
            clones: vec![clone_usage()],
            ram_bytes: 1,
            disk_bytes: 1,
            free_disk_bytes: 1,
            queued: 0,
            max_clones: 8,
            ram_budget: Some(1),
            disk_budget: Some(1),
        }),
        Response::Events {
            events: vec![event_row()],
        },
        Response::Event(event_row()),
        Response::Diff(diff_report()),
        Response::Ok,
    ];
    for r in &all {
        match r {
            Response::TemplateBuilt(_)
            | Response::Templates { .. }
            | Response::Generations { .. }
            | Response::CloneCreated(_)
            | Response::Clone(_)
            | Response::Clones { .. }
            | Response::Destroyed { .. }
            | Response::PendingRestores { .. }
            | Response::Gc(_)
            | Response::Status(_)
            | Response::Usage(_)
            | Response::Events { .. }
            | Response::Event(_)
            | Response::Diff(_)
            | Response::Ok => {}
        }
    }
    all
}

/// Each message by its `type` tag.
fn by_type<T: Serialize>(items: &[T]) -> Value {
    let mut out = Map::new();
    for item in items {
        let v = serde_json::to_value(item).unwrap();
        let tag = v["type"].as_str().expect("tagged by type").to_string();
        assert!(out.insert(tag.clone(), shape(&v)).is_none(), "{tag} twice");
    }
    Value::Object(out)
}

#[test]
fn daemon_protocol() {
    assert_eq!(PROTOCOL_VERSION, 3);
    snapshot("protocol_requests", &by_type(&requests()));
    snapshot("protocol_responses", &by_type(&responses()));
    let ok = ResponseEnvelope {
        v: PROTOCOL_VERSION,
        result: Ok(Response::Ok),
    };
    let err = ResponseEnvelope {
        v: PROTOCOL_VERSION,
        result: Err(MitosisError::new(ErrorCode::Internal, "m").with_hint("h")),
    };
    let req = RequestEnvelope {
        v: PROTOCOL_VERSION,
        request: Request::Status,
    };
    snapshot(
        "protocol_envelopes",
        &json!({
            "request": shape(&serde_json::to_value(&req).unwrap()),
            "ok": shape(&serde_json::to_value(&ok).unwrap()),
            "err": shape(&serde_json::to_value(&err).unwrap()),
        }),
    );
}
