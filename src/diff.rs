//! `mitosis diff`: what a clone changed against the template generation it was made from.
//!
//! The base side is a private, socket-only Postgres on a copy-on-write clone of the generation folder; a
//! guard stops it and deletes the folder on every path. Both sides are read with `psql`.

use crate::clone_backend::CloneBackend;
use crate::clones;
use crate::ctx::Ctx;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::names::random_hex;
use crate::pg::{psql, start, stop, wait_ready, PgInstall, RunningPg, StartOptions};
use crate::protocol::{Count, DiffReport, RowChange, RowDiff, SchemaChange, TableCount};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Rows shown per kind when `--limit` is not given.
pub const DEFAULT_LIMIT: u32 = 20;
/// The most rows per kind `--limit` accepts.
pub const MAX_LIMIT: u32 = 1000;
/// `--table` refuses tables above this many rows (by the planner's estimate).
const MAX_TABLE_ROWS: i64 = 1_000_000;
/// The socket file name only; the base server listens on no port.
const BASE_PORT: u16 = 5432;

fn invalid(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::InvalidRequest, msg)
}

fn internal(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::Internal, msg)
}

async fn blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| internal(format!("background task failed: {e}")))?
}

/// Refuses a clone that does not belong to the MCP session `parent_pid`.
pub fn check_session(ctx: &Ctx, id_or_name: &str, parent_pid: i32) -> Result<()> {
    let row = ctx.store().get_clone(id_or_name)?.ok_or_else(|| {
        MitosisError::new(
            ErrorCode::CloneNotFound,
            format!("clone {id_or_name} not found"),
        )
        .with_hint("see this session's clones with the clone_list tool")
    })?;
    if row.parent_pid != Some(parent_pid) {
        return Err(invalid(format!(
            "clone {} was not created by this session; it may be another agent's database",
            row.id
        ))
        .with_hint("this tool only reads clones from clone_create in this session"));
    }
    Ok(())
}

/// The private base server and the folders around it. `close` (or, on a cancelled request, drop)
/// stops the server and removes everything it made; a failure is written to `daemon.log` with the
/// leaked folder, and `Drop` tries once more.
struct BaseServer {
    backend: Arc<dyn CloneBackend>,
    install: PgInstall,
    dir: PathBuf,
    socket_dir: PathBuf,
    log: PathBuf,
    daemon_log: PathBuf,
    pg: Option<RunningPg>,
    timeout: Duration,
    done: bool,
}

impl BaseServer {
    /// Stops the server first; the folder is removed only once nothing runs in it.
    fn cleanup(&mut self) -> Result<()> {
        if self.dir.exists() {
            stop(&self.install, &self.dir, self.pg, self.timeout)?;
            self.backend.remove_dir(&self.dir)?;
        }
        let _ = std::fs::remove_dir_all(&self.socket_dir);
        let _ = std::fs::remove_file(&self.log);
        self.done = true;
        Ok(())
    }

    fn cleanup_logged(&mut self) {
        if let Err(e) = self.cleanup() {
            use std::io::Write;
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.daemon_log)
            {
                let _ = writeln!(
                    f,
                    "[{}] diff: could not clean up the private server in {}: {}",
                    crate::templates::now(),
                    self.dir.display(),
                    e.message
                );
            }
        }
    }

    /// Cleans up off the runtime. Never fails: the diff's own result stands.
    async fn close(mut self) {
        let _ = tokio::task::spawn_blocking(move || {
            self.cleanup_logged();
            self // dropping it here retries a cleanup that failed
        })
        .await;
    }
}

// shortcut: a drop on a runtime thread blocks on pg_ctl; it only runs for a cancelled request
// or a failed cleanup. If `start` was still running then, `pg` is unknown and the postmaster can
// outlive the folder removal.
impl Drop for BaseServer {
    fn drop(&mut self) {
        if !self.done {
            self.cleanup_logged();
        }
    }
}

/// One server a query can reach over a socket.
#[derive(Clone)]
struct Side {
    install: PgInstall,
    socket_dir: PathBuf,
    port: u16,
    db: String,
}

impl Side {
    async fn query(&self, sql: String) -> Result<String> {
        let s = self.clone();
        blocking(move || psql(&s.install, &s.socket_dir, s.port, &s.db, &sql)).await
    }
}

pub async fn diff(
    ctx: &Arc<Ctx>,
    id_or_name: &str,
    table: Option<&str>,
    limit: u32,
) -> Result<DiffReport> {
    if limit > MAX_LIMIT {
        return Err(invalid(format!("limit {limit} is above {MAX_LIMIT}"))
            .with_hint(format!("ask for at most {MAX_LIMIT} rows per kind")));
    }
    let row = clones::ensure_running(ctx, id_or_name).await?;
    let label = row.name.clone().unwrap_or_else(|| row.id.clone());
    let pruned = || {
        invalid(format!(
            "generation {} of template {} that clone {label} was made from was pruned",
            row.generation, row.template
        ))
        .with_hint("the base is gone; nothing to compare against. Raise the template's `keep` before pruning next time")
    };

    // Held from reading the generation until its folder is cloned, so no prune moves it.
    let swap = ctx.template_swap.read().await;
    let gen = ctx
        .store()
        .get_generation(&row.template, row.generation)?
        .filter(|g| g.dir.is_dir())
        .ok_or_else(pruned)?;
    let install = ctx.install_major(gen.pg_major).await?;

    let tag = random_hex(4);
    let mut base = BaseServer {
        backend: ctx.backend.clone(),
        install: install.clone(),
        dir: ctx.home.templates_dir().join(format!(".diff-{tag}")),
        // Short: Unix socket paths are limited to about 100 bytes.
        socket_dir: ctx.home.sockets_dir().join(format!("d{tag}")),
        log: ctx.home.logs_dir().join(format!("diff-{tag}.log")),
        daemon_log: ctx.home.logs_dir().join("daemon.log"),
        pg: None,
        timeout: ctx.config().stop_timeout,
        done: false,
    };
    let (backend, src, dst) = (ctx.backend.clone(), gen.dir.clone(), base.dir.clone());
    let cloned = blocking(move || backend.clone_dir(&src, &dst)).await;
    drop(swap);

    let run = async {
        cloned?;
        let opts = StartOptions {
            port: BASE_PORT,
            socket_dir: base.socket_dir.clone(),
            listen_tcp: false,
            durable: false,
            log_file: base.log.clone(),
        };
        let (i, d, o) = (install.clone(), base.dir.clone(), opts.clone());
        let pg = blocking(move || start(&i, &d, &o)).await?;
        base.pg = Some(pg);
        wait_ready(&install, &pg, &opts, ctx.config().ready_timeout).await?;
        let before = Side {
            install: install.clone(),
            socket_dir: base.socket_dir.clone(),
            port: BASE_PORT,
            db: row.database.clone(),
        };
        let after = Side {
            install: install.clone(),
            socket_dir: row.socket_dir.clone(),
            port: row.port,
            db: row.database.clone(),
        };
        compare(&before, &after, table, limit).await
    }
    .await;
    base.close().await;
    let (schema, tables, rows) = run?;
    Ok(DiffReport {
        clone: label,
        template: row.template,
        generation: row.generation,
        schema,
        tables,
        rows,
    })
}

type Compared = (Vec<SchemaChange>, Vec<TableCount>, Option<RowDiff>);

async fn compare(before: &Side, after: &Side, table: Option<&str>, limit: u32) -> Result<Compared> {
    let (b, a) = tokio::try_join!(snapshot(before), snapshot(after))?;
    let schema = schema_changes(&b.objects, &a.objects);
    let tables = table_counts(before, after, &b.tables, &a.tables).await?;
    let rows = match table {
        Some(t) => Some(row_diff(before, after, t, limit as usize).await?),
        None => None,
    };
    Ok((schema, tables, rows))
}

// ---- schema -------------------------------------------------------------------------------

/// (kind, name, definition, owning table).
type Obj = (String, String, Option<String>, Option<String>);

/// (schema, table, planner estimate).
type TableEntry = (String, String, i64);

struct Snapshot {
    objects: Vec<Obj>,
    tables: Vec<TableEntry>,
}

const KINDS: [&str; 8] = [
    "table",
    "column",
    "index",
    "constraint",
    "view",
    "function",
    "sequence",
    "extension",
];

/// Catalog reads in user schemas only. Two statements, one JSON line each.
const SNAPSHOT_SQL: &str = r#"
WITH ns AS (
  SELECT oid, nspname FROM pg_namespace
  WHERE nspname NOT IN ('pg_catalog', 'information_schema')
    AND nspname NOT LIKE 'pg\_toast%' AND nspname NOT LIKE 'pg\_temp\_%'
    AND nspname NOT LIKE 'pg\_toast\_temp\_%'
)
SELECT coalesce(jsonb_agg(jsonb_build_array(kind, name, def, tbl)), '[]'::jsonb) FROM (
  SELECT 'table'::text AS kind, n.nspname || '.' || c.relname AS name, NULL::text AS def, NULL::text AS tbl
    FROM pg_class c JOIN ns n ON n.oid = c.relnamespace WHERE c.relkind IN ('r', 'p', 'f')
  UNION ALL
  SELECT 'column', n.nspname || '.' || c.relname || '.' || a.attname,
         format_type(a.atttypid, a.atttypmod)
           || CASE WHEN a.attnotnull THEN ' not null' ELSE '' END
           || CASE WHEN a.attidentity <> '' THEN ' identity ' || a.attidentity::text ELSE '' END
           || CASE WHEN a.attgenerated <> '' THEN ' generated as (' || pg_get_expr(d.adbin, d.adrelid) || ')'
                   WHEN d.adbin IS NOT NULL THEN ' default ' || pg_get_expr(d.adbin, d.adrelid)
                   ELSE '' END,
         n.nspname || '.' || c.relname
    FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN ns n ON n.oid = c.relnamespace
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE c.relkind IN ('r', 'p', 'f') AND a.attnum > 0 AND NOT a.attisdropped
  UNION ALL
  SELECT 'index', n.nspname || '.' || ic.relname, pg_get_indexdef(i.indexrelid), n.nspname || '.' || tc.relname
    FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class tc ON tc.oid = i.indrelid
    JOIN ns n ON n.oid = ic.relnamespace
  UNION ALL
  SELECT 'constraint', n.nspname || '.' || c.relname || '.' || k.conname, pg_get_constraintdef(k.oid),
         n.nspname || '.' || c.relname
    FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid JOIN ns n ON n.oid = c.relnamespace
    WHERE k.contype IN ('p', 'u', 'f', 'c', 'x')
  UNION ALL
  SELECT 'view', n.nspname || '.' || c.relname, pg_get_viewdef(c.oid), NULL
    FROM pg_class c JOIN ns n ON n.oid = c.relnamespace WHERE c.relkind IN ('v', 'm')
  UNION ALL
  SELECT 'function', n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')',
         pg_get_functiondef(p.oid), NULL
    FROM pg_proc p JOIN ns n ON n.oid = p.pronamespace
    WHERE p.prokind IN ('f', 'p')
      AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')
  UNION ALL
  SELECT 'sequence', n.nspname || '.' || c.relname,
         format_type(s.seqtypid, NULL) || ' start ' || s.seqstart || ' increment ' || s.seqincrement
           || ' min ' || s.seqmin || ' max ' || s.seqmax || CASE WHEN s.seqcycle THEN ' cycle' ELSE '' END,
         (SELECT rn.nspname || '.' || rc.relname FROM pg_depend d
            JOIN pg_class rc ON rc.oid = d.refobjid JOIN pg_namespace rn ON rn.oid = rc.relnamespace
            WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
              AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i') LIMIT 1)
    FROM pg_class c JOIN pg_sequence s ON s.seqrelid = c.oid JOIN ns n ON n.oid = c.relnamespace
  UNION ALL
  SELECT 'extension', extname::text, extversion, NULL FROM pg_extension
) x;
WITH ns AS (
  SELECT oid, nspname FROM pg_namespace
  WHERE nspname NOT IN ('pg_catalog', 'information_schema')
    AND nspname NOT LIKE 'pg\_toast%' AND nspname NOT LIKE 'pg\_temp\_%'
    AND nspname NOT LIKE 'pg\_toast\_temp\_%'
)
SELECT coalesce(jsonb_agg(jsonb_build_array(n.nspname, c.relname, c.reltuples::bigint)
                          ORDER BY n.nspname, c.relname), '[]'::jsonb)
  FROM pg_class c JOIN ns n ON n.oid = c.relnamespace WHERE c.relkind IN ('r', 'p');
"#;

/// `public.t.id` -> `t.id`: the default schema is left off.
fn short(name: &str) -> String {
    name.strip_prefix("public.").unwrap_or(name).to_string()
}

async fn snapshot(side: &Side) -> Result<Snapshot> {
    let out = side.query(SNAPSHOT_SQL.to_string()).await?;
    let mut lines = out.lines().filter(|l| !l.trim().is_empty());
    let bad = |what: &str| internal(format!("unexpected catalog reply ({what})"));
    let objects: Vec<Obj> = serde_json::from_str(lines.next().ok_or_else(|| bad("objects"))?)?;
    let tables: Vec<TableEntry> = serde_json::from_str(lines.next().ok_or_else(|| bad("tables"))?)?;
    let objects = objects
        .into_iter()
        .map(|(k, n, d, t)| (k, short(&n), d, t.map(|t| short(&t))))
        .collect();
    Ok(Snapshot { objects, tables })
}

/// Added, removed and changed objects. The columns, indexes, constraints and sequences of an added
/// or removed table are left out: the table says it.
fn schema_changes(before: &[Obj], after: &[Obj]) -> Vec<SchemaChange> {
    let rank = |k: &str| KINDS.iter().position(|x| *x == k).unwrap_or(KINDS.len());
    let map = |objs: &[Obj]| -> BTreeMap<(usize, String), (Option<String>, Option<String>)> {
        objs.iter()
            .map(|(k, n, d, t)| ((rank(k), n.clone()), (d.clone(), t.clone())))
            .collect()
    };
    let (b, a) = (map(before), map(after));
    let table_rank = rank("table");
    let names = |m: &BTreeMap<(usize, String), _>| -> HashSet<String> {
        m.keys()
            .filter(|(r, _)| *r == table_rank)
            .map(|(_, n)| n.clone())
            .collect()
    };
    let (bt, at) = (names(&b), names(&a));
    let whole: HashSet<&String> = bt.symmetric_difference(&at).collect();

    let keys: std::collections::BTreeSet<&(usize, String)> = b.keys().chain(a.keys()).collect();
    let mut out = Vec::new();
    for key in keys {
        let (bv, av) = (b.get(key), a.get(key));
        let owner = av.or(bv).and_then(|(_, t)| t.as_ref());
        if owner.is_some_and(|t| whole.contains(t)) {
            continue;
        }
        let (change, bdef, adef) = match (bv, av) {
            (None, Some((d, _))) => ("added", None, d.clone()),
            (Some((d, _)), None) => ("removed", d.clone(), None),
            (Some((bd, _)), Some((ad, _))) if bd != ad => ("changed", bd.clone(), ad.clone()),
            _ => continue,
        };
        out.push(SchemaChange {
            kind: KINDS.get(key.0).unwrap_or(&"table").to_string(),
            name: key.1.clone(),
            change: change.into(),
            before: bdef,
            after: adef,
        });
    }
    out
}

// ---- row counts ---------------------------------------------------------------------------

fn qi(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn ql(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn display_table(schema: &str, rel: &str) -> String {
    if schema == "public" {
        rel.to_string()
    } else {
        format!("{schema}.{rel}")
    }
}

/// Exact counts under a 5 s statement timeout; a table that times out keeps its estimate.
async fn counts(side: &Side, tables: &[TableEntry]) -> Result<HashMap<String, Count>> {
    let mut sql = String::from("\\set ON_ERROR_STOP off\nSET statement_timeout = '5s';\n");
    for (i, (s, r, _)) in tables.iter().enumerate() {
        sql.push_str(&format!(
            "SELECT jsonb_build_array({i}, count(*)) FROM {}.{};\n",
            qi(s),
            qi(r)
        ));
    }
    let out = side.query(sql).await?;
    let mut exact: HashMap<usize, u64> = HashMap::new();
    for line in out.lines().filter(|l| l.starts_with('[')) {
        if let Ok((i, n)) = serde_json::from_str::<(usize, u64)>(line) {
            exact.insert(i, n);
        }
    }
    Ok(tables
        .iter()
        .enumerate()
        .map(|(i, (s, r, est))| {
            let count = match exact.get(&i) {
                Some(n) => Count {
                    n: *n,
                    estimate: false,
                },
                None => Count {
                    n: (*est).max(0) as u64,
                    estimate: true,
                },
            };
            (display_table(s, r), count)
        })
        .collect())
}

async fn table_counts(
    before: &Side,
    after: &Side,
    bt: &[TableEntry],
    at: &[TableEntry],
) -> Result<Vec<TableCount>> {
    let (b, a) = tokio::try_join!(counts(before, bt), counts(after, at))?;
    let names: std::collections::BTreeSet<&String> = b.keys().chain(a.keys()).collect();
    Ok(names
        .into_iter()
        .map(|t| TableCount {
            table: t.clone(),
            before: b.get(t).cloned(),
            after: a.get(t).cloned(),
        })
        .collect())
}

// ---- rows ---------------------------------------------------------------------------------

struct TableInfo {
    pk: Vec<String>,
    estimate: i64,
}

/// The key columns and row estimate of `schema.rel`; none when the table does not exist.
async fn table_info(side: &Side, schema: &str, rel: &str) -> Result<Option<TableInfo>> {
    let qualified = format!("{}.{}", qi(schema), qi(rel));
    let sql = format!(
        "SELECT coalesce((SELECT jsonb_build_object(
           'pk', (SELECT coalesce(jsonb_agg(a.attname ORDER BY array_position(i.indkey::int2[], a.attnum)), '[]'::jsonb)
                    FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey::int2[])
                    WHERE i.indrelid = c.oid AND i.indisprimary),
           'est', CASE WHEN c.reltuples >= 0 THEN c.reltuples::bigint ELSE pg_relation_size(c.oid) / 8192 * 255 END)
           FROM pg_class c WHERE c.oid = to_regclass({})), 'null'::jsonb);",
        ql(&qualified)
    );
    let v: Value = serde_json::from_str(side.query(sql).await?.trim())?;
    if v.is_null() {
        return Ok(None);
    }
    let pk = serde_json::from_value(v["pk"].clone())?;
    Ok(Some(TableInfo {
        pk,
        estimate: v["est"].as_i64().unwrap_or(0),
    }))
}

fn pk_list(pk: &[String]) -> String {
    pk.iter().map(|c| qi(c)).collect::<Vec<_>>().join(", ")
}

/// Sorted by the key's text in byte order, so the merge below can compare keys as plain strings.
fn hash_sql(q: &str, pk: &[String]) -> String {
    format!(
        "SELECT md5(row_to_json(mitosis_row)::text) || ' ' || jsonb_build_array({cols})::text \
         FROM {q} AS mitosis_row ORDER BY jsonb_build_array({cols})::text COLLATE \"C\";",
        cols = pk_list(pk)
    )
}

/// The lines of one psql query, read as they arrive: `(md5 of the row, key text)`.
struct RowStream {
    child: std::process::Child,
    lines: std::io::Lines<std::io::BufReader<std::process::ChildStdout>>,
    writer: Option<std::thread::JoinHandle<std::io::Result<()>>>,
}

impl RowStream {
    fn open(side: &Side, sql: String) -> Result<RowStream> {
        use std::io::Write;
        use std::process::{Command, Stdio};
        let mut child = Command::new(side.install.bin("psql"))
            .arg("-h")
            .arg(&side.socket_dir)
            .args([
                "-p",
                &side.port.to_string(),
                "-U",
                "postgres",
                "-d",
                &side.db,
            ])
            .args(["-v", "ON_ERROR_STOP=1", "-X", "-A", "-t", "-q", "-f", "-"])
            .env("PGCONNECT_TIMEOUT", "5")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut stdin = child.stdin.take().expect("psql stdin is piped");
        let stdout = child.stdout.take().expect("psql stdout is piped");
        let writer = std::thread::spawn(move || stdin.write_all(sql.as_bytes()));
        Ok(RowStream {
            child,
            lines: std::io::BufRead::lines(std::io::BufReader::new(stdout)),
            writer: Some(writer),
        })
    }

    fn next_row(&mut self) -> Result<Option<(String, String)>> {
        loop {
            match self.lines.next() {
                None => return Ok(None),
                Some(Err(e)) => return Err(e.into()),
                Some(Ok(l)) => {
                    if let Some((h, k)) = l.split_once(' ') {
                        return Ok(Some((h.to_string(), k.to_string())));
                    }
                }
            }
        }
    }

    /// Waits for psql after its output is read to the end; a failed query is an error.
    fn finish(&mut self) -> Result<()> {
        use std::io::Read;
        let mut err = String::new();
        if let Some(mut e) = self.child.stderr.take() {
            let _ = e.read_to_string(&mut err);
        }
        let status = self.child.wait()?;
        let sent = self.writer.take().map(|w| {
            w.join()
                .unwrap_or_else(|_| Err(std::io::Error::other("writer panicked")))
        });
        if !status.success() {
            return Err(internal(format!("psql failed: {}", err.trim())));
        }
        if let Some(Err(e)) = sent {
            return Err(internal(format!("could not send SQL to psql: {e}")));
        }
        Ok(())
    }
}

impl Drop for RowStream {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Default)]
struct Merged {
    inserted: Vec<String>,
    deleted: Vec<String>,
    updated: Vec<String>,
    inserted_total: u64,
    deleted_total: u64,
    updated_total: u64,
}

impl Merged {
    fn push(list: &mut Vec<String>, total: &mut u64, key: String, limit: usize) {
        *total += 1;
        if list.len() < limit {
            list.push(key);
        }
    }
}

/// Merges two key-ordered streams; memory is the `limit` keys kept per kind, not the table.
fn merge(
    mut before: Option<RowStream>,
    mut after: Option<RowStream>,
    limit: usize,
) -> Result<Merged> {
    let next = |s: &mut Option<RowStream>| -> Result<Option<(String, String)>> {
        match s {
            Some(s) => s.next_row(),
            None => Ok(None),
        }
    };
    let mut m = Merged::default();
    let (mut b, mut a) = (next(&mut before)?, next(&mut after)?);
    loop {
        match (b.take(), a.take()) {
            (None, None) => break,
            (Some((_, k)), None) => {
                Merged::push(&mut m.deleted, &mut m.deleted_total, k, limit);
                b = next(&mut before)?;
            }
            (None, Some((_, k))) => {
                Merged::push(&mut m.inserted, &mut m.inserted_total, k, limit);
                a = next(&mut after)?;
            }
            (Some(bv), Some(av)) => match bv.1.cmp(&av.1) {
                std::cmp::Ordering::Less => {
                    Merged::push(&mut m.deleted, &mut m.deleted_total, bv.1, limit);
                    b = next(&mut before)?;
                    a = Some(av);
                }
                std::cmp::Ordering::Greater => {
                    Merged::push(&mut m.inserted, &mut m.inserted_total, av.1, limit);
                    a = next(&mut after)?;
                    b = Some(bv);
                }
                std::cmp::Ordering::Equal => {
                    if bv.0 != av.0 {
                        Merged::push(&mut m.updated, &mut m.updated_total, av.1, limit);
                    }
                    b = next(&mut before)?;
                    a = next(&mut after)?;
                }
            },
        }
    }
    for s in [&mut before, &mut after].into_iter().flatten() {
        s.finish()?;
    }
    Ok(m)
}

fn canon(text: &str) -> Result<Value> {
    Ok(serde_json::from_str(text)?)
}

/// Whole rows as JSON.
const ROW_JSON: &str = "to_jsonb(mitosis_row)";
/// Per column, the md5 of its JSON text: compares values exactly, as the row hash does.
const COLUMN_HASHES: &str =
    "(SELECT jsonb_object_agg(k, md5(v::text)) FROM json_each(row_to_json(mitosis_row)) AS e(k, v))";

/// `expr` of the rows with the given key texts, keyed by their canonical key.
async fn fetch_rows(
    side: &Side,
    q: &str,
    pk: &[String],
    keys: &[&str],
    expr: &str,
) -> Result<HashMap<String, Value>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let lits = keys.iter().map(|k| ql(k)).collect::<Vec<_>>().join(", ");
    let cols = pk_list(pk);
    let sql = format!(
        "SELECT jsonb_build_array(jsonb_build_array({cols}), {expr})::text \
         FROM {q} AS mitosis_row WHERE jsonb_build_array({cols}) = ANY (ARRAY[{lits}]::jsonb[]);"
    );
    let out = side.query(sql).await?;
    let mut map = HashMap::new();
    for line in out.lines().filter(|l| l.starts_with('[')) {
        let Value::Array(mut pair) = canon(line)? else {
            continue;
        };
        if pair.len() == 2 {
            let row = pair.pop().unwrap_or(Value::Null);
            map.insert(pair[0].to_string(), row);
        }
    }
    Ok(map)
}

/// A one-column key is shown as its value, a composite key as an array.
fn pk_value(key: &str) -> Result<Value> {
    Ok(match canon(key)? {
        Value::Array(mut a) if a.len() == 1 => a.pop().unwrap_or(Value::Null),
        v => v,
    })
}

fn changed_columns(b: &Value, a: &Value) -> Vec<String> {
    let (Some(b), Some(a)) = (b.as_object(), a.as_object()) else {
        return Vec::new();
    };
    let mut cols: Vec<String> = b
        .keys()
        .chain(a.keys().filter(|k| !b.contains_key(*k)))
        .filter(|k| b.get(*k) != a.get(*k))
        .cloned()
        .collect();
    cols.sort();
    cols
}

async fn row_diff(before: &Side, after: &Side, table: &str, limit: usize) -> Result<RowDiff> {
    let (schema, rel) = table.split_once('.').unwrap_or(("public", table));
    let q = format!("{}.{}", qi(schema), qi(rel));
    let (bi, ai) = tokio::try_join!(
        table_info(before, schema, rel),
        table_info(after, schema, rel)
    )?;
    let pk = match (&bi, &ai) {
        (None, None) => {
            return Err(invalid(format!("table {table} does not exist"))
                .with_hint("use `schema.table` for a table outside the public schema"))
        }
        (Some(b), Some(a)) if b.pk != a.pk => {
            return Err(invalid(format!(
                "the primary key of {table} differs between the template and the clone"
            ))
            .with_hint("a row diff needs the same key on both sides"))
        }
        (_, Some(i)) | (Some(i), None) => i.pk.clone(),
    };
    if pk.is_empty() {
        return Err(invalid(format!("table {table} has no primary key"))
            .with_hint("rows are matched by primary key; use the row counts instead"));
    }
    let biggest = bi
        .iter()
        .chain(ai.iter())
        .map(|i| i.estimate)
        .max()
        .unwrap_or(0);
    if biggest > MAX_TABLE_ROWS {
        return Err(invalid(format!(
            "table {table} has about {biggest} rows; a row diff stops at {MAX_TABLE_ROWS}"
        ))
        .with_hint("diff a smaller table, or compare the row counts"));
    }

    let (bsql, asql) = (hash_sql(&q, &pk), hash_sql(&q, &pk));
    let (bs, as_) = (
        bi.is_some().then(|| (before.clone(), bsql)),
        ai.is_some().then(|| (after.clone(), asql)),
    );
    let m = blocking(move || {
        let open = |s: Option<(Side, String)>| {
            s.map(|(side, sql)| RowStream::open(&side, sql)).transpose()
        };
        merge(open(bs)?, open(as_)?, limit)
    })
    .await?;

    let (del, ins, upd) = (
        m.deleted.iter().map(String::as_str).collect::<Vec<_>>(),
        m.inserted.iter().map(String::as_str).collect::<Vec<_>>(),
        m.updated.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let (bcols, acols) = tokio::try_join!(
        fetch_rows(before, &q, &pk, &upd, COLUMN_HASHES),
        fetch_rows(after, &q, &pk, &upd, COLUMN_HASHES)
    )?;
    let (brows, arows) = tokio::try_join!(
        fetch_rows(before, &q, &pk, &del, ROW_JSON),
        fetch_rows(after, &q, &pk, &ins, ROW_JSON)
    )?;
    let by_key = |m: &HashMap<String, Value>, k: &str| -> Result<Value> {
        Ok(m.get(&canon(k)?.to_string())
            .cloned()
            .unwrap_or(Value::Null))
    };
    let mut out = RowDiff {
        table: table.to_string(),
        inserted: Vec::new(),
        deleted: Vec::new(),
        updated: Vec::new(),
        inserted_total: m.inserted_total,
        deleted_total: m.deleted_total,
        updated_total: m.updated_total,
    };
    for k in &ins {
        out.inserted.push(by_key(&arows, k)?);
    }
    for k in &del {
        out.deleted.push(by_key(&brows, k)?);
    }
    for k in &upd {
        out.updated.push(RowChange {
            pk: pk_value(k)?,
            columns: changed_columns(&by_key(&bcols, k)?, &by_key(&acols, k)?),
        });
    }
    Ok(out)
}

// ---- human output -------------------------------------------------------------------------

fn count_text(c: &Option<Count>) -> String {
    match c {
        Some(c) => format!("{}{}", if c.estimate { "~" } else { "" }, c.n),
        None => "(none)".into(),
    }
}

/// The report as text: Schema, Row counts and (with `--table`) Rows.
pub fn render(r: &DiffReport) -> String {
    let mut o = format!(
        "clone {} against template {} generation {}\n\nSchema\n",
        r.clone, r.template, r.generation
    );
    if r.schema.is_empty() {
        o.push_str("  no changes\n");
    }
    for c in &r.schema {
        let mark = match c.change.as_str() {
            "added" => '+',
            "removed" => '-',
            _ => '~',
        };
        o.push_str(&format!("  {mark} {} {}\n", c.kind, c.name));
        if c.change == "changed" {
            for (label, v) in [("before", &c.before), ("after", &c.after)] {
                if let Some(v) = v {
                    o.push_str(&format!(
                        "      {label}: {}\n",
                        v.replace('\n', "\n        ")
                    ));
                }
            }
        } else if c.kind == "column" {
            if let Some(v) = c.after.as_ref().or(c.before.as_ref()) {
                o.push_str(&format!("      {v}\n"));
            }
        }
    }

    o.push_str("\nRow counts (~ is an estimate)\n");
    let shown: Vec<&TableCount> = r
        .tables
        .iter()
        .filter(|t| {
            t.before != t.after
                || t.before.as_ref().is_some_and(|c| c.estimate)
                || t.after.as_ref().is_some_and(|c| c.estimate)
        })
        .collect();
    if shown.is_empty() {
        o.push_str("  no changes\n");
    }
    for t in &shown {
        o.push_str(&format!(
            "  {}  {} -> {}\n",
            t.table,
            count_text(&t.before),
            count_text(&t.after)
        ));
    }
    let same = r.tables.len() - shown.len();
    if same > 0 {
        o.push_str(&format!("  ({same} tables unchanged)\n"));
    }

    if let Some(d) = &r.rows {
        o.push_str(&format!(
            "\nRows of {}: {} inserted, {} deleted, {} updated\n",
            d.table, d.inserted_total, d.deleted_total, d.updated_total
        ));
        for row in &d.inserted {
            o.push_str(&format!("  + {row}\n"));
        }
        for row in &d.deleted {
            o.push_str(&format!("  - {row}\n"));
        }
        for u in &d.updated {
            o.push_str(&format!("  ~ {}: {}\n", u.pk, u.columns.join(", ")));
        }
        let shown = (d.inserted.len() + d.deleted.len() + d.updated.len()) as u64;
        if shown < d.inserted_total + d.deleted_total + d.updated_total {
            o.push_str("  (more rows exist; raise --limit)\n");
        }
    }
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(k: &str, n: &str, d: Option<&str>, t: Option<&str>) -> Obj {
        (k.into(), n.into(), d.map(Into::into), t.map(Into::into))
    }

    #[test]
    fn added_table_hides_its_columns_but_a_changed_index_shows() {
        let before = vec![
            obj("table", "a", None, None),
            obj("index", "a_i", Some("on x"), Some("a")),
        ];
        let after = vec![
            obj("table", "a", None, None),
            obj("table", "b", None, None),
            obj("column", "b.id", Some("integer"), Some("b")),
            obj("index", "a_i", Some("on y"), Some("a")),
        ];
        let got = schema_changes(&before, &after);
        let got: Vec<(&str, &str, &str)> = got
            .iter()
            .map(|c| (c.kind.as_str(), c.name.as_str(), c.change.as_str()))
            .collect();
        assert_eq!(got, [("table", "b", "added"), ("index", "a_i", "changed")]);
    }

    #[test]
    fn short_drops_public_only() {
        assert_eq!(short("public.t.id"), "t.id");
        assert_eq!(short("app.t"), "app.t");
    }

    #[test]
    fn changed_columns_lists_differences_and_missing() {
        let b = serde_json::json!({"id": 1, "a": "x", "gone": 1});
        let a = serde_json::json!({"id": 1, "a": "y", "new": 2});
        assert_eq!(changed_columns(&b, &a), ["a", "gone", "new"]);
    }
}
