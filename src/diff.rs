//! `mitosis diff`: what a clone changed against the template generation it was made from.
//!
//! The base side is a private, socket-only Postgres on a `clonefile` of the generation folder; a
//! guard stops it and deletes the folder on every path. Both sides are read with `psql`.

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
/// stops the server and removes everything it made.
struct BaseServer {
    install: PgInstall,
    dir: PathBuf,
    socket_dir: PathBuf,
    log: PathBuf,
    pg: Option<RunningPg>,
    timeout: Duration,
    done: bool,
}

impl BaseServer {
    fn cleanup(&mut self) -> Result<()> {
        self.done = true;
        if self.dir.exists() {
            stop(&self.install, &self.dir, self.pg, self.timeout)?;
            std::fs::remove_dir_all(&self.dir)?;
        }
        let _ = std::fs::remove_dir_all(&self.socket_dir);
        let _ = std::fs::remove_file(&self.log);
        Ok(())
    }

    async fn close(mut self) -> Result<()> {
        blocking(move || self.cleanup()).await
    }
}

impl Drop for BaseServer {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.cleanup();
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
        install: install.clone(),
        dir: ctx.home.templates_dir().join(format!(".diff-{tag}")),
        // Short: Unix socket paths are limited to about 100 bytes.
        socket_dir: ctx.home.sockets_dir().join(format!("d{tag}")),
        log: ctx.home.logs_dir().join(format!("diff-{tag}.log")),
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
    let closed = base.close().await;
    let (schema, tables, rows) = run?;
    closed?;
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
    let none = Count {
        n: 0,
        estimate: false,
    };
    Ok(names
        .into_iter()
        .map(|t| TableCount {
            table: t.clone(),
            before: b.get(t).cloned().unwrap_or_else(|| none.clone()),
            after: a.get(t).cloned().unwrap_or_else(|| none.clone()),
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

/// `(pk json text, md5 of the row)` in key order.
async fn row_hashes(side: &Side, q: &str, pk: &[String]) -> Result<Vec<(String, String)>> {
    let sql = format!(
        "SELECT md5(row_to_json(mitosis_row)::text) || ' ' || jsonb_build_array({cols})::text \
         FROM {q} AS mitosis_row ORDER BY {cols};",
        cols = pk_list(pk)
    );
    let out = side.query(sql).await?;
    Ok(out
        .lines()
        .filter_map(|l| l.split_once(' '))
        .map(|(h, k)| (k.to_string(), h.to_string()))
        .collect())
}

fn canon(text: &str) -> Result<Value> {
    Ok(serde_json::from_str(text)?)
}

/// The rows with the given key texts, keyed by their canonical key.
async fn fetch_rows(
    side: &Side,
    q: &str,
    pk: &[String],
    keys: &[&str],
) -> Result<HashMap<String, Value>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let lits = keys.iter().map(|k| ql(k)).collect::<Vec<_>>().join(", ");
    let cols = pk_list(pk);
    let sql = format!(
        "SELECT jsonb_build_array(jsonb_build_array({cols}), to_jsonb(mitosis_row))::text \
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

    let (bh, ah) = tokio::try_join!(
        async {
            if bi.is_some() {
                row_hashes(before, &q, &pk).await
            } else {
                Ok(Vec::new())
            }
        },
        async {
            if ai.is_some() {
                row_hashes(after, &q, &pk).await
            } else {
                Ok(Vec::new())
            }
        }
    )?;
    // shortcut: both sides are held in memory; the 1,000,000-row cap keeps that small.
    let bmap: HashMap<&str, &str> = bh.iter().map(|(k, h)| (k.as_str(), h.as_str())).collect();
    let amap: HashMap<&str, &str> = ah.iter().map(|(k, h)| (k.as_str(), h.as_str())).collect();
    let deleted: Vec<&str> = bh
        .iter()
        .map(|(k, _)| k.as_str())
        .filter(|k| !amap.contains_key(k))
        .collect();
    let inserted: Vec<&str> = ah
        .iter()
        .map(|(k, _)| k.as_str())
        .filter(|k| !bmap.contains_key(k))
        .collect();
    let updated: Vec<&str> = ah
        .iter()
        .filter(|(k, h)| bmap.get(k.as_str()).is_some_and(|bh| *bh != h.as_str()))
        .map(|(k, _)| k.as_str())
        .collect();

    let (del, ins, upd) = (
        &deleted[..deleted.len().min(limit)],
        &inserted[..inserted.len().min(limit)],
        &updated[..updated.len().min(limit)],
    );
    let (bkeys, akeys) = ([del, upd].concat(), [ins, upd].concat());
    let (brows, arows) = tokio::try_join!(
        fetch_rows(before, &q, &pk, &bkeys),
        fetch_rows(after, &q, &pk, &akeys)
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
        inserted_total: inserted.len() as u64,
        deleted_total: deleted.len() as u64,
        updated_total: updated.len() as u64,
    };
    for k in ins {
        out.inserted.push(by_key(&arows, k)?);
    }
    for k in del {
        out.deleted.push(by_key(&brows, k)?);
    }
    for k in upd {
        out.updated.push(RowChange {
            pk: pk_value(k)?,
            columns: changed_columns(&by_key(&brows, k)?, &by_key(&arows, k)?),
        });
    }
    Ok(out)
}

// ---- human output -------------------------------------------------------------------------

fn count_text(c: &Count) -> String {
    format!("{}{}", if c.estimate { "~" } else { "" }, c.n)
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
        .filter(|t| t.before != t.after || t.before.estimate || t.after.estimate)
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
