use crate::error::{ErrorCode, MitosisError, Result};
use crate::protocol::{GenKind, MigrateCmd, OnStale, PendingRestore, RefreshMode, TemplateSource};
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashSet;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloneStatus {
    Creating,
    Starting,
    Ready,
    Stopping,
}

impl CloneStatus {
    fn as_str(self) -> &'static str {
        match self {
            CloneStatus::Creating => "creating",
            CloneStatus::Starting => "starting",
            CloneStatus::Ready => "ready",
            CloneStatus::Stopping => "stopping",
        }
    }

    fn parse(s: &str) -> Option<CloneStatus> {
        Some(match s {
            "creating" => CloneStatus::Creating,
            "starting" => CloneStatus::Starting,
            "ready" => CloneStatus::Ready,
            "stopping" => CloneStatus::Stopping,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TemplateRow {
    pub name: String,
    pub database: String,
    pub source: TemplateSource,
    pub migrations: Option<PathBuf>,
    pub max_age_secs: u64,
    /// 0 until the first generation is committed.
    pub current_gen: u32,
    /// The number `alloc_gen` hands out next; never goes down.
    pub next_gen: u32,
    pub keep: u32,
    pub every_secs: Option<u64>,
    pub every_mode: RefreshMode,
    pub migrate: Option<MigrateCmd>,
    pub auto_migrate: bool,
    pub on_stale: OnStale,
    pub paused: bool,
    pub last_error: Option<String>,
    pub last_attempt_at: Option<i64>,
}

impl TemplateRow {
    /// A template with no generation yet and default settings.
    pub fn new(
        name: &str,
        database: &str,
        source: TemplateSource,
        migrations: Option<PathBuf>,
        max_age_secs: u64,
    ) -> TemplateRow {
        TemplateRow {
            name: name.into(),
            database: database.into(),
            source,
            migrations,
            max_age_secs,
            current_gen: 0,
            next_gen: 1,
            keep: DEFAULT_KEEP,
            every_secs: None,
            every_mode: RefreshMode::default(),
            migrate: None,
            auto_migrate: false,
            on_stale: OnStale::default(),
            paused: false,
            last_error: None,
            last_attempt_at: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GenerationRow {
    pub template: String,
    pub gen: u32,
    pub kind: GenKind,
    pub dir: PathBuf,
    pub pg_major: u32,
    pub bin_dir: PathBuf,
    pub extensions: Vec<String>,
    pub migrations_hash: Option<String>,
    pub built_at: i64,
    pub size_bytes: u64,
    pub from_clone: Option<String>,
    pub from_gen: Option<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CloneRow {
    pub id: String,
    pub name: Option<String>,
    pub template: String,
    pub dir: PathBuf,
    pub port: u16,
    pub socket_dir: PathBuf,
    pub password: String,
    pub database: String,
    pub pg_pid: Option<i32>,
    pub pg_start_time: Option<u64>,
    pub parent_pid: Option<i32>,
    pub parent_start_time: Option<u64>,
    pub created_at: i64,
    pub expires_at: i64,
    pub durable: bool,
    pub status: CloneStatus,
    pub generation: u32,
    pub protected: bool,
    pub suspect: bool,
}

/// A clone name's sticky port and password; kept after the clone is destroyed.
#[derive(Clone, Debug, PartialEq)]
pub struct ReservationRow {
    pub name: String,
    pub port: u16,
    pub password: String,
    pub template: String,
    pub last_used: i64,
}

pub struct Store {
    conn: Connection,
}

/// SQLite `user_version` of the current schema; v0.1 stores are version 0.
pub const SCHEMA_VERSION: i32 = 2;
pub const DEFAULT_KEEP: u32 = 3;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS templates (
    name TEXT PRIMARY KEY,
    database TEXT NOT NULL,
    source TEXT NOT NULL,
    migrations TEXT,
    max_age_secs INTEGER NOT NULL,
    current_gen INTEGER NOT NULL,
    next_gen INTEGER NOT NULL,
    keep INTEGER NOT NULL DEFAULT 3,
    every_secs INTEGER,
    every_mode TEXT NOT NULL DEFAULT 'full',
    migrate TEXT,
    auto_migrate INTEGER NOT NULL DEFAULT 0,
    on_stale TEXT NOT NULL DEFAULT 'warn',
    paused INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at INTEGER
);
CREATE TABLE IF NOT EXISTS generations (
    template TEXT NOT NULL,
    gen INTEGER NOT NULL,
    kind TEXT NOT NULL,
    dir TEXT NOT NULL,
    pg_major INTEGER NOT NULL,
    bin_dir TEXT NOT NULL,
    extensions TEXT NOT NULL,
    migrations_hash TEXT,
    built_at INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    from_clone TEXT,
    from_gen INTEGER,
    PRIMARY KEY (template, gen)
);
CREATE TABLE IF NOT EXISTS clones (
    id TEXT PRIMARY KEY,
    name TEXT,
    template TEXT NOT NULL,
    dir TEXT NOT NULL,
    port INTEGER NOT NULL,
    socket_dir TEXT NOT NULL,
    password TEXT NOT NULL,
    database TEXT NOT NULL,
    pg_pid INTEGER,
    pg_start_time INTEGER,
    parent_pid INTEGER,
    parent_start_time INTEGER,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    durable INTEGER NOT NULL,
    status TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1,
    protected INTEGER NOT NULL DEFAULT 0,
    suspect INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS clones_name_uniq ON clones(name) WHERE name IS NOT NULL;
";

/// v0.1 -> v2: each template row becomes a template plus generation 1 in its old folder.
const MIGRATE_V1: &str = "
ALTER TABLE templates RENAME TO templates_v1;
ALTER TABLE clones ADD COLUMN generation INTEGER NOT NULL DEFAULT 1;
ALTER TABLE clones ADD COLUMN protected INTEGER NOT NULL DEFAULT 0;
ALTER TABLE clones ADD COLUMN suspect INTEGER NOT NULL DEFAULT 0;
";
const MIGRATE_V1_ROWS: &str = "
INSERT INTO templates (name, database, source, migrations, max_age_secs, current_gen, next_gen)
    SELECT name, database, source, migrations, max_age_secs, 1, 2 FROM templates_v1;
INSERT INTO generations (template, gen, kind, dir, pg_major, bin_dir, extensions, migrations_hash, built_at, size_bytes)
    SELECT name, 1, 'full', dir, pg_major, bin_dir, extensions, migrations_hash, built_at, size_bytes FROM templates_v1;
DROP TABLE templates_v1;
";

/// Runs on every open: a v2 store made before reservations existed gains the table too.
const RESERVATIONS: &str = "
CREATE TABLE IF NOT EXISTS reservations (
    name TEXT PRIMARY KEY,
    port INTEGER NOT NULL,
    password TEXT NOT NULL,
    template TEXT NOT NULL,
    last_used INTEGER NOT NULL
);
";

/// Runs on every open, like `RESERVATIONS`.
const ENV_TABLES: &str = "
CREATE TABLE IF NOT EXISTS env_records (
    clone_id TEXT NOT NULL,
    file TEXT NOT NULL,
    var TEXT NOT NULL,
    previous TEXT,
    url TEXT NOT NULL,
    PRIMARY KEY (clone_id, file, var)
);
CREATE TABLE IF NOT EXISTS pending_restores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file TEXT NOT NULL,
    var TEXT NOT NULL,
    previous TEXT,
    url TEXT NOT NULL
);
";

const TEMPLATE_COLS: &str = "name, database, source, migrations, max_age_secs, current_gen, next_gen, keep, every_secs, every_mode, migrate, auto_migrate, on_stale, paused, last_error, last_attempt_at";
const GEN_COLS: &str = "template, gen, kind, dir, pg_major, bin_dir, extensions, migrations_hash, built_at, size_bytes, from_clone, from_gen";
const CLONE_COLS: &str = "id, name, template, dir, port, socket_dir, password, database, pg_pid, pg_start_time, parent_pid, parent_start_time, created_at, expires_at, durable, status, generation, protected, suspect";

fn conv_err(idx: usize, msg: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        idx,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, msg)),
    )
}

/// A unit enum as its serde name (`"full"`), for TEXT columns.
fn enum_str<T: Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

fn enum_col<T: DeserializeOwned>(r: &Row, idx: usize) -> rusqlite::Result<T> {
    let s: String = r.get(idx)?;
    serde_json::from_value(serde_json::Value::String(s)).map_err(|e| conv_err(idx, e.to_string()))
}

fn json_col<T: DeserializeOwned>(r: &Row, idx: usize) -> rusqlite::Result<T> {
    let s: String = r.get(idx)?;
    serde_json::from_str(&s).map_err(|e| conv_err(idx, e.to_string()))
}

fn template_from_row(r: &Row) -> rusqlite::Result<TemplateRow> {
    Ok(TemplateRow {
        name: r.get(0)?,
        database: r.get(1)?,
        source: json_col(r, 2)?,
        migrations: r.get::<_, Option<String>>(3)?.map(PathBuf::from),
        max_age_secs: r.get::<_, i64>(4)? as u64,
        current_gen: r.get::<_, i64>(5)? as u32,
        next_gen: r.get::<_, i64>(6)? as u32,
        keep: r.get::<_, i64>(7)? as u32,
        every_secs: r.get::<_, Option<i64>>(8)?.map(|v| v as u64),
        every_mode: enum_col(r, 9)?,
        migrate: match r.get::<_, Option<String>>(10)? {
            Some(s) => Some(serde_json::from_str(&s).map_err(|e| conv_err(10, e.to_string()))?),
            None => None,
        },
        auto_migrate: r.get::<_, i64>(11)? != 0,
        on_stale: enum_col(r, 12)?,
        paused: r.get::<_, i64>(13)? != 0,
        last_error: r.get(14)?,
        last_attempt_at: r.get(15)?,
    })
}

fn gen_from_row(r: &Row) -> rusqlite::Result<GenerationRow> {
    Ok(GenerationRow {
        template: r.get(0)?,
        gen: r.get::<_, i64>(1)? as u32,
        kind: enum_col(r, 2)?,
        dir: PathBuf::from(r.get::<_, String>(3)?),
        pg_major: r.get::<_, i64>(4)? as u32,
        bin_dir: PathBuf::from(r.get::<_, String>(5)?),
        extensions: json_col(r, 6)?,
        migrations_hash: r.get(7)?,
        built_at: r.get(8)?,
        size_bytes: r.get::<_, i64>(9)? as u64,
        from_clone: r.get(10)?,
        from_gen: r.get::<_, Option<i64>>(11)?.map(|v| v as u32),
    })
}

fn clone_from_row(r: &Row) -> rusqlite::Result<CloneRow> {
    let status: String = r.get(15)?;
    Ok(CloneRow {
        id: r.get(0)?,
        name: r.get(1)?,
        template: r.get(2)?,
        dir: PathBuf::from(r.get::<_, String>(3)?),
        port: r.get::<_, i64>(4)? as u16,
        socket_dir: PathBuf::from(r.get::<_, String>(5)?),
        password: r.get(6)?,
        database: r.get(7)?,
        pg_pid: r.get::<_, Option<i64>>(8)?.map(|v| v as i32),
        pg_start_time: r.get::<_, Option<i64>>(9)?.map(|v| v as u64),
        parent_pid: r.get::<_, Option<i64>>(10)?.map(|v| v as i32),
        parent_start_time: r.get::<_, Option<i64>>(11)?.map(|v| v as u64),
        created_at: r.get(12)?,
        expires_at: r.get(13)?,
        durable: r.get::<_, i64>(14)? != 0,
        status: CloneStatus::parse(&status)
            .ok_or_else(|| conv_err(15, format!("bad status {status}")))?,
        generation: r.get::<_, i64>(16)? as u32,
        protected: r.get::<_, i64>(17)? != 0,
        suspect: r.get::<_, i64>(18)? != 0,
    })
}

fn p(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

// Shared by insert/update; column order matches CLONE_COLS.
macro_rules! clone_params {
    ($c:expr) => {
        params![
            $c.id,
            $c.name,
            $c.template,
            p(&$c.dir),
            $c.port as i64,
            p(&$c.socket_dir),
            $c.password,
            $c.database,
            $c.pg_pid.map(|v| v as i64),
            $c.pg_start_time.map(|v| v as i64),
            $c.parent_pid.map(|v| v as i64),
            $c.parent_start_time.map(|v| v as i64),
            $c.created_at,
            $c.expires_at,
            $c.durable as i64,
            $c.status.as_str(),
            $c.generation as i64,
            $c.protected as i64,
            $c.suspect as i64,
        ]
    };
}

// Shared by insert/update; column order matches TEMPLATE_COLS.
macro_rules! template_params {
    ($t:expr) => {
        params![
            $t.name,
            $t.database,
            serde_json::to_string(&$t.source)?,
            $t.migrations.as_deref().map(p),
            $t.max_age_secs as i64,
            $t.current_gen as i64,
            $t.next_gen as i64,
            $t.keep as i64,
            $t.every_secs.map(|v| v as i64),
            enum_str(&$t.every_mode),
            $t.migrate.as_ref().map(serde_json::to_string).transpose()?,
            $t.auto_migrate as i64,
            enum_str(&$t.on_stale),
            $t.paused as i64,
            $t.last_error,
            $t.last_attempt_at,
        ]
    };
}

/// Brings a store of any older version to `SCHEMA_VERSION` in one transaction.
fn migrate(conn: &mut Connection) -> Result<()> {
    let version: i32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(MitosisError::new(
            ErrorCode::Internal,
            format!(
                "state.db has schema version {version}; this mitosis knows up to {SCHEMA_VERSION}"
            ),
        )
        .with_hint("run a newer mitosis"));
    }
    conn.execute_batch(RESERVATIONS)?;
    conn.execute_batch(ENV_TABLES)?;
    if version == SCHEMA_VERSION {
        return Ok(());
    }
    let tx = conn.transaction()?;
    // A v0.1 store has a templates table but no version; a new file has no tables.
    let v01 = tx.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'templates'",
        [],
        |r| r.get::<_, i64>(0),
    )? > 0;
    if v01 {
        tx.execute_batch(MIGRATE_V1)?;
    }
    tx.execute_batch(SCHEMA)?;
    if v01 {
        tx.execute_batch(MIGRATE_V1_ROWS)?;
    }
    tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))?;
    tx.commit()?;
    Ok(())
}

impl Store {
    pub fn open(path: &Path) -> Result<Store> {
        // Create the file 0600 first so the -wal/-shm files SQLite derives from it inherit that mode.
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        let mut conn = Connection::open(path)?;
        // journal_mode returns a row, so read it instead of executing.
        conn.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
        migrate(&mut conn)?;
        Ok(Store { conn })
    }

    /// Fails when the name is taken.
    pub fn insert_template(&self, t: &TemplateRow) -> Result<()> {
        self.conn.execute(
            &format!("INSERT INTO templates ({TEMPLATE_COLS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)"),
            template_params!(t),
        )?;
        Ok(())
    }

    /// Writes every column except the generation pointers (`current_gen`, `next_gen`).
    pub fn update_template_settings(&self, t: &TemplateRow) -> Result<()> {
        let n = self.conn.execute(
            "UPDATE templates SET database=?2, source=?3, migrations=?4, max_age_secs=?5, keep=?8, \
             every_secs=?9, every_mode=?10, migrate=?11, auto_migrate=?12, on_stale=?13, paused=?14, \
             last_error=?15, last_attempt_at=?16 WHERE name=?1",
            template_params!(t),
        )?;
        if n == 0 {
            return Err(MitosisError::new(
                ErrorCode::TemplateNotFound,
                format!("template {} not found", t.name),
            ));
        }
        Ok(())
    }

    /// Removes the template and all its generation rows (not their folders).
    pub fn delete_template(&self, name: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM generations WHERE template = ?1", [name])?;
        self.conn
            .execute("DELETE FROM templates WHERE name = ?1", [name])?;
        Ok(())
    }

    pub fn get_template(&self, name: &str) -> Result<Option<TemplateRow>> {
        Ok(self
            .conn
            .query_row(
                &format!("SELECT {TEMPLATE_COLS} FROM templates WHERE name = ?1"),
                [name],
                template_from_row,
            )
            .optional()?)
    }

    pub fn list_templates(&self) -> Result<Vec<TemplateRow>> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {TEMPLATE_COLS} FROM templates ORDER BY name"
        ))?;
        let rows = stmt
            .query_map([], template_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Returns the template's next generation number and moves `next_gen` past it.
    pub fn alloc_gen(&self, name: &str) -> Result<u32> {
        let gen = self
            .conn
            .query_row(
                "UPDATE templates SET next_gen = next_gen + 1 WHERE name = ?1 RETURNING next_gen - 1",
                [name],
                |r| r.get::<_, i64>(0),
            )
            .optional()?;
        gen.map(|g| g as u32).ok_or_else(|| {
            MitosisError::new(
                ErrorCode::TemplateNotFound,
                format!("template {name} not found"),
            )
        })
    }

    pub fn insert_generation(&self, g: &GenerationRow) -> Result<()> {
        self.conn.execute(
            &format!("INSERT INTO generations ({GEN_COLS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)"),
            params![
                g.template,
                g.gen as i64,
                enum_str(&g.kind),
                p(&g.dir),
                g.pg_major as i64,
                p(&g.bin_dir),
                serde_json::to_string(&g.extensions)?,
                g.migrations_hash,
                g.built_at,
                g.size_bytes as i64,
                g.from_clone,
                g.from_gen.map(|v| v as i64),
            ],
        )?;
        Ok(())
    }

    pub fn get_generation(&self, name: &str, gen: u32) -> Result<Option<GenerationRow>> {
        Ok(self
            .conn
            .query_row(
                &format!("SELECT {GEN_COLS} FROM generations WHERE template = ?1 AND gen = ?2"),
                params![name, gen as i64],
                gen_from_row,
            )
            .optional()?)
    }

    /// Oldest first.
    pub fn list_generations(&self, name: &str) -> Result<Vec<GenerationRow>> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {GEN_COLS} FROM generations WHERE template = ?1 ORDER BY gen"
        ))?;
        let rows = stmt
            .query_map([name], gen_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// The template and its current generation; `None` while it has none.
    pub fn current(&self, name: &str) -> Result<Option<(TemplateRow, GenerationRow)>> {
        let Some(t) = self.get_template(name)? else {
            return Ok(None);
        };
        Ok(self.get_generation(name, t.current_gen)?.map(|g| (t, g)))
    }

    /// Compare-and-set of the current pointer: changes it only while it still equals `expected`
    /// (`None`: always) and generation `gen` exists. True when it was changed.
    pub fn set_current(&self, name: &str, gen: u32, expected: Option<u32>) -> Result<bool> {
        Ok(self.conn.execute(
            "UPDATE templates SET current_gen = ?2 WHERE name = ?1 AND (?3 IS NULL OR current_gen = ?3) \
             AND EXISTS (SELECT 1 FROM generations WHERE template = ?1 AND gen = ?2)",
            params![name, gen as i64, expected.map(|e| e as i64)],
        )? > 0)
    }

    pub fn delete_generation(&self, name: &str, gen: u32) -> Result<()> {
        self.conn.execute(
            "DELETE FROM generations WHERE template = ?1 AND gen = ?2",
            params![name, gen as i64],
        )?;
        Ok(())
    }

    pub fn insert_clone(&self, c: &CloneRow) -> Result<()> {
        self.conn
            .execute(
                &format!("INSERT INTO clones ({CLONE_COLS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)"),
                clone_params!(c),
            )
            .map_err(|e| match &e {
                rusqlite::Error::SqliteFailure(f, _) if f.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE => {
                    MitosisError::new(
                        ErrorCode::InvalidRequest,
                        format!("a clone named '{}' already exists", c.name.as_deref().unwrap_or(&c.id)),
                    )
                }
                _ => e.into(),
            })?;
        Ok(())
    }

    /// `CloneNotFound` when the row is gone (for example destroyed meanwhile).
    pub fn update_clone(&self, c: &CloneRow) -> Result<()> {
        let n = self.conn.execute(
            "UPDATE clones SET name=?2, template=?3, dir=?4, port=?5, socket_dir=?6, password=?7, database=?8, \
             pg_pid=?9, pg_start_time=?10, parent_pid=?11, parent_start_time=?12, created_at=?13, expires_at=?14, \
             durable=?15, status=?16, generation=?17, protected=?18, suspect=?19 WHERE id=?1",
            clone_params!(c),
        )?;
        if n == 0 {
            return Err(MitosisError::new(
                ErrorCode::CloneNotFound,
                format!("clone {} no longer exists", c.id),
            ));
        }
        Ok(())
    }

    /// Changes only the status column; false when no row has this id.
    pub fn set_clone_status(&self, id: &str, status: CloneStatus) -> Result<bool> {
        Ok(self.conn.execute(
            "UPDATE clones SET status = ?2 WHERE id = ?1",
            params![id, status.as_str()],
        )? > 0)
    }

    /// Matches id first, then name.
    pub fn get_clone(&self, id_or_name: &str) -> Result<Option<CloneRow>> {
        Ok(self
            .conn
            .query_row(
                &format!("SELECT {CLONE_COLS} FROM clones WHERE id = ?1 OR name = ?1 ORDER BY id = ?1 DESC LIMIT 1"),
                [id_or_name],
                clone_from_row,
            )
            .optional()?)
    }

    pub fn list_clones(&self) -> Result<Vec<CloneRow>> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {CLONE_COLS} FROM clones ORDER BY created_at, id"
        ))?;
        let rows = stmt
            .query_map([], clone_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn delete_clone(&self, id: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM clones WHERE id = ?1", [id])?;
        Ok(())
    }

    /// Remembers a `.env` entry that holds the clone's URL. A second record for the same file and
    /// variable keeps the first `previous` (the user's own value) and only updates the URL.
    pub fn add_env_record(&self, clone_id: &str, rec: &PendingRestore) -> Result<()> {
        self.conn.execute(
            "INSERT INTO env_records (clone_id, file, var, previous, url) VALUES (?1,?2,?3,?4,?5) \
             ON CONFLICT(clone_id, file, var) DO UPDATE SET url = excluded.url",
            params![
                clone_id,
                rec.file.to_string_lossy(),
                rec.var,
                rec.previous,
                rec.url
            ],
        )?;
        Ok(())
    }

    /// The clone's `.env` records as restores (`id` is 0).
    pub fn env_records(&self, clone_id: &str) -> Result<Vec<PendingRestore>> {
        let mut stmt = self.conn.prepare(
            "SELECT file, var, previous, url FROM env_records WHERE clone_id = ?1 ORDER BY rowid",
        )?;
        let rows = stmt
            .query_map([clone_id], |r| {
                Ok(PendingRestore {
                    id: 0,
                    file: PathBuf::from(r.get::<_, String>(0)?),
                    var: r.get(1)?,
                    previous: r.get(2)?,
                    url: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn delete_env_records(&self, clone_id: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM env_records WHERE clone_id = ?1", [clone_id])?;
        Ok(())
    }

    /// Drops the one record for `file` and `var` and returns its `previous`: a `--fresh` create
    /// takes over the entry, so the destroy of the old clone must not restore it.
    pub fn take_env_record(
        &self,
        clone_id: &str,
        file: &Path,
        var: &str,
    ) -> Result<Option<Option<String>>> {
        Ok(self
            .conn
            .query_row(
                "DELETE FROM env_records WHERE clone_id = ?1 AND file = ?2 AND var = ?3 RETURNING previous",
                params![clone_id, file.to_string_lossy(), var],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?)
    }

    pub fn add_pending_restore(&self, rec: &PendingRestore) -> Result<()> {
        self.conn.execute(
            "INSERT INTO pending_restores (file, var, previous, url) VALUES (?1,?2,?3,?4)",
            params![rec.file.to_string_lossy(), rec.var, rec.previous, rec.url],
        )?;
        Ok(())
    }

    pub fn list_pending_restores(&self) -> Result<Vec<PendingRestore>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, file, var, previous, url FROM pending_restores ORDER BY id")?;
        let rows = stmt
            .query_map([], |r| {
                Ok(PendingRestore {
                    id: r.get(0)?,
                    file: PathBuf::from(r.get::<_, String>(1)?),
                    var: r.get(2)?,
                    previous: r.get(3)?,
                    url: r.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn delete_pending_restore(&self, id: i64) -> Result<()> {
        self.conn
            .execute("DELETE FROM pending_restores WHERE id = ?1", [id])?;
        Ok(())
    }

    pub fn get_reservation(&self, name: &str) -> Result<Option<ReservationRow>> {
        Ok(self
            .conn
            .query_row(
                "SELECT name, port, password, template, last_used FROM reservations WHERE name = ?1",
                [name],
                |r| {
                    Ok(ReservationRow {
                        name: r.get(0)?,
                        port: r.get::<_, i64>(1)? as u16,
                        password: r.get(2)?,
                        template: r.get(3)?,
                        last_used: r.get(4)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn upsert_reservation(&self, r: &ReservationRow) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO reservations (name, port, password, template, last_used) VALUES (?1,?2,?3,?4,?5)",
            params![r.name, i64::from(r.port), r.password, r.template, r.last_used],
        )?;
        Ok(())
    }

    pub fn delete_reservation(&self, name: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM reservations WHERE name = ?1", [name])?;
        Ok(())
    }

    /// Every reserved port, whether or not a clone of that name runs.
    pub fn reserved_ports(&self) -> Result<HashSet<u16>> {
        let mut stmt = self.conn.prepare("SELECT port FROM reservations")?;
        let ports = stmt
            .query_map([], |r| Ok(r.get::<_, i64>(0)? as u16))?
            .collect::<rusqlite::Result<HashSet<_>>>()?;
        Ok(ports)
    }

    /// Deletes reservations last used before `before`; returns their names.
    pub fn drop_stale_reservations(&self, before: i64) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("DELETE FROM reservations WHERE last_used < ?1 RETURNING name")?;
        let names = stmt
            .query_map([before], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(names)
    }

    /// The clone row is the port lease.
    pub fn leased_ports(&self) -> Result<HashSet<u16>> {
        let mut stmt = self.conn.prepare("SELECT port FROM clones")?;
        let ports = stmt
            .query_map([], |r| Ok(r.get::<_, i64>(0)? as u16))?
            .collect::<rusqlite::Result<HashSet<_>>>()?;
        Ok(ports)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tpl(name: &str) -> TemplateRow {
        TemplateRow::new(
            name,
            "app",
            TemplateSource::Url {
                url: "postgres://x".into(),
            },
            Some(PathBuf::from("/tmp/m")),
            604_800,
        )
    }

    fn gen(name: &str, gen: u32) -> GenerationRow {
        GenerationRow {
            template: name.into(),
            gen,
            kind: GenKind::Full,
            dir: PathBuf::from(format!("/tmp/t/{name}@{gen}")),
            pg_major: 16,
            bin_dir: PathBuf::from("/opt/pg16/bin"),
            extensions: vec!["pgcrypto".into()],
            migrations_hash: Some("abc".into()),
            built_at: 1_700_000_000,
            size_bytes: 1,
            from_clone: None,
            from_gen: None,
        }
    }

    fn clone_row(id: &str, name: Option<&str>, port: u16) -> CloneRow {
        CloneRow {
            id: id.into(),
            name: name.map(String::from),
            template: "main".into(),
            dir: PathBuf::from("/tmp/c"),
            port,
            socket_dir: PathBuf::from("/tmp/s"),
            password: "pw".into(),
            database: "app".into(),
            pg_pid: Some(123),
            pg_start_time: Some(456),
            parent_pid: None,
            parent_start_time: None,
            created_at: 1_700_000_000,
            expires_at: 1_700_007_200,
            durable: false,
            status: CloneStatus::Ready,
            generation: 1,
            protected: false,
            suspect: false,
        }
    }

    #[test]
    fn template_roundtrip_and_settings() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("state.db")).unwrap();
        assert!(store.get_template("main").unwrap().is_none());
        store.insert_template(&tpl("main")).unwrap();
        assert!(store.insert_template(&tpl("main")).is_err());
        assert_eq!(store.get_template("main").unwrap().unwrap(), tpl("main"));
        let changed = TemplateRow {
            keep: 5,
            every_secs: Some(3600),
            every_mode: RefreshMode::Layered,
            migrate: Some(MigrateCmd {
                command: "make migrate".into(),
                cwd: "/w".into(),
                path: Some("/bin".into()),
            }),
            auto_migrate: true,
            on_stale: OnStale::Refresh,
            paused: true,
            last_error: Some("boom".into()),
            last_attempt_at: Some(7),
            // pointers are not settings: these must be ignored
            current_gen: 9,
            next_gen: 9,
            ..tpl("main")
        };
        store.update_template_settings(&changed).unwrap();
        assert_eq!(
            store.get_template("main").unwrap().unwrap(),
            TemplateRow {
                current_gen: 0,
                next_gen: 1,
                ..changed
            }
        );
        assert_eq!(
            store
                .update_template_settings(&tpl("nope"))
                .unwrap_err()
                .code,
            ErrorCode::TemplateNotFound
        );
        store.insert_template(&tpl("other")).unwrap();
        assert_eq!(store.list_templates().unwrap().len(), 2);
        store.delete_template("other").unwrap();
        assert_eq!(store.list_templates().unwrap().len(), 1);
    }

    #[test]
    fn generations_alloc_cas_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("state.db")).unwrap();
        assert_eq!(
            store.alloc_gen("main").unwrap_err().code,
            ErrorCode::TemplateNotFound
        );
        store.insert_template(&tpl("main")).unwrap();
        assert!(store.current("main").unwrap().is_none());
        assert_eq!(store.alloc_gen("main").unwrap(), 1);
        assert_eq!(store.alloc_gen("main").unwrap(), 2);
        // gen 2 never got a row; numbers still move on
        assert_eq!(store.alloc_gen("main").unwrap(), 3);
        store.insert_generation(&gen("main", 3)).unwrap();
        store.insert_generation(&gen("main", 1)).unwrap();
        assert!(store.insert_generation(&gen("main", 1)).is_err());
        assert_eq!(
            store
                .list_generations("main")
                .unwrap()
                .iter()
                .map(|g| g.gen)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            store.get_generation("main", 3).unwrap().unwrap(),
            gen("main", 3)
        );

        // CAS: wrong expectation and missing generation both refuse
        assert!(!store.set_current("main", 3, Some(1)).unwrap());
        assert!(!store.set_current("main", 2, Some(0)).unwrap());
        assert!(store.set_current("main", 3, Some(0)).unwrap());
        let (t, g) = store.current("main").unwrap().unwrap();
        assert_eq!((t.current_gen, g.gen), (3, 3));
        assert!(store.set_current("main", 1, None).unwrap());
        assert_eq!(store.current("main").unwrap().unwrap().1.gen, 1);

        store.delete_generation("main", 3).unwrap();
        assert!(store.get_generation("main", 3).unwrap().is_none());
        assert_eq!(store.alloc_gen("main").unwrap(), 4);
    }

    /// The v0.1 schema, verbatim.
    const V01_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS templates (
    name TEXT PRIMARY KEY, dir TEXT NOT NULL, pg_major INTEGER NOT NULL, bin_dir TEXT NOT NULL,
    extensions TEXT NOT NULL, database TEXT NOT NULL, source TEXT NOT NULL, migrations TEXT,
    migrations_hash TEXT, max_age_secs INTEGER NOT NULL, built_at INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS clones (
    id TEXT PRIMARY KEY, name TEXT, template TEXT NOT NULL, dir TEXT NOT NULL, port INTEGER NOT NULL,
    socket_dir TEXT NOT NULL, password TEXT NOT NULL, database TEXT NOT NULL, pg_pid INTEGER,
    pg_start_time INTEGER, parent_pid INTEGER, parent_start_time INTEGER, created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL, durable INTEGER NOT NULL, status TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS clones_name_uniq ON clones(name) WHERE name IS NOT NULL;
";

    #[test]
    fn v01_store_is_migrated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(V01_SCHEMA).unwrap();
            conn.execute(
                "INSERT INTO templates VALUES ('main', '/h/templates/main', 16, '/opt/pg16/bin', \
                 '[\"plpgsql\"]', 'app', '{\"kind\":\"url\",\"url\":\"postgres://x\"}', NULL, 'h1', 604800, 5, 9)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO clones VALUES ('c1', 'x', 'main', '/h/clones/c1', 54320, '/h/s/c1', 'pw', \
                 'app', 1, 2, NULL, NULL, 3, 4, 0, 'ready')",
                [],
            )
            .unwrap();
        }
        let store = Store::open(&path).unwrap();
        let (t, g) = store.current("main").unwrap().unwrap();
        assert_eq!(
            (t.current_gen, t.next_gen, t.keep, t.paused),
            (1, 2, DEFAULT_KEEP, false)
        );
        assert_eq!(t.database, "app");
        assert_eq!(
            (g.gen, g.kind, g.dir.as_path(), g.pg_major),
            (1, GenKind::Full, Path::new("/h/templates/main"), 16)
        );
        assert_eq!(
            (
                g.extensions,
                g.migrations_hash.as_deref(),
                g.built_at,
                g.size_bytes
            ),
            (vec!["plpgsql".to_string()], Some("h1"), 5, 9)
        );
        let c = store.get_clone("c1").unwrap().unwrap();
        assert_eq!((c.generation, c.protected, c.suspect), (1, false, false));
        assert_eq!(store.alloc_gen("main").unwrap(), 2);
        drop(store);
        // Reopening is a no-op; a newer store is refused.
        let store = Store::open(&path).unwrap();
        assert_eq!(store.list_generations("main").unwrap().len(), 1);
        store
            .conn
            .execute_batch("PRAGMA user_version = 99")
            .unwrap();
        drop(store);
        assert!(Store::open(&path).is_err());
    }

    #[test]
    fn clone_crud_names_and_ports() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("state.db")).unwrap();
        store
            .insert_clone(&clone_row("c1", Some("x"), 54320))
            .unwrap();
        assert_eq!(
            store
                .insert_clone(&clone_row("c2", Some("x"), 54321))
                .unwrap_err()
                .code,
            ErrorCode::InvalidRequest
        );
        // unnamed clones never collide on name
        store.insert_clone(&clone_row("c3", None, 54322)).unwrap();
        store.insert_clone(&clone_row("c4", None, 54323)).unwrap();

        assert_eq!(store.get_clone("x").unwrap().unwrap().id, "c1");
        assert_eq!(store.get_clone("c1").unwrap().unwrap().port, 54320);
        assert_eq!(
            store.get_clone("c1").unwrap().unwrap(),
            clone_row("c1", Some("x"), 54320)
        );
        assert!(store.get_clone("nope").unwrap().is_none());
        assert!(store.leased_ports().unwrap().contains(&54320));
        assert_eq!(store.list_clones().unwrap().len(), 3);

        let mut c = clone_row("c1", Some("x"), 54320);
        c.status = CloneStatus::Stopping;
        c.durable = true;
        c.parent_pid = Some(9);
        store.update_clone(&c).unwrap();
        assert_eq!(store.get_clone("c1").unwrap().unwrap(), c);

        assert!(store.set_clone_status("c3", CloneStatus::Stopping).unwrap());
        let c3 = store.get_clone("c3").unwrap().unwrap();
        assert_eq!(
            c3,
            CloneRow {
                status: CloneStatus::Stopping,
                ..clone_row("c3", None, 54322)
            }
        );

        store.delete_clone("c1").unwrap();
        assert!(store.get_clone("c1").unwrap().is_none());
        assert_eq!(
            store.update_clone(&c).unwrap_err().code,
            ErrorCode::CloneNotFound
        );
        assert!(!store.set_clone_status("c1", CloneStatus::Ready).unwrap());
        assert!(!store.leased_ports().unwrap().contains(&54320));
    }

    #[test]
    fn reopening_keeps_rows_and_file_is_private() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let store = Store::open(&path).unwrap();
            store.insert_template(&tpl("main")).unwrap();
            store
                .insert_clone(&clone_row("c1", Some("x"), 54320))
                .unwrap();
            // while open, the WAL side files exist and must be private too
            for suffix in ["-wal", "-shm"] {
                let side = dir.path().join(format!("state.db{suffix}"));
                let mode = std::fs::metadata(&side).unwrap().permissions().mode() & 0o777;
                assert_eq!(mode, 0o600, "{suffix}");
            }
        }
        let store = Store::open(&path).unwrap();
        assert!(store.get_template("main").unwrap().is_some());
        assert_eq!(store.get_clone("x").unwrap().unwrap().id, "c1");
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
