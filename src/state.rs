use crate::error::{ErrorCode, MitosisError, Result};
use crate::protocol::TemplateSource;
use rusqlite::{params, Connection, OptionalExtension, Row};
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
    pub dir: PathBuf,
    pub pg_major: u32,
    pub bin_dir: PathBuf,
    pub extensions: Vec<String>,
    pub database: String,
    pub source: TemplateSource,
    pub migrations: Option<PathBuf>,
    pub migrations_hash: Option<String>,
    pub max_age_secs: u64,
    pub built_at: i64,
    pub size_bytes: u64,
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
}

pub struct Store {
    conn: Connection,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS templates (
    name TEXT PRIMARY KEY,
    dir TEXT NOT NULL,
    pg_major INTEGER NOT NULL,
    bin_dir TEXT NOT NULL,
    extensions TEXT NOT NULL,
    database TEXT NOT NULL,
    source TEXT NOT NULL,
    migrations TEXT,
    migrations_hash TEXT,
    max_age_secs INTEGER NOT NULL,
    built_at INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL
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
    status TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS clones_name_uniq ON clones(name) WHERE name IS NOT NULL;
";

const TEMPLATE_COLS: &str = "name, dir, pg_major, bin_dir, extensions, database, source, migrations, migrations_hash, max_age_secs, built_at, size_bytes";
const CLONE_COLS: &str = "id, name, template, dir, port, socket_dir, password, database, pg_pid, pg_start_time, parent_pid, parent_start_time, created_at, expires_at, durable, status";

fn conv_err(idx: usize, msg: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        idx,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, msg)),
    )
}

fn template_from_row(r: &Row) -> rusqlite::Result<TemplateRow> {
    let extensions: String = r.get(4)?;
    let source: String = r.get(6)?;
    Ok(TemplateRow {
        name: r.get(0)?,
        dir: PathBuf::from(r.get::<_, String>(1)?),
        pg_major: r.get::<_, i64>(2)? as u32,
        bin_dir: PathBuf::from(r.get::<_, String>(3)?),
        extensions: serde_json::from_str(&extensions).map_err(|e| conv_err(4, e.to_string()))?,
        database: r.get(5)?,
        source: serde_json::from_str(&source).map_err(|e| conv_err(6, e.to_string()))?,
        migrations: r.get::<_, Option<String>>(7)?.map(PathBuf::from),
        migrations_hash: r.get(8)?,
        max_age_secs: r.get::<_, i64>(9)? as u64,
        built_at: r.get(10)?,
        size_bytes: r.get::<_, i64>(11)? as u64,
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
        status: CloneStatus::parse(&status).ok_or_else(|| conv_err(15, format!("bad status {status}")))?,
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
        ]
    };
}

impl Store {
    pub fn open(path: &Path) -> Result<Store> {
        // Create the file 0600 first so the -wal/-shm files SQLite derives from it inherit that mode.
        std::fs::OpenOptions::new().create(true).append(true).mode(0o600).open(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        let conn = Connection::open(path)?;
        // journal_mode returns a row, so read it instead of executing.
        conn.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
        conn.execute_batch(SCHEMA)?;
        Ok(Store { conn })
    }

    pub fn upsert_template(&self, t: &TemplateRow) -> Result<()> {
        self.conn.execute(
            &format!("INSERT OR REPLACE INTO templates ({TEMPLATE_COLS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)"),
            params![
                t.name,
                p(&t.dir),
                t.pg_major as i64,
                p(&t.bin_dir),
                serde_json::to_string(&t.extensions)?,
                t.database,
                serde_json::to_string(&t.source)?,
                t.migrations.as_deref().map(p),
                t.migrations_hash,
                t.max_age_secs as i64,
                t.built_at,
                t.size_bytes as i64,
            ],
        )?;
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
        let mut stmt = self.conn.prepare(&format!("SELECT {TEMPLATE_COLS} FROM templates ORDER BY name"))?;
        let rows = stmt.query_map([], template_from_row)?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn insert_clone(&self, c: &CloneRow) -> Result<()> {
        self.conn
            .execute(
                &format!("INSERT INTO clones ({CLONE_COLS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)"),
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

    pub fn update_clone(&self, c: &CloneRow) -> Result<()> {
        self.conn.execute(
            "UPDATE clones SET name=?2, template=?3, dir=?4, port=?5, socket_dir=?6, password=?7, database=?8, \
             pg_pid=?9, pg_start_time=?10, parent_pid=?11, parent_start_time=?12, created_at=?13, expires_at=?14, \
             durable=?15, status=?16 WHERE id=?1",
            clone_params!(c),
        )?;
        Ok(())
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
        let mut stmt = self.conn.prepare(&format!("SELECT {CLONE_COLS} FROM clones ORDER BY created_at, id"))?;
        let rows = stmt.query_map([], clone_from_row)?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn delete_clone(&self, id: &str) -> Result<()> {
        self.conn.execute("DELETE FROM clones WHERE id = ?1", [id])?;
        Ok(())
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
        TemplateRow {
            name: name.into(),
            dir: PathBuf::from("/tmp/t"),
            pg_major: 16,
            bin_dir: PathBuf::from("/opt/pg16/bin"),
            extensions: vec!["pgcrypto".into()],
            database: "app".into(),
            source: TemplateSource::Url { url: "postgres://x".into() },
            migrations: Some(PathBuf::from("/tmp/m")),
            migrations_hash: Some("abc".into()),
            max_age_secs: 604_800,
            built_at: 1_700_000_000,
            size_bytes: 1,
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
        }
    }

    #[test]
    fn template_roundtrip_and_upsert() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("state.db")).unwrap();
        assert!(store.get_template("main").unwrap().is_none());
        store.upsert_template(&tpl("main")).unwrap();
        assert_eq!(store.get_template("main").unwrap().unwrap(), tpl("main"));
        store.upsert_template(&TemplateRow { size_bytes: 42, ..tpl("main") }).unwrap();
        assert_eq!(store.get_template("main").unwrap().unwrap().size_bytes, 42);
        store.upsert_template(&tpl("other")).unwrap();
        assert_eq!(store.list_templates().unwrap().len(), 2);
    }

    #[test]
    fn clone_crud_names_and_ports() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("state.db")).unwrap();
        store.insert_clone(&clone_row("c1", Some("x"), 54320)).unwrap();
        assert_eq!(
            store.insert_clone(&clone_row("c2", Some("x"), 54321)).unwrap_err().code,
            ErrorCode::InvalidRequest
        );
        // unnamed clones never collide on name
        store.insert_clone(&clone_row("c3", None, 54322)).unwrap();
        store.insert_clone(&clone_row("c4", None, 54323)).unwrap();

        assert_eq!(store.get_clone("x").unwrap().unwrap().id, "c1");
        assert_eq!(store.get_clone("c1").unwrap().unwrap().port, 54320);
        assert_eq!(store.get_clone("c1").unwrap().unwrap(), clone_row("c1", Some("x"), 54320));
        assert!(store.get_clone("nope").unwrap().is_none());
        assert!(store.leased_ports().unwrap().contains(&54320));
        assert_eq!(store.list_clones().unwrap().len(), 4);

        let mut c = clone_row("c1", Some("x"), 54320);
        c.status = CloneStatus::Stopping;
        c.durable = true;
        c.parent_pid = Some(9);
        store.update_clone(&c).unwrap();
        assert_eq!(store.get_clone("c1").unwrap().unwrap(), c);

        store.delete_clone("c1").unwrap();
        assert!(store.get_clone("c1").unwrap().is_none());
        assert!(!store.leased_ports().unwrap().contains(&54320));
    }

    #[test]
    fn reopening_keeps_rows_and_file_is_private() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let store = Store::open(&path).unwrap();
            store.upsert_template(&tpl("main")).unwrap();
            store.insert_clone(&clone_row("c1", Some("x"), 54320)).unwrap();
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
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
    }
}
