use crate::clone_backend::{same_volume, ApfsBackend, CloneBackend};
use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use crate::pg::{discover, PgInstall};
use crate::state::Store;
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct Config {
    pub max_clones: usize,
    pub min_free_bytes: u64,
    pub port_range: RangeInclusive<u16>,
    pub default_max_age_secs: u64,
    pub ready_timeout: Duration,
    pub stop_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_clones: 8,
            min_free_bytes: 2_000_000_000,
            port_range: 54320..=54999,
            default_max_age_secs: 604_800,
            ready_timeout: Duration::from_secs(10),
            stop_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InFlightOp {
    Create,
    Destroy,
}

pub struct Ctx {
    pub home: Home,
    pub config: Config,
    pub store: Mutex<Store>,
    pub backend: Arc<dyn CloneBackend>,
    pub installs: Vec<PgInstall>,
    pub started_at: Instant,
    /// Template names with a build in progress (one build per name at a time).
    pub(crate) building: Mutex<HashSet<String>>,
    /// Clone ids with a create or destroy still running; gc and the reaper leave them alone.
    pub(crate) in_flight: Mutex<HashMap<String, InFlightOp>>,
    /// Clone ids with a parent-exit watcher thread, so gc does not start a second one.
    pub(crate) watching: Mutex<HashSet<String>>,
}

impl Ctx {
    pub fn open(home: Home, config: Config) -> Result<Ctx> {
        home.ensure_dirs()?;
        let backend: Arc<dyn CloneBackend> = Arc::new(ApfsBackend);
        backend.check_supported(&home.root)?;
        if !same_volume(&home.templates_dir(), &home.clones_dir())? {
            return Err(MitosisError::new(
                ErrorCode::CrossVolume,
                format!(
                    "{} and {} are on different volumes",
                    home.templates_dir().display(),
                    home.clones_dir().display()
                ),
            )
            .with_hint(
                "keep templates/ and clones/ on the same APFS volume (no symlinks to other disks)",
            ));
        }
        let store = Store::open(&home.state_db())?;
        Ok(Ctx {
            home,
            config,
            store: Mutex::new(store),
            backend,
            installs: discover(),
            started_at: Instant::now(),
            building: Mutex::new(HashSet::new()),
            in_flight: Mutex::new(HashMap::new()),
            watching: Mutex::new(HashSet::new()),
        })
    }

    /// Locks the store; use it for one store call at a time, never across Postgres work.
    pub fn store(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let c = Config::default();
        assert_eq!(c.max_clones, 8);
        assert_eq!(c.min_free_bytes, 2_000_000_000);
        assert_eq!(c.port_range, 54320..=54999);
        assert_eq!(c.default_max_age_secs, 604_800);
        assert_eq!(c.ready_timeout, Duration::from_secs(10));
        assert_eq!(c.stop_timeout, Duration::from_secs(10));
    }

    #[test]
    fn open_creates_layout() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = Ctx::open(Home::at(tmp.path().join("m")), Config::default()).unwrap();
        assert!(ctx.home.templates_dir().is_dir());
        assert!(ctx.home.state_db().is_file());
        assert!(ctx.store().list_templates().unwrap().is_empty());
    }

    #[allow(dead_code)]
    fn ctx_is_send_sync(c: Ctx) -> impl Send + Sync {
        c
    }
}
