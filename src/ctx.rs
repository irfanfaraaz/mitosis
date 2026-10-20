use crate::clone_backend::{same_volume, ApfsBackend, CloneBackend};
pub use crate::config::Config;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use crate::pg::{discover_with, find_major, newest, PgInstall};
use crate::protocol::EventRow;
use crate::state::Store;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Instant;

/// Events a slow follower may fall behind by before it skips some.
const EVENT_CHANNEL: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InFlightOp {
    Create,
    Destroy,
}

pub struct Ctx {
    pub home: Home,
    /// Read through `config()`; `set_config` swaps it on a reload.
    config: RwLock<Config>,
    pub store: Mutex<Store>,
    pub backend: Arc<dyn CloneBackend>,
    /// Postgres installs found on this machine; looked up again when a request finds no match.
    installs: RwLock<Vec<PgInstall>>,
    pub started_at: Instant,
    /// Template names with a build in progress (one build per name at a time).
    pub(crate) building: Mutex<HashSet<String>>,
    /// Clone ids with a create or destroy still running; gc and the reaper leave them alone.
    pub(crate) in_flight: Mutex<HashMap<String, InFlightOp>>,
    /// Clone ids with a parent-exit watcher thread, so gc does not start a second one.
    pub(crate) watching: Mutex<HashSet<String>>,
    /// Clone creation reads a template folder under the read guard; a template swap renames and
    /// removes folders under the write guard, so no clone copies a half-moved template. One lock
    /// for all templates: the guarded work is a clonefile or a swap.
    pub(crate) template_swap: tokio::sync::RwLock<()>,
    /// Process groups of running seed commands and dump/restore pipes; killed on daemon shutdown.
    pub(crate) build_groups: Arc<Mutex<HashSet<i32>>>,
    /// Woken whenever a clone goes away or the settings change, so queued creates look again.
    pub room: tokio::sync::Notify,
    /// Held by the one create at the head of the queue from its limit check until its row is
    /// reserved; tokio's mutex hands it out in arrival order, which is the FIFO order.
    pub(crate) admission: Arc<tokio::sync::Mutex<()>>,
    /// Creates waiting for room (including the one at the head).
    pub(crate) queued: AtomicUsize,
    /// Every event `events::emit` writes, for `mitosis events --follow` connections.
    pub events_tx: tokio::sync::broadcast::Sender<EventRow>,
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
        let installs = discover_with(Some(&home));
        Ok(Ctx {
            home,
            config: RwLock::new(config),
            store: Mutex::new(store),
            backend,
            installs: RwLock::new(installs),
            started_at: Instant::now(),
            building: Mutex::new(HashSet::new()),
            in_flight: Mutex::new(HashMap::new()),
            watching: Mutex::new(HashSet::new()),
            template_swap: tokio::sync::RwLock::new(()),
            build_groups: Arc::new(Mutex::new(HashSet::new())),
            room: tokio::sync::Notify::new(),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            queued: AtomicUsize::new(0),
            events_tx: tokio::sync::broadcast::channel(EVENT_CHANNEL).0,
        })
    }

    /// A snapshot of the current settings; a later reload does not change it.
    pub fn config(&self) -> Config {
        self.config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn set_config(&self, config: Config) {
        *self.config.write().unwrap_or_else(|e| e.into_inner()) = config;
    }

    /// A snapshot of the known Postgres installs, newest first.
    pub fn installs(&self) -> Vec<PgInstall> {
        self.installs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Searches the machine for Postgres installs again (e.g. one was installed since start).
    async fn rediscover(&self) {
        let home = self.home.clone();
        if let Ok(found) = tokio::task::spawn_blocking(move || discover_with(Some(&home))).await {
            *self.installs.write().unwrap_or_else(|e| e.into_inner()) = found;
        }
    }

    /// The install of `major`; on a miss the machine is searched once more before giving up.
    pub async fn install_major(&self, major: u32) -> Result<PgInstall> {
        if let Ok(i) = find_major(&self.installs(), major) {
            return Ok(i.clone());
        }
        self.rediscover().await;
        find_major(&self.installs(), major).cloned()
    }

    /// The newest install; on a miss the machine is searched once more before giving up.
    pub async fn newest_install(&self) -> Result<PgInstall> {
        if let Ok(i) = newest(&self.installs()) {
            return Ok(i.clone());
        }
        self.rediscover().await;
        newest(&self.installs()).cloned()
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
