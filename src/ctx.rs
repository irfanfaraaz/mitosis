use crate::clone_backend::{same_volume, ApfsBackend, CloneBackend};
use crate::error::{ErrorCode, MitosisError, Result};
use crate::paths::Home;
use crate::pg::{discover, find_major, newest, PgInstall};
use crate::state::Store;
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
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

impl Config {
    /// The defaults, with `MITOSIS_MAX_CLONES` and `MITOSIS_MIN_FREE_DISK` applied when set.
    pub fn from_env() -> Result<Config> {
        Config::default().with_overrides(|k| std::env::var(k).ok())
    }

    fn with_overrides(mut self, var: impl Fn(&str) -> Option<String>) -> Result<Config> {
        let bad = |name: &str, value: &str, want: &str| {
            MitosisError::new(
                ErrorCode::InvalidRequest,
                format!("{name} must be {want}, got {value:?}"),
            )
            .with_hint(format!("fix or unset {name}, then run the command again"))
        };
        if let Some(v) = var("MITOSIS_MAX_CLONES") {
            self.max_clones = v
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|n| *n >= 1)
                .ok_or_else(|| bad("MITOSIS_MAX_CLONES", &v, "a whole number of at least 1"))?;
        }
        if let Some(v) = var("MITOSIS_MIN_FREE_DISK") {
            self.min_free_bytes = v
                .trim()
                .parse::<u64>()
                .map_err(|_| bad("MITOSIS_MIN_FREE_DISK", &v, "a whole number of bytes"))?;
        }
        Ok(self)
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
            installs: RwLock::new(discover()),
            started_at: Instant::now(),
            building: Mutex::new(HashSet::new()),
            in_flight: Mutex::new(HashMap::new()),
            watching: Mutex::new(HashSet::new()),
            template_swap: tokio::sync::RwLock::new(()),
            build_groups: Arc::new(Mutex::new(HashSet::new())),
        })
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
        if let Ok(found) = tokio::task::spawn_blocking(discover).await {
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
    fn env_overrides() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        let c = Config::default().with_overrides(env(&[])).unwrap();
        assert_eq!((c.max_clones, c.min_free_bytes), (8, 2_000_000_000));
        let c = Config::default()
            .with_overrides(env(&[
                ("MITOSIS_MAX_CLONES", "20"),
                ("MITOSIS_MIN_FREE_DISK", "500000000"),
            ]))
            .unwrap();
        assert_eq!((c.max_clones, c.min_free_bytes), (20, 500_000_000));
        for (k, v) in [
            ("MITOSIS_MAX_CLONES", "0"),
            ("MITOSIS_MAX_CLONES", "lots"),
            ("MITOSIS_MAX_CLONES", "-1"),
            ("MITOSIS_MIN_FREE_DISK", "2GB"),
            ("MITOSIS_MIN_FREE_DISK", ""),
        ] {
            let e = Config::default()
                .with_overrides(|n: &str| (n == k).then(|| v.to_string()))
                .unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidRequest);
            assert!(e.message.starts_with(k), "{e}");
        }
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
