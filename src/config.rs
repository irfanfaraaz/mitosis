use crate::error::{ErrorCode, MitosisError, Result};
use crate::names::parse_duration;
use crate::paths::Home;
use serde::Serialize;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr};
use std::ops::RangeInclusive;
use std::os::unix::fs::OpenOptionsExt;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub max_clones: usize,
    pub min_free_bytes: u64,
    /// Total memory the running clones may reserve; `None` means no limit.
    pub ram_budget: Option<u64>,
    /// Total disk the clones may use; `None` means no limit.
    pub disk_budget: Option<u64>,
    /// How long a create waits for room under a budget before it fails.
    pub queue_wait: Duration,
    /// A clone that grows past this many bytes gets a warning.
    pub clone_disk_warn: u64,
    pub port_range: RangeInclusive<u16>,
    pub default_max_age_secs: u64,
    pub ready_timeout: Duration,
    pub stop_timeout: Duration,
    /// How often the template scheduler looks for work; not user-settable, tests shorten it.
    pub scheduler_tick: Duration,
    /// Serve every clone on one port, picked by database name; off makes `url` the direct URL.
    pub router: bool,
    pub router_port: u16,
    /// The address the router binds; `0.0.0.0` makes it reachable from the network.
    pub router_listen: IpAddr,
    /// A clone with no connections for this long is put to sleep; `None` (off) never.
    pub idle_sleep: Option<Duration>,
    /// An unnamed, unprotected clone idle for this long is destroyed; `None` (off) never.
    pub idle_reap: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_clones: 8,
            min_free_bytes: 2_000_000_000,
            ram_budget: None,
            disk_budget: None,
            queue_wait: Duration::from_secs(60),
            clone_disk_warn: 5_000_000_000,
            port_range: 54320..=54999,
            default_max_age_secs: 604_800,
            ready_timeout: Duration::from_secs(10),
            stop_timeout: Duration::from_secs(10),
            scheduler_tick: Duration::from_secs(60),
            router: true,
            router_port: 54300,
            router_listen: IpAddr::V4(Ipv4Addr::LOCALHOST),
            idle_sleep: Some(Duration::from_secs(600)),
            idle_reap: None,
        }
    }
}

/// Where a setting's value came from. In JSON: `"source": "default" | "file" | "env"`, plus
/// `"env_var"` for `env`, so `source` is always a string.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "source", content = "env_var", rename_all = "snake_case")]
pub enum ConfigSource {
    Default,
    File,
    /// The name of the environment variable.
    Env(String),
}

impl ConfigSource {
    /// `default`, `config.toml` or `env MITOSIS_X`.
    pub fn label(&self) -> String {
        match self {
            ConfigSource::Default => "default".into(),
            ConfigSource::File => "config.toml".into(),
            ConfigSource::Env(v) => format!("env {v}"),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ConfigEntry {
    pub key: String,
    pub value: String,
    #[serde(flatten)]
    pub source: ConfigSource,
}

struct KeySpec {
    key: &'static str,
    env: &'static str,
    /// Completes "<where> must be ...".
    want: &'static str,
    example: &'static str,
}

const KEYS: [KeySpec; 11] = [
    KeySpec {
        key: "max_clones",
        env: "MITOSIS_MAX_CLONES",
        want: "a whole number of at least 1",
        example: "8",
    },
    KeySpec {
        key: "min_free_disk",
        env: "MITOSIS_MIN_FREE_DISK",
        want: "a size like 2GB, or a number of bytes",
        example: "2GB",
    },
    KeySpec {
        key: "ram_budget",
        env: "MITOSIS_RAM_BUDGET",
        want: "a size like 4GB (more than 0), or off",
        example: "4GB",
    },
    KeySpec {
        key: "disk_budget",
        env: "MITOSIS_DISK_BUDGET",
        want: "a size like 50GB (more than 0), or off",
        example: "50GB",
    },
    KeySpec {
        key: "queue_wait",
        env: "MITOSIS_QUEUE_WAIT",
        want: "a duration like 60s or 2m, or 0 for no wait",
        example: "60s",
    },
    KeySpec {
        key: "clone_disk_warn",
        env: "MITOSIS_CLONE_DISK_WARN",
        want: "a size like 5GB, or a number of bytes",
        example: "5GB",
    },
    KeySpec {
        key: "router",
        env: "MITOSIS_ROUTER",
        want: "on or off",
        example: "on",
    },
    KeySpec {
        key: "router_port",
        env: "MITOSIS_ROUTER_PORT",
        want: "a port number from 1 to 65535",
        example: "54300",
    },
    KeySpec {
        key: "router_listen",
        env: "MITOSIS_ROUTER_LISTEN",
        want: "an IP address like 127.0.0.1 or 0.0.0.0",
        example: "127.0.0.1",
    },
    KeySpec {
        key: "idle_sleep",
        env: "MITOSIS_IDLE_SLEEP",
        want: "a duration like 10m or 1h, or off",
        example: "10m",
    },
    KeySpec {
        key: "idle_reap",
        env: "MITOSIS_IDLE_REAP",
        want: "a duration like 2h or 1d, or off",
        example: "1d",
    },
];

fn invalid(msg: impl Into<String>) -> MitosisError {
    MitosisError::new(ErrorCode::InvalidRequest, msg)
}

/// Every setting as `(config key, environment variable)`, in `mitosis config` order.
pub fn keys() -> impl Iterator<Item = (&'static str, &'static str)> {
    KEYS.iter().map(|k| (k.key, k.env))
}

fn key_list() -> String {
    KEYS.iter().map(|k| k.key).collect::<Vec<_>>().join(", ")
}

fn spec(key: &str) -> Result<&'static KeySpec> {
    KEYS.iter().find(|k| k.key == key).ok_or_else(|| {
        invalid(format!("unknown setting {key:?}"))
            .with_hint(format!("settings are: {}", key_list()))
    })
}

/// Plain bytes, or a whole number with a unit: `KB|MB|GB|TB` (powers of 1000) or
/// `KiB|MiB|GiB|TiB` (powers of 1024), any case, no space between number and unit.
pub fn parse_size(s: &str) -> Result<u64> {
    let bad = || {
        invalid(format!("invalid size {s:?}"))
            .with_hint("use a whole number of bytes or a number plus a unit, like 500MB or 4GiB")
    };
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (digits, unit) = s.split_at(split);
    if digits.is_empty() {
        return Err(bad());
    }
    let mult: u64 = match unit.to_ascii_lowercase().as_str() {
        "" => 1,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "tb" => 1_000_000_000_000,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        "tib" => 1 << 40,
        _ => return Err(bad()),
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(mult))
        .ok_or_else(bad)
}

/// The largest decimal unit that divides `n` exactly, so `parse_size(format_size(n)) == n`.
fn format_size(n: u64) -> String {
    for (unit, name) in [
        (1_000_000_000_000, "TB"),
        (1_000_000_000, "GB"),
        (1_000_000, "MB"),
        (1_000, "KB"),
    ] {
        if n != 0 && n.is_multiple_of(unit) {
            return format!("{}{name}", n / unit);
        }
    }
    n.to_string()
}

/// One unit, the largest that divides the duration exactly (`90s`, not `1m30s`), so that
/// `parse_wait(format_setting_duration(d)) == d` and the shown value can be pasted into
/// `config set`. `0s` for zero.
fn format_setting_duration(d: Duration) -> String {
    let secs = d.as_secs();
    for (unit, size) in [('d', 86_400), ('h', 3600), ('m', 60)] {
        if secs != 0 && secs.is_multiple_of(size) {
            return format!("{}{unit}", secs / size);
        }
    }
    format!("{secs}s")
}

fn parse_budget(v: &str) -> Option<Option<u64>> {
    if v.eq_ignore_ascii_case("off") {
        Some(None)
    } else {
        // A zero budget would refuse every clone; `off` is the way to say no limit.
        parse_size(v).ok().filter(|n| *n > 0).map(Some)
    }
}

fn parse_switch(v: &str) -> Option<bool> {
    match v.to_ascii_lowercase().as_str() {
        "on" | "true" => Some(true),
        "off" | "false" => Some(false),
        _ => None,
    }
}

fn parse_idle(v: &str) -> Option<Option<Duration>> {
    if v.eq_ignore_ascii_case("off") {
        Some(None)
    } else {
        parse_duration(v).ok().map(Some)
    }
}

fn parse_wait(v: &str) -> Option<Duration> {
    crate::names::parse_wait(v).ok()
}

impl Config {
    /// The defaults, then `<home>/config.toml`, then the `MITOSIS_*` environment variables.
    pub fn load(home: &Home) -> Result<Config> {
        Config::default()
            .layered(home, &|k| std::env::var(k).ok())
            .map(|(c, _)| c)
    }

    /// Like `load`, but keeps the settings nobody can set from file or environment.
    pub fn reloaded(&self, home: &Home) -> Result<Config> {
        let base = Config {
            port_range: self.port_range.clone(),
            default_max_age_secs: self.default_max_age_secs,
            ready_timeout: self.ready_timeout,
            stop_timeout: self.stop_timeout,
            scheduler_tick: self.scheduler_tick,
            ..Config::default()
        };
        // The router binds once at start, so its settings change on a daemon restart only.
        base.layered(home, &|k| std::env::var(k).ok())
            .map(|(c, _)| Config {
                router: self.router,
                router_port: self.router_port,
                router_listen: self.router_listen,
                ..c
            })
    }

    /// One entry per setting with its current value and where it came from.
    pub fn describe(home: &Home) -> Result<Vec<ConfigEntry>> {
        let (cfg, sources) = Config::default().layered(home, &|k| std::env::var(k).ok())?;
        Ok(KEYS
            .iter()
            .zip(sources)
            .map(|(k, source)| ConfigEntry {
                key: k.key.into(),
                value: cfg.show(k.key),
                source,
            })
            .collect())
    }

    /// The environment variable that overrides `key` and the value in effect, if one is set.
    pub fn env_override(home: &Home, key: &str) -> Result<Option<(String, String)>> {
        Config::env_override_with(home, key, &|k| std::env::var(k).ok())
    }

    fn env_override_with(
        home: &Home,
        key: &str,
        var: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Option<(String, String)>> {
        let (cfg, sources) = Config::default().layered(home, var)?;
        Ok(KEYS
            .iter()
            .zip(sources)
            .find(|(k, _)| k.key == key)
            .and_then(|(k, source)| match source {
                ConfigSource::Env(v) => Some((v, cfg.show(k.key))),
                _ => None,
            }))
    }

    /// Validates `value`, then stores it in `<home>/config.toml`, keeping the other keys.
    pub fn set_in_file(home: &Home, key: &str, value: &str) -> Result<()> {
        let value = value.trim();
        let s = spec(key)?;
        Config::default()
            .set_field(key, value)
            .ok_or_else(|| bad_value(&format!("setting {key}"), s, value, false))?;
        home.ensure_dirs()?;
        let mut table = read_table(home)?;
        let stored = match key {
            "max_clones" | "router_port" => value.parse::<i64>().map(toml::Value::Integer).ok(),
            _ => None,
        };
        table.insert(
            key.into(),
            stored.unwrap_or_else(|| toml::Value::String(value.into())),
        );
        let text = toml::to_string(&table)
            .map_err(|e| MitosisError::new(ErrorCode::Internal, format!("encode config: {e}")))?;
        // A temp file and a rename, so a reader never sees a half-written file.
        // Named by pid, so two processes writing at once never share a half-written file.
        let tmp = home
            .root
            .join(format!("config.toml.tmp-{}", std::process::id()));
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(text.as_bytes())?;
        f.sync_all()?;
        if let Err(e) = std::fs::rename(&tmp, home.config_file()) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e.into());
        }
        Ok(())
    }

    /// Applies one setting from its text form; `None` when the value is not valid.
    fn set_field(&mut self, key: &str, v: &str) -> Option<()> {
        match key {
            "max_clones" => self.max_clones = v.parse::<usize>().ok().filter(|n| *n >= 1)?,
            "min_free_disk" => self.min_free_bytes = parse_size(v).ok()?,
            "ram_budget" => self.ram_budget = parse_budget(v)?,
            "disk_budget" => self.disk_budget = parse_budget(v)?,
            "queue_wait" => self.queue_wait = parse_wait(v)?,
            "clone_disk_warn" => self.clone_disk_warn = parse_size(v).ok()?,
            "router" => self.router = parse_switch(v)?,
            "router_port" => self.router_port = v.parse::<u16>().ok().filter(|p| *p >= 1)?,
            "router_listen" => self.router_listen = v.parse().ok()?,
            "idle_sleep" => self.idle_sleep = parse_idle(v)?,
            "idle_reap" => self.idle_reap = parse_idle(v)?,
            _ => return None,
        }
        Some(())
    }

    fn show(&self, key: &str) -> String {
        let budget = |b: Option<u64>| b.map_or("off".to_string(), format_size);
        match key {
            "max_clones" => self.max_clones.to_string(),
            "min_free_disk" => format_size(self.min_free_bytes),
            "ram_budget" => budget(self.ram_budget),
            "disk_budget" => budget(self.disk_budget),
            "queue_wait" => format_setting_duration(self.queue_wait),
            "clone_disk_warn" => format_size(self.clone_disk_warn),
            "router" => if self.router { "on" } else { "off" }.to_string(),
            "router_port" => self.router_port.to_string(),
            "router_listen" => self.router_listen.to_string(),
            "idle_sleep" => self
                .idle_sleep
                .map_or("off".into(), format_setting_duration),
            "idle_reap" => self.idle_reap.map_or("off".into(), format_setting_duration),
            _ => String::new(),
        }
    }

    /// `self`, then the file, then the environment; also says which layer set each key.
    fn layered(
        mut self,
        home: &Home,
        var: &dyn Fn(&str) -> Option<String>,
    ) -> Result<(Config, Vec<ConfigSource>)> {
        let mut sources = vec![ConfigSource::Default; KEYS.len()];
        for (key, value) in &read_table(home)? {
            let Some(i) = KEYS.iter().position(|k| k.key == key) else {
                return Err(invalid(format!("config.toml has an unknown key {key:?}"))
                    .with_hint(format!("settings are: {}", key_list())));
            };
            let origin = format!("config.toml key {key}");
            let text = match value {
                toml::Value::Integer(n) => n.to_string(),
                toml::Value::String(s) => s.clone(),
                toml::Value::Boolean(b) => b.to_string(),
                other => {
                    return Err(invalid(format!(
                        "{origin} must be {}, got a {}",
                        KEYS[i].want,
                        other.type_str()
                    ))
                    .with_hint(file_hint(&KEYS[i])))
                }
            };
            self.set_field(key, text.trim())
                .ok_or_else(|| bad_value(&origin, &KEYS[i], &text, true))?;
            sources[i] = ConfigSource::File;
        }
        for (i, k) in KEYS.iter().enumerate() {
            if let Some(v) = var(k.env) {
                self.set_field(k.key, v.trim())
                    .ok_or_else(|| bad_value(k.env, k, &v, false))?;
                sources[i] = ConfigSource::Env(k.env.into());
            }
        }
        Ok((self, sources))
    }
}

fn file_hint(s: &KeySpec) -> String {
    format!("fix it with: mitosis config set {} {}", s.key, s.example)
}

fn bad_value(origin: &str, s: &KeySpec, value: &str, from_file: bool) -> MitosisError {
    let hint = if from_file {
        file_hint(s)
    } else if origin == s.env {
        format!("fix or unset {origin}, e.g. {}={}", s.env, s.example)
    } else {
        format!("for example: mitosis config set {} {}", s.key, s.example)
    };
    invalid(format!("{origin} must be {}, got {value:?}", s.want)).with_hint(hint)
}

/// The parsed file; empty when there is none.
fn read_table(home: &Home) -> Result<toml::Table> {
    let path = home.config_file();
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(toml::Table::new()),
        Err(e) => {
            return Err(MitosisError::new(
                ErrorCode::Internal,
                format!("read {}: {e}", path.display()),
            ))
        }
    };
    toml::from_str(&text).map_err(|e| {
        invalid(format!("{} is not valid TOML: {e}", path.display()))
            .with_hint("fix the file, or delete it to go back to the defaults")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn home() -> (tempfile::TempDir, Home) {
        let tmp = tempfile::tempdir().unwrap();
        let home = Home::at(tmp.path().to_path_buf());
        (tmp, home)
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn entry(
        home: &Home,
        env: &dyn Fn(&str) -> Option<String>,
        key: &str,
    ) -> (String, ConfigSource) {
        let (cfg, sources) = Config::default().layered(home, env).unwrap();
        let i = KEYS.iter().position(|k| k.key == key).unwrap();
        (cfg.show(key), sources[i].clone())
    }

    #[test]
    fn defaults() {
        let c = Config::default();
        assert_eq!(c.max_clones, 8);
        assert_eq!(c.min_free_bytes, 2_000_000_000);
        assert_eq!((c.ram_budget, c.disk_budget), (None, None));
        assert_eq!(c.queue_wait, Duration::from_secs(60));
        assert_eq!(c.clone_disk_warn, 5_000_000_000);
        assert_eq!(c.port_range, 54320..=54999);
        assert_eq!(c.default_max_age_secs, 604_800);
        assert_eq!(c.ready_timeout, Duration::from_secs(10));
        assert_eq!(c.stop_timeout, Duration::from_secs(10));
        assert_eq!(c.scheduler_tick, Duration::from_secs(60));
        assert!(c.router);
        assert_eq!(c.router_port, 54300);
        assert_eq!(c.router_listen.to_string(), "127.0.0.1");
        assert_eq!(c.idle_sleep, Some(Duration::from_secs(600)));
        assert_eq!(c.idle_reap, None);
    }

    #[test]
    fn idle_keys() {
        let (_tmp, home) = home();
        std::fs::write(
            home.config_file(),
            "idle_sleep = \"off\"\nidle_reap = \"2h\"\n",
        )
        .unwrap();
        let (c, _) = Config::default().layered(&home, &no_env).unwrap();
        assert_eq!(
            (c.idle_sleep, c.idle_reap),
            (None, Some(Duration::from_secs(7200)))
        );
        assert_eq!(entry(&home, &no_env, "idle_sleep").0, "off");
        assert_eq!(entry(&home, &no_env, "idle_reap").0, "2h");
        let env = |k: &str| (k == "MITOSIS_IDLE_SLEEP").then(|| "0s".to_string());
        assert!(Config::default().layered(&home, &env).is_err());
        assert!(Config::set_in_file(&home, "idle_sleep", "soon").is_err());
        Config::set_in_file(&home, "idle_sleep", "5m").unwrap();
        let (c, _) = Config::default().layered(&home, &no_env).unwrap();
        assert_eq!(c.idle_sleep, Some(Duration::from_secs(300)));
    }

    #[test]
    fn router_keys() {
        let (_tmp, home) = home();
        std::fs::write(
            home.config_file(),
            "router = false\nrouter_port = 6000\nrouter_listen = \"0.0.0.0\"\n",
        )
        .unwrap();
        let (c, _) = Config::default().layered(&home, &no_env).unwrap();
        assert_eq!((c.router, c.router_port), (false, 6000));
        assert_eq!(c.router_listen.to_string(), "0.0.0.0");
        let env = |k: &str| (k == "MITOSIS_ROUTER").then(|| "on".to_string());
        assert_eq!(entry(&home, &env, "router").0, "on");
        for (k, v) in [
            ("MITOSIS_ROUTER", "maybe"),
            ("MITOSIS_ROUTER_PORT", "0"),
            ("MITOSIS_ROUTER_PORT", "70000"),
            ("MITOSIS_ROUTER_LISTEN", "localhost"),
        ] {
            let env = |n: &str| (n == k).then(|| v.to_string());
            let e = Config::default().layered(&home, &env).unwrap_err();
            assert!(e.message.starts_with(k), "{e}");
        }
        Config::set_in_file(&home, "router_port", "6001").unwrap();
        Config::set_in_file(&home, "router", "off").unwrap();
        let (c, _) = Config::default().layered(&home, &no_env).unwrap();
        assert_eq!((c.router, c.router_port), (false, 6001));
    }

    #[test]
    fn parse_size_units() {
        assert_eq!(parse_size("500MB").unwrap(), 500_000_000);
        assert_eq!(parse_size("4GiB").unwrap(), 4 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("123").unwrap(), 123);
        assert_eq!(parse_size("2gb").unwrap(), 2_000_000_000);
        assert_eq!(parse_size("1TiB").unwrap(), 1 << 40);
        for bad in [
            "4 gb",
            "-1",
            "GB",
            "",
            "1.5GB",
            "5XB",
            "99999999999999999999",
        ] {
            assert!(parse_size(bad).is_err(), "{bad:?}");
        }
        assert_eq!(
            parse_size(&format_size(2_000_000_000)).unwrap(),
            2_000_000_000
        );
    }

    #[test]
    fn config_file_then_env_precedence() {
        let (_tmp, home) = home();
        std::fs::write(home.config_file(), "max_clones = 3\n").unwrap();
        let env = |k: &str| (k == "MITOSIS_MAX_CLONES").then(|| "5".to_string());
        assert_eq!(
            entry(&home, &env, "max_clones"),
            ("5".into(), ConfigSource::Env("MITOSIS_MAX_CLONES".into()))
        );
        assert_eq!(
            entry(&home, &no_env, "max_clones"),
            ("3".into(), ConfigSource::File)
        );
        assert_eq!(
            entry(&home, &no_env, "ram_budget"),
            ("off".into(), ConfigSource::Default)
        );
    }

    #[test]
    fn config_file_errors_name_the_key() {
        let (_tmp, home) = home();
        let err = |text: &str| {
            std::fs::write(home.config_file(), text).unwrap();
            Config::default().layered(&home, &no_env).unwrap_err()
        };
        let e = err("ram_budget = \"lots\"\n");
        assert_eq!(e.code, ErrorCode::InvalidRequest);
        assert!(
            e.message.contains("ram_budget") && e.message.contains("config.toml"),
            "{e}"
        );
        assert!(e.hint.as_deref().unwrap().contains("4GB"), "{e}");
        let e = err("colour = 1\n");
        assert!(
            e.message.contains("colour") && e.message.contains("config.toml"),
            "{e}"
        );
        let e = err("max_clones = [\n");
        assert!(e.message.contains("config.toml"), "{e}");
        let e = err("max_clones = true\n");
        assert!(e.message.contains("max_clones"), "{e}");
        let e = err("max_clones = 0\n");
        assert!(e.message.contains("max_clones"), "{e}");
    }

    #[test]
    fn env_errors_name_the_variable() {
        let (_tmp, home) = home();
        for (k, v) in [
            ("MITOSIS_MAX_CLONES", "0"),
            ("MITOSIS_MAX_CLONES", "lots"),
            ("MITOSIS_MIN_FREE_DISK", ""),
            ("MITOSIS_RAM_BUDGET", "4 GB"),
            ("MITOSIS_QUEUE_WAIT", "soon"),
        ] {
            let env = |n: &str| (n == k).then(|| v.to_string());
            let e = Config::default().layered(&home, &env).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidRequest);
            assert!(e.message.starts_with(k), "{e}");
        }
    }

    #[test]
    fn budgets_and_wait() {
        let (_tmp, home) = home();
        std::fs::write(
            home.config_file(),
            "ram_budget = \"2GB\"\ndisk_budget = \"off\"\nqueue_wait = \"0s\"\nmin_free_disk = 500000000\n",
        )
        .unwrap();
        let (c, _) = Config::default().layered(&home, &no_env).unwrap();
        assert_eq!(c.ram_budget, Some(2_000_000_000));
        assert_eq!(c.disk_budget, None);
        assert_eq!(c.queue_wait, Duration::ZERO);
        assert_eq!(c.min_free_bytes, 500_000_000);
        let env = |k: &str| (k == "MITOSIS_QUEUE_WAIT").then(|| "2m".to_string());
        let (c, _) = Config::default().layered(&home, &env).unwrap();
        assert_eq!(c.queue_wait, Duration::from_secs(120));
    }

    #[test]
    fn zero_budgets_are_rejected() {
        let (_tmp, home) = home();
        for key in ["ram_budget", "disk_budget"] {
            std::fs::write(home.config_file(), format!("{key} = \"0\"\n")).unwrap();
            let e = Config::default().layered(&home, &no_env).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidRequest);
            assert!(e.message.contains(key), "{}", e.message);
            assert!(Config::set_in_file(&home, key, "0").is_err());
            let env =
                |k: &str| (k == format!("MITOSIS_{}", key.to_uppercase())).then(|| "0".to_string());
            std::fs::remove_file(home.config_file()).unwrap();
            assert!(Config::default().layered(&home, &env).is_err());
        }
    }

    #[test]
    fn set_in_file_keeps_other_keys() {
        let (_tmp, home) = home();
        Config::set_in_file(&home, "max_clones", "4").unwrap();
        Config::set_in_file(&home, "ram_budget", "2GB").unwrap();
        let table: toml::Table =
            toml::from_str(&std::fs::read_to_string(home.config_file()).unwrap()).unwrap();
        assert_eq!(table["max_clones"].as_integer(), Some(4));
        assert_eq!(table["ram_budget"].as_str(), Some("2GB"));
        let (c, _) = Config::default().layered(&home, &no_env).unwrap();
        assert_eq!((c.max_clones, c.ram_budget), (4, Some(2_000_000_000)));
        // Invalid values and unknown keys leave the file alone.
        assert!(Config::set_in_file(&home, "ram_budget", "lots").is_err());
        assert!(Config::set_in_file(&home, "colour", "red").is_err());
        let again: toml::Table =
            toml::from_str(&std::fs::read_to_string(home.config_file()).unwrap()).unwrap();
        assert_eq!(table, again);
    }

    #[test]
    fn reload_keeps_fixed_settings() {
        let (_tmp, home) = home();
        let base = Config {
            port_range: 60000..=60019,
            scheduler_tick: Duration::from_millis(50),
            max_clones: 1,
            ..Config::default()
        };
        // Env may be set in the developer's shell; only the fixed settings are asserted on.
        if let Ok(c) = base.reloaded(&home) {
            assert_eq!(c.port_range, 60000..=60019);
            assert_eq!(c.scheduler_tick, Duration::from_millis(50));
        }
    }

    #[test]
    fn shown_durations_parse_back() {
        for secs in [0, 1, 45, 60, 90, 3600, 5400, 7200, 86_400, 90_000] {
            let d = Duration::from_secs(secs);
            let shown = format_setting_duration(d);
            assert_eq!(parse_wait(&shown), Some(d), "{secs}s shown as {shown}");
        }
        assert_eq!(format_setting_duration(Duration::from_secs(90)), "90s");
        assert_eq!(format_setting_duration(Duration::from_secs(7200)), "2h");
    }

    #[test]
    fn set_in_file_leaves_no_temp_file() {
        let (_tmp, home) = home();
        Config::set_in_file(&home, "max_clones", "4").unwrap();
        Config::set_in_file(&home, "router", "off").unwrap();
        let names: Vec<String> = std::fs::read_dir(&home.root)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().all(|n| !n.contains(".tmp")), "{names:?}");
        assert!(names.contains(&"config.toml".to_string()));
    }

    #[test]
    fn env_override_names_the_variable_in_effect() {
        let (_tmp, home) = home();
        Config::set_in_file(&home, "max_clones", "4").unwrap();
        assert_eq!(
            Config::env_override_with(&home, "max_clones", &no_env).unwrap(),
            None
        );
        let env = |k: &str| (k == "MITOSIS_MAX_CLONES").then(|| "9".to_string());
        assert_eq!(
            Config::env_override_with(&home, "max_clones", &env).unwrap(),
            Some(("MITOSIS_MAX_CLONES".to_string(), "9".to_string()))
        );
        assert_eq!(
            Config::env_override_with(&home, "router", &env).unwrap(),
            None
        );
    }
}
