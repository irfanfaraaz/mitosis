use crate::client;
use crate::config::{Config, ConfigEntry};
use crate::daemon;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::mcp;
use crate::names::{format_duration, parse_duration, validate_name};
use crate::paths::Home;
use crate::protocol::{
    CloneCreateArgs, CloneInfo, MigrateCmd, Request, Response, TemplateInfo, TemplateInitArgs,
    TemplateSetArgs, TemplateSettings, TemplateSource,
};
use crate::templates::now;
use clap::{Parser, Subcommand};
use std::process::ExitCode;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(
    name = "mitosis",
    version,
    about = "Copy-on-write Postgres clones for AI agent tasks"
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Build, refresh and list templates.
    Template {
        #[command(subcommand)]
        command: TemplateCmd,
    },
    /// Create a clone of a template and print its URL.
    Clone {
        #[arg(long, default_value = "main")]
        template: String,
        #[arg(long)]
        name: Option<String>,
        /// Time to live, like 30m, 2h or 1d.
        #[arg(long, default_value = "2h")]
        ttl: String,
        /// Keep fsync on (slower, survives a crash).
        #[arg(long)]
        durable: bool,
        #[arg(long)]
        json: bool,
    },
    /// List clones.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Destroy a clone by id or name.
    Destroy { id_or_name: String },
    /// Make a running clone the newest generation of its template (or of --template).
    Promote {
        clone: String,
        /// Promote into this template instead of the clone's own; created when it does not exist.
        #[arg(long)]
        template: Option<String>,
    },
    /// Remove expired clones and leftovers.
    Gc,
    /// Run the daemon in the foreground, or manage it.
    Daemon {
        #[command(subcommand)]
        command: Option<DaemonCmd>,
    },
    /// Serve the MCP tools over stdio.
    Mcp,
    /// Show the settings and where each value comes from, or change one.
    Config {
        #[command(subcommand)]
        command: Option<ConfigCmd>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ConfigCmd {
    /// Write a setting to config.toml and tell a running daemon to reload.
    Set {
        /// max_clones, min_free_disk, ram_budget, disk_budget, queue_wait or clone_disk_warn.
        key: String,
        /// A number, a size like 4GB, a duration like 60s, or off.
        value: String,
    },
}

#[derive(Subcommand)]
enum TemplateCmd {
    /// Build a template from a database URL or a seed command.
    Init {
        #[arg(default_value = "main")]
        name: String,
        /// Copy an existing database with pg_dump.
        #[arg(long, conflicts_with = "seed", required_unless_present = "seed")]
        from_url: Option<String>,
        /// Shell command run in this folder with DATABASE_URL set.
        #[arg(long)]
        seed: Option<String>,
        #[arg(long)]
        database: Option<String>,
        /// Migrations folder; the template is flagged stale when it changes.
        #[arg(long)]
        migrations: Option<std::path::PathBuf>,
        #[arg(long)]
        pg_version: Option<u32>,
        #[command(flatten)]
        settings: SettingsFlags,
    },
    /// Rebuild a template from its recorded source.
    Refresh {
        #[arg(default_value = "main")]
        name: String,
        /// Copy the current generation and run only the migrate command on it.
        #[arg(long)]
        layered: bool,
    },
    /// Change a template's settings.
    Set {
        #[arg(default_value = "main")]
        name: String,
        #[command(flatten)]
        settings: SettingsFlags,
        /// Restart automatic builds stopped by a rollback.
        #[arg(long)]
        resume: bool,
    },
    /// List templates.
    List {
        #[arg(long)]
        json: bool,
    },
    /// List a template's generations; `*` marks the current one.
    History {
        #[arg(default_value = "main")]
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Point a template back at an older generation and pause its scheduled refreshes.
    Rollback {
        #[arg(default_value = "main")]
        name: String,
        /// Generation number; the newest one older than the current by default.
        #[arg(long)]
        to: Option<u32>,
    },
}

/// Template settings shared by `template init` and `template set`.
#[derive(clap::Args)]
struct SettingsFlags {
    /// Age after which clones warn, like 7d.
    #[arg(long)]
    max_age: Option<String>,
    /// Generations to keep on disk (at least 1).
    #[arg(long)]
    keep: Option<u32>,
    /// Rebuild on a schedule, like 24h (at least 1m), or off.
    #[arg(long)]
    every: Option<String>,
    /// How scheduled rebuilds run.
    #[arg(long, value_parser = ["full", "layered"])]
    mode: Option<String>,
    /// Migration command for layered refreshes, run in this folder with DATABASE_URL set, or off.
    #[arg(long)]
    migrate: Option<String>,
    /// Run a layered refresh by itself when the migrations folder changes.
    #[arg(long, value_parser = ["on", "off"])]
    auto_migrate: Option<String>,
    /// When a clone's template is older than --max-age: warn, or also refresh in the background.
    #[arg(long, value_parser = ["warn", "refresh"])]
    on_stale: Option<String>,
}

/// A protocol enum from its snake_case name (clap already limited the choices).
fn enum_arg<T: serde::de::DeserializeOwned>(s: Option<String>) -> Result<Option<T>> {
    Ok(
        s.map(|s| serde_json::from_value(serde_json::Value::String(s)))
            .transpose()?,
    )
}

impl SettingsFlags {
    /// The max age in seconds, and the settings.
    fn parse(self) -> Result<(Option<u64>, TemplateSettings)> {
        let max_age_secs = self
            .max_age
            .as_deref()
            .map(parse_duration)
            .transpose()?
            .map(|d| d.as_secs());
        let (every_secs, every_off) = match self.every.as_deref() {
            Some("off") => (None, true),
            Some(d) => (Some(parse_duration(d)?.as_secs()), false),
            None => (None, false),
        };
        let (migrate, migrate_off) = match self.migrate {
            Some(c) if c == "off" => (None, true),
            // Runs where and with the PATH it would in this shell, like a seed.
            Some(command) => (
                Some(MigrateCmd {
                    command,
                    cwd: std::env::current_dir()?,
                    path: std::env::var("PATH").ok(),
                }),
                false,
            ),
            None => (None, false),
        };
        let settings = TemplateSettings {
            keep: self.keep,
            every_secs,
            every_off,
            every_mode: enum_arg(self.mode)?,
            migrate,
            migrate_off,
            auto_migrate: self.auto_migrate.map(|a| a == "on"),
            on_stale: enum_arg(self.on_stale)?,
        };
        Ok((max_age_secs, settings))
    }
}

#[derive(Subcommand)]
enum DaemonCmd {
    /// Show whether the daemon is running.
    Status,
    /// Stop the daemon; clones keep running.
    Stop,
}

pub fn main() -> ExitCode {
    let cli = Cli::parse();
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail(&MitosisError::from(e)),
    };
    match rt.block_on(run(cli)) {
        Ok(code) => code,
        Err(e) => fail(&e),
    }
}

fn fail(e: &MitosisError) -> ExitCode {
    eprintln!("error: {}", e.message);
    if let Some(hint) = &e.hint {
        eprintln!("hint: {hint}");
    }
    // Only `mitosis daemon` returns AlreadyRunning; `client::ensure_daemon` looks for this status.
    if e.code == ErrorCode::AlreadyRunning {
        return ExitCode::from(client::EXIT_ALREADY_RUNNING);
    }
    ExitCode::FAILURE
}

// Deliberately not `{r:?}`: a reply can hold a password-bearing URL. Same in `unexpected` in mcp.rs.
fn unexpected(_: Response) -> MitosisError {
    MitosisError::new(ErrorCode::ProtocolMismatch, "unexpected daemon reply")
        .with_hint("restart the daemon: mitosis daemon stop")
}

fn print_json<T: serde::Serialize>(v: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

async fn run(cli: Cli) -> Result<ExitCode> {
    let home = Home::from_env()?;
    match cli.command {
        Cmd::Daemon { command: None } => {
            let config = Config::load(&home)?;
            daemon::run(home, config).await?
        }
        Cmd::Mcp => mcp::serve_stdio(home).await?,
        Cmd::Config {
            command: None,
            json,
        } => {
            let entries = Config::describe(&home)?;
            if json {
                print_json(&entries)?;
            } else {
                print_config(&entries);
            }
        }
        Cmd::Config {
            command: Some(ConfigCmd::Set { key, value }),
            ..
        } => {
            Config::set_in_file(&home, &key, &value)?;
            // No daemon start: a stopped daemon reads the file when it next starts.
            client::request_if_running(&home, Request::ConfigReload).await?;
            println!("{key} = {}", value.trim());
        }
        Cmd::Template { command } => template(&home, command).await?,
        Cmd::Clone {
            template,
            name,
            ttl,
            durable,
            json,
        } => {
            validate_name("template", &template)?;
            if let Some(n) = &name {
                validate_name("clone", n)?;
            }
            let ttl_secs = parse_duration(&ttl)?.as_secs();
            let args = CloneCreateArgs {
                template: template.clone(),
                name,
                ttl_secs,
                durable,
                // The CLI exits right after printing the URL, so watching it would destroy the
                // clone at once: CLI clones live until their TTL or `mitosis destroy`.
                parent_pid: None,
            };
            let info = match client::request(&home, Request::CloneCreate(args)).await? {
                Response::CloneCreated(info) => info,
                other => return Err(unexpected(other)),
            };
            // Warnings are best effort: the clone already exists.
            if let Ok(Response::Templates { templates }) =
                client::request(&home, Request::TemplateList).await
            {
                for w in templates
                    .iter()
                    .filter(|t| t.name == template)
                    .flat_map(|t| &t.warnings)
                {
                    eprintln!("warning: {w}");
                }
            }
            if json {
                print_json(&info)?;
            } else {
                println!("{}", info.url);
            }
        }
        Cmd::List { json } => {
            let clones = match client::request(&home, Request::CloneList).await? {
                Response::Clones { clones } => clones,
                other => return Err(unexpected(other)),
            };
            if json {
                print_json(&clones)?;
            } else {
                print_clones(&clones);
            }
        }
        Cmd::Destroy { id_or_name } => {
            match client::request(&home, Request::CloneDestroy { id_or_name }).await? {
                Response::Destroyed { id } => println!("{id}"),
                other => return Err(unexpected(other)),
            }
        }
        Cmd::Promote { clone, template } => {
            let req = Request::ClonePromote {
                id_or_name: clone.clone(),
                template,
            };
            match client::request(&home, req).await? {
                Response::TemplateBuilt(t) => println!(
                    "promoted {clone} to template {} generation {} (Postgres {}, {})",
                    t.name,
                    t.current_gen,
                    t.pg_major,
                    human_bytes(t.size_bytes)
                ),
                other => return Err(unexpected(other)),
            }
        }
        Cmd::Gc => match client::request(&home, Request::Gc).await? {
            Response::Gc(r) => {
                println!(
                    "adopted {}, destroyed {}, orphan folders removed {}, records dropped {}, failed {}",
                    r.adopted.len(),
                    r.destroyed.len(),
                    r.orphan_folders_removed.len(),
                    r.records_dropped.len(),
                    r.failed.len()
                );
                for f in &r.failed {
                    println!("failed: {}: {}", f.item, f.error);
                }
            }
            other => return Err(unexpected(other)),
        },
        Cmd::Daemon {
            command: Some(DaemonCmd::Status),
        } => match client::request_if_running(&home, Request::Status).await? {
            None => {
                println!("daemon is not running");
                return Ok(ExitCode::FAILURE);
            }
            Some(Response::Status(s)) => {
                println!("daemon is running");
                println!("pid       {}", s.pid);
                println!("home      {}", s.home.display());
                println!("clones    {}", s.clones);
                println!("templates {}", s.templates);
                println!(
                    "uptime    {}",
                    format_duration(Duration::from_secs(s.uptime_secs))
                );
            }
            Some(other) => return Err(unexpected(other)),
        },
        Cmd::Daemon {
            command: Some(DaemonCmd::Stop),
        } => {
            match client::request_if_running(&home, Request::Shutdown).await? {
                None => println!("daemon is not running"),
                Some(Response::Ok) => {
                    // The daemon removes its socket right after replying; wait so callers see it gone.
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while home.daemon_sock().exists() && Instant::now() < deadline {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    println!("daemon stopped");
                }
                Some(other) => return Err(unexpected(other)),
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn print_config(entries: &[ConfigEntry]) {
    let width = entries.iter().map(|e| e.key.len()).max().unwrap_or(0);
    let vwidth = entries.iter().map(|e| e.value.len()).max().unwrap_or(0);
    for e in entries {
        println!(
            "{:<width$} = {:<vwidth$}  ({})",
            e.key,
            e.value,
            e.source.label()
        );
    }
}

async fn template(home: &Home, cmd: TemplateCmd) -> Result<()> {
    match cmd {
        TemplateCmd::Init {
            name,
            from_url,
            seed,
            database,
            migrations,
            pg_version,
            settings,
        } => {
            validate_name("template", &name)?;
            let (max_age_secs, settings) = settings.parse()?;
            let migrations = migrations
                .map(|dir| {
                    std::fs::canonicalize(&dir).map_err(|e| {
                        MitosisError::new(
                            ErrorCode::InvalidRequest,
                            format!("migrations folder {}: {e}", dir.display()),
                        )
                    })
                })
                .transpose()?;
            let source = match (from_url, seed) {
                (Some(url), _) => TemplateSource::Url { url },
                // The seed runs where and with the PATH it would in this shell.
                (None, Some(command)) => TemplateSource::Seed {
                    command,
                    cwd: Some(std::env::current_dir()?),
                    path: std::env::var("PATH").ok(),
                },
                (None, None) => unreachable!("clap requires --from-url or --seed"),
            };
            let args = TemplateInitArgs {
                name,
                source,
                database,
                migrations,
                max_age_secs,
                pg_major: pg_version,
                settings,
            };
            match client::request(home, Request::TemplateInit(args)).await? {
                Response::TemplateBuilt(t) => print_built(&t),
                other => return Err(unexpected(other)),
            }
        }
        TemplateCmd::Refresh { name, layered } => {
            validate_name("template", &name)?;
            let path = std::env::var("PATH").ok();
            let req = Request::TemplateRefresh {
                name,
                path,
                layered,
            };
            match client::request(home, req).await? {
                Response::TemplateBuilt(t) => print_built(&t),
                other => return Err(unexpected(other)),
            }
        }
        TemplateCmd::Set {
            name,
            settings,
            resume,
        } => {
            validate_name("template", &name)?;
            let (max_age_secs, settings) = settings.parse()?;
            let args = TemplateSetArgs {
                name,
                max_age_secs,
                settings,
                resume,
            };
            match client::request(home, Request::TemplateSet(args)).await? {
                Response::TemplateBuilt(t) => {
                    println!("template {} settings saved", t.name);
                    if t.paused {
                        println!("{}", paused_line(&t.name));
                    }
                }
                other => return Err(unexpected(other)),
            }
        }
        TemplateCmd::List { json } => {
            let templates = match client::request(home, Request::TemplateList).await? {
                Response::Templates { templates } => templates,
                other => return Err(unexpected(other)),
            };
            for t in &templates {
                for w in &t.warnings {
                    eprintln!("warning: {}: {w}", t.name);
                }
                if let Some(e) = &t.last_error {
                    eprintln!("last error: {}: {e}", t.name);
                }
            }
            if json {
                print_json(&templates)?;
            } else {
                print_table(
                    &[
                        "NAME", "GEN", "PAUSED", "PG", "DATABASE", "SIZE", "AGE", "SOURCE",
                    ],
                    templates
                        .iter()
                        .map(|t| {
                            vec![
                                t.name.clone(),
                                format!("{}/{}", t.current_gen, t.generations),
                                if t.paused { "paused" } else { "-" }.to_string(),
                                t.pg_major.to_string(),
                                t.database.clone(),
                                human_bytes(t.size_bytes),
                                format_duration(Duration::from_secs(t.age_secs)),
                                source_cell(&t.source),
                            ]
                        })
                        .collect(),
                );
            }
        }
        TemplateCmd::History { name, json } => {
            validate_name("template", &name)?;
            let (template, generations, paused) =
                match client::request(home, Request::TemplateHistory { name }).await? {
                    Response::Generations {
                        template,
                        generations,
                        paused,
                    } => (template, generations, paused),
                    other => return Err(unexpected(other)),
                };
            if json {
                print_json(&serde_json::json!({
                    "template": template,
                    "paused": paused,
                    "generations": generations,
                }))?;
            } else {
                if paused {
                    println!("{}", paused_line(&template));
                }
                let now = now();
                print_table(
                    &["GEN", "KIND", "BUILT", "PG", "SIZE", "CURRENT"],
                    generations
                        .iter()
                        .map(|g| {
                            let ago = u64::try_from(now - g.built_at).unwrap_or(0);
                            vec![
                                g.gen.to_string(),
                                serde_json::to_value(g.kind)
                                    .ok()
                                    .and_then(|v| v.as_str().map(String::from))
                                    .unwrap_or_default(),
                                format!("{} ago", format_duration(Duration::from_secs(ago))),
                                g.pg_major.to_string(),
                                human_bytes(g.size_bytes),
                                if g.current { "*" } else { "" }.to_string(),
                            ]
                        })
                        .collect(),
                );
            }
        }
        TemplateCmd::Rollback { name, to } => {
            validate_name("template", &name)?;
            match client::request(home, Request::TemplateRollback { name, to }).await? {
                Response::TemplateBuilt(t) => println!(
                    "template {} now uses generation {} (scheduled refreshes paused)",
                    t.name, t.current_gen
                ),
                other => return Err(unexpected(other)),
            }
        }
    }
    Ok(())
}

/// The template's source for the table: the URL (the daemon already masked its password) or
/// the seed command, cut to one short line.
fn source_cell(source: &TemplateSource) -> String {
    const MAX: usize = 48;
    let text = match source {
        TemplateSource::Url { url } => url.clone(),
        TemplateSource::Seed { command, .. } => {
            format!(
                "seed: {}",
                command.split_whitespace().collect::<Vec<_>>().join(" ")
            )
        }
        TemplateSource::Promoted { clone_id } => format!("promoted from {clone_id}"),
    };
    if text.chars().count() <= MAX {
        return text;
    }
    let cut: String = text.chars().take(MAX - 1).collect();
    format!("{cut}…")
}

fn paused_line(name: &str) -> String {
    format!(
        "paused: automatic builds stopped — run \"mitosis template refresh {name}\" or \"mitosis template set {name} --resume\""
    )
}

fn print_built(t: &TemplateInfo) {
    println!(
        "template {} built (Postgres {}, {})",
        t.name,
        t.pg_major,
        human_bytes(t.size_bytes)
    );
}

fn print_clones(clones: &[CloneInfo]) {
    let now = now();
    let secs = |s: i64| format_duration(Duration::from_secs(u64::try_from(s).unwrap_or(0)));
    let opt_bytes = |b: Option<u64>| b.map_or("-".to_string(), human_bytes);
    print_table(
        &[
            "ID",
            "NAME",
            "TEMPLATE",
            "PORT",
            "AGE",
            "TTL LEFT",
            "RAM",
            "EXTRA DISK",
        ],
        clones
            .iter()
            .map(|c| {
                vec![
                    c.id.clone(),
                    c.name.clone().unwrap_or_else(|| "-".into()),
                    c.template.clone(),
                    c.port.to_string(),
                    secs(now - c.created_at),
                    secs(c.expires_at - now),
                    opt_bytes(c.rss_bytes),
                    opt_bytes(c.own_disk_bytes),
                ]
            })
            .collect(),
    );
}

/// Left-aligned columns separated by two spaces.
fn print_table(headers: &[&str], rows: Vec<Vec<String>>) {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in &rows {
        for (w, cell) in widths.iter_mut().zip(row) {
            *w = (*w).max(cell.chars().count());
        }
    }
    let line = |cells: Vec<&str>| {
        let parts: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(c, w)| format!("{c:<w$}"))
            .collect();
        println!("{}", parts.join("  ").trim_end());
    };
    line(headers.to_vec());
    for row in &rows {
        line(row.iter().map(String::as_str).collect());
    }
}

/// Decimal units: 512 B, 3.4 kB, 34 MB, 1.2 GB.
fn human_bytes(b: u64) -> String {
    if b < 1000 {
        return format!("{b} B");
    }
    let mut v = b as f64;
    let mut unit = "B";
    for u in ["kB", "MB", "GB", "TB", "PB"] {
        if v < 1000.0 {
            break;
        }
        v /= 1000.0;
        unit = u;
    }
    if v < 10.0 {
        format!("{v:.1} {unit}")
    } else {
        format!("{v:.0} {unit}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn clap_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn sizes() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(999), "999 B");
        assert_eq!(human_bytes(3_400), "3.4 kB");
        assert_eq!(human_bytes(34_000_000), "34 MB");
        assert_eq!(human_bytes(1_200_000_000), "1.2 GB");
    }

    #[test]
    fn source_cells_are_short() {
        let url = TemplateSource::Url {
            url: "postgres://u:***@h/db".into(),
        };
        assert_eq!(source_cell(&url), "postgres://u:***@h/db");
        let seed = TemplateSource::Seed {
            command: format!("psql \"$DATABASE_URL\"\n  -f {}", "x".repeat(80)),
            cwd: None,
            path: None,
        };
        let cell = source_cell(&seed);
        assert!(
            cell.starts_with("seed: psql \"$DATABASE_URL\" -f xx"),
            "{cell}"
        );
        assert_eq!(cell.chars().count(), 48);
        assert!(cell.ends_with('…') && !cell.contains('\n'));
    }

    #[test]
    fn init_needs_exactly_one_source() {
        assert!(Cli::try_parse_from(["mitosis", "template", "init"]).is_err());
        assert!(Cli::try_parse_from([
            "mitosis",
            "template",
            "init",
            "--seed",
            "x",
            "--from-url",
            "y"
        ])
        .is_err());
        assert!(Cli::try_parse_from(["mitosis", "template", "init", "--seed", "x"]).is_ok());
    }

    #[test]
    fn settings_flags_parse() {
        let cli = Cli::try_parse_from([
            "mitosis",
            "template",
            "set",
            "main",
            "--keep",
            "2",
            "--every",
            "off",
            "--mode",
            "layered",
            "--migrate",
            "make migrate",
            "--auto-migrate",
            "on",
            "--on-stale",
            "refresh",
            "--max-age",
            "1d",
            "--resume",
        ])
        .unwrap();
        let Cmd::Template {
            command: TemplateCmd::Set {
                settings, resume, ..
            },
        } = cli.command
        else {
            panic!("wrong command")
        };
        assert!(resume);
        let (max_age, s) = settings.parse().unwrap();
        assert_eq!(max_age, Some(86_400));
        assert_eq!(s.keep, Some(2));
        assert!(s.every_off && s.every_secs.is_none());
        assert_eq!(s.every_mode, Some(crate::protocol::RefreshMode::Layered));
        assert_eq!(s.migrate.unwrap().command, "make migrate");
        assert_eq!(s.auto_migrate, Some(true));
        assert_eq!(s.on_stale, Some(crate::protocol::OnStale::Refresh));
        assert!(Cli::try_parse_from(["mitosis", "template", "set", "--mode", "fast"]).is_err());
    }
}
