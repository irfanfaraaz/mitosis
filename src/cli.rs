use crate::budget;
use crate::client;
use crate::config::{Config, ConfigEntry};
use crate::daemon;
use crate::doctor;
use crate::envfile;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::events;
use crate::mcp;
use crate::names::{format_duration, parse_duration, validate_name};
use crate::paths::Home;
use crate::protocol::{
    BranchArgs, CloneCreateArgs, CloneInfo, EventFilter, EventRow, MigrateCmd, Request, Response,
    TemplateInfo, TemplateInitArgs, TemplateSetArgs, TemplateSettings, TemplateSource,
};
use crate::service;
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
        /// Destroy a running clone of this name first (same URL, fresh data).
        #[arg(long)]
        fresh: bool,
        /// Never expire; needs --name. Destroy it with `mitosis destroy <name> --force`.
        #[arg(long)]
        protect: bool,
        /// Write the URL into this .env file (restored when the clone is destroyed).
        #[arg(long)]
        env_file: Option<std::path::PathBuf>,
        /// The variable to set in --env-file.
        #[arg(long, requires = "env_file")]
        env_var: Option<String>,
        /// How long to wait for room under the limits, like 30s or 2m; 0 fails at once.
        #[arg(long)]
        wait: Option<String>,
        /// Clone the template built for a git branch (default: the branch checked out here),
        /// layered on --template by running its migrate command in this checkout.
        #[arg(long, num_args = 0..=1, default_missing_value = "")]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Print `DATABASE_URL=<url>` (or --var) for a running clone.
    Env {
        id_or_name: String,
        #[arg(long, default_value = "DATABASE_URL")]
        var: String,
        /// Print the URL straight to the clone's port instead of the router URL.
        #[arg(long)]
        direct: bool,
    },
    /// List clones.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Live view of clones, memory and disk against the limits (q to quit).
    Top,
    /// Destroy a clone by id or name.
    Destroy {
        id_or_name: String,
        /// Needed for a protected clone.
        #[arg(long)]
        force: bool,
        /// Also release the name's reserved port and password.
        #[arg(long)]
        forget: bool,
    },
    /// Start a sleeping clone now instead of on its first connection.
    Wake { id_or_name: String },
    /// Show what a clone changed against the template generation it came from.
    Diff {
        id_or_name: String,
        /// Compare one table row by row (needs a primary key).
        #[arg(long)]
        table: Option<String>,
        /// Rows shown per kind with --table.
        #[arg(long, requires = "table")]
        limit: Option<u32>,
        #[arg(long)]
        json: bool,
    },
    /// Make a running clone the newest generation of its template (or of --template).
    Promote {
        clone: String,
        /// Promote into this template instead of the clone's own; created when it does not exist.
        #[arg(long)]
        template: Option<String>,
    },
    /// Remove expired clones and leftovers.
    Gc,
    /// What the daemon did and why: creates, destroys, builds, rollbacks, gc, recovery, queue waits.
    Events {
        /// Keep running and print new events as they happen.
        #[arg(long)]
        follow: bool,
        /// Only events of this clone (name or id).
        #[arg(long)]
        clone: Option<String>,
        /// Only events of this template.
        #[arg(long)]
        template: Option<String>,
        /// Only this kind (`clone.created`) or group (`clone`).
        #[arg(long)]
        kind: Option<String>,
        /// Only events from this long ago, like 30m, 2h or 1d.
        #[arg(long)]
        since: Option<String>,
        /// A JSON array; with --follow one JSON object per line.
        #[arg(long)]
        json: bool,
    },
    /// Download and manage Postgres builds (the only commands that ever download).
    Pg {
        #[command(subcommand)]
        command: PgCmd,
    },
    /// Check the home, config, daemon, Postgres installs and templates for problems.
    Doctor {
        /// Finish pending .env restores and run gc.
        #[arg(long)]
        fix: bool,
        #[arg(long)]
        json: bool,
    },
    /// Run the daemon in the foreground, or manage it.
    Daemon {
        #[command(subcommand)]
        command: Option<DaemonCmd>,
    },
    /// Serve the MCP tools over stdio.
    Mcp,
    /// Keep the daemon running from login (launchd user agent).
    Service {
        #[command(subcommand)]
        command: ServiceCmd,
    },
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
    /// Build a template from a database URL, a physical copy of a server, or a seed command.
    Init {
        #[arg(default_value = "main")]
        name: String,
        /// Copy an existing database with pg_dump.
        #[arg(
            long,
            conflicts_with_all = ["seed", "basebackup"],
            required_unless_present_any = ["seed", "basebackup"]
        )]
        from_url: Option<String>,
        /// Copy a running server physically with pg_basebackup (needs a role with REPLICATION).
        #[arg(long, conflicts_with = "seed")]
        basebackup: Option<String>,
        /// Shell command run in this folder with DATABASE_URL set.
        #[arg(long)]
        seed: Option<String>,
        /// The database to keep (a --basebackup copy has all of the server's; default: the URL's).
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
    /// Move a template to a newer Postgres major with pg_upgrade.
    Upgrade {
        name: String,
        /// Target Postgres major, like 18.
        #[arg(long)]
        to: u32,
        /// Make a new template with this name instead; the original stays as it is.
        #[arg(long = "as")]
        as_name: Option<String>,
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
    /// Masking file (TOML: columns to mask, row subsets) applied to every build, or off.
    #[arg(long)]
    mask: Option<std::path::PathBuf>,
    /// The git branch the template's checkout follows (layered builds skip on other branches),
    /// or off.
    #[arg(long)]
    base_branch: Option<String>,
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
        let (mask, mask_off) = match self.mask {
            Some(p) if p.as_os_str() == "off" => (None, true),
            // Stored absolute, like --migrations, so the daemon finds it from any folder.
            Some(p) => (
                Some(std::fs::canonicalize(&p).map_err(|e| {
                    MitosisError::new(
                        ErrorCode::InvalidRequest,
                        format!("masking file {}: {e}", p.display()),
                    )
                })?),
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
            mask,
            mask_off,
            base_branch: self.base_branch.clone().filter(|b| b != "off"),
            base_branch_off: self.base_branch.as_deref() == Some("off"),
        };
        Ok((max_age_secs, settings))
    }
}

#[derive(Subcommand)]
enum PgCmd {
    /// Download, verify and unpack the newest build of a Postgres major into the Mitosis home.
    Install {
        major: u32,
        /// An exact minor (17.9), or a release tag (17.9.1), instead of the newest.
        #[arg(long)]
        version: Option<String>,
    },
    /// List the builds installed by `pg install` and which ones Mitosis uses.
    List,
    /// Remove an installed build by major (17) or version (17.9).
    Remove { spec: String },
}

#[derive(Subcommand)]
enum ServiceCmd {
    /// Install and start the login service for this binary and home.
    Install,
    /// Stop and remove the login service.
    Uninstall,
    /// Show whether the login service is installed and running.
    Status {
        #[arg(long)]
        json: bool,
    },
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
        // Under launchd a non-zero exit means "relaunch me": another daemon already serves, so stop quietly.
        if std::env::var_os("MITOSIS_SERVICE").is_some_and(|v| v == "1") {
            return ExitCode::SUCCESS;
        }
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
        Cmd::Service { command } => match command {
            ServiceCmd::Install => {
                service::install()?;
                let exe = std::env::current_exe()?;
                println!("service installed: the daemon starts at login");
                println!("binary    {}", exe.display());
                if exe.components().any(|c| c.as_os_str() == "target") {
                    eprintln!(
                        "warning: the service points at a build folder; it breaks if you run cargo clean (use cargo install for a stable path)"
                    );
                }
            }
            ServiceCmd::Uninstall => {
                service::uninstall()?;
                println!("service removed");
            }
            ServiceCmd::Status { json } => {
                let s = service::status()?;
                if json {
                    print_json(&s)?;
                } else {
                    println!("installed {}", s.installed);
                    println!("loaded    {}", s.loaded);
                    println!("running   {}", s.running);
                    if let Some(b) = &s.binary {
                        let gone = if s.binary_exists { "" } else { "  (missing)" };
                        println!("binary    {}{gone}", b.display());
                    }
                }
            }
        },
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
            // The file is written, but an environment variable of this shell still wins.
            if let Some((var, value)) = Config::env_override(&home, &key)? {
                eprintln!("note: {var} is set and overrides this; the value in effect is {value}");
            }
        }
        Cmd::Template { command } => template(&home, command).await?,
        Cmd::Pg { command } => pg(&home, command).await?,
        Cmd::Clone {
            template,
            name,
            ttl,
            durable,
            fresh,
            protect,
            env_file,
            env_var,
            wait,
            branch,
            json,
        } => {
            validate_name("template", &template)?;
            if let Some(n) = &name {
                validate_name("clone", n)?;
            }
            let ttl_secs = parse_duration(&ttl)?.as_secs();
            let wait_secs = match wait.as_deref() {
                None => None,
                Some("0" | "0s") => Some(0),
                Some(w) => Some(parse_duration(w)?.as_secs()),
            };
            let args = CloneCreateArgs {
                template: template.clone(),
                name,
                ttl_secs,
                durable,
                fresh,
                protect,
                // The CLI exits right after printing the URL, so watching it would destroy the
                // clone at once: CLI clones live until their TTL or `mitosis destroy`.
                parent_pid: None,
                env: None,
                wait_secs,
                // `create_clone` resolves the branch and the repository root from this folder.
                branch: match branch {
                    Some(name) => Some(BranchArgs {
                        name,
                        repo_root: std::env::current_dir()?,
                        path: None,
                    }),
                    None => None,
                },
            };
            let info = envfile::create_clone(&home, args, env_file, env_var).await?;
            for w in &info.warnings {
                eprintln!("warning: {w}");
            }
            if info.existing {
                eprintln!("note: clone {} was already running", info.id);
            }
            if info.rebuilt {
                eprintln!(
                    "note: template {} was rebuilt for this branch",
                    info.template
                );
            }
            // Warnings are best effort: the clone already exists.
            if let Ok(Response::Templates { templates }) =
                client::request(&home, Request::TemplateList).await
            {
                for w in templates
                    .iter()
                    .filter(|t| t.name == info.template)
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
        Cmd::Env {
            id_or_name,
            var,
            direct,
        } => {
            // The printed line goes into a .env file: the same names `--env-var` accepts.
            envfile::check_var(&var)?;
            let clones = match client::request(&home, Request::CloneList).await? {
                Response::Clones { clones } => clones,
                other => return Err(unexpected(other)),
            };
            let c = clones
                .iter()
                .find(|c| c.id == id_or_name || c.name.as_deref() == Some(id_or_name.as_str()))
                .ok_or_else(|| {
                    MitosisError::new(
                        ErrorCode::CloneNotFound,
                        format!("clone {id_or_name} not found"),
                    )
                    .with_hint("see running clones with `mitosis list`")
                })?;
            // A sleeping clone's URL works (the router wakes it); these two do not.
            let not_usable = match c.status.as_str() {
                "starting" => Some("is still starting; try again in a moment"),
                "stopping" => Some("is being destroyed"),
                _ => None,
            };
            if let Some(why) = not_usable {
                return Err(MitosisError::new(
                    ErrorCode::InvalidRequest,
                    format!("clone {id_or_name} {why}"),
                ));
            }
            println!("{var}={}", if direct { &c.direct_url } else { &c.url });
        }
        Cmd::List { json } => {
            let clones = match client::request(&home, Request::CloneList).await? {
                Response::Clones { clones } => clones,
                other => return Err(unexpected(other)),
            };
            for w in envfile::finish_pending(&home).await {
                eprintln!("warning: {w}");
            }
            if json {
                print_json(&clones)?;
            } else {
                print_clones(&clones);
            }
        }
        Cmd::Top => top(&home).await?,
        Cmd::Destroy {
            id_or_name,
            force,
            forget,
        } => {
            let req = Request::CloneDestroy {
                id_or_name,
                force,
                forget,
            };
            match client::request(&home, req).await? {
                Response::Destroyed { id } => println!("{id}"),
                other => return Err(unexpected(other)),
            }
            for w in envfile::finish_pending(&home).await {
                eprintln!("warning: {w}");
            }
        }
        Cmd::Wake { id_or_name } => {
            let req = Request::CloneWake { id_or_name };
            match client::request(&home, req).await? {
                Response::Clone(info) => {
                    for w in &info.warnings {
                        eprintln!("warning: {w}");
                    }
                    println!("{}", info.name.as_deref().unwrap_or(&info.id));
                }
                other => return Err(unexpected(other)),
            }
        }
        Cmd::Diff {
            id_or_name,
            table,
            limit,
            json,
        } => {
            let req = Request::CloneDiff {
                id_or_name,
                table,
                limit,
                parent_pid: None,
            };
            match client::request(&home, req).await? {
                Response::Diff(report) if json => print_json(&report)?,
                Response::Diff(report) => print!("{}", crate::diff::render(&report)),
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
        Cmd::Doctor { fix, json } => {
            let checks = doctor::run(&home, fix).await;
            if json {
                print_json(&checks)?;
            } else {
                print!("{}", doctor::render(&checks));
            }
            return Ok(ExitCode::from(doctor::exit_code(&checks)));
        }
        Cmd::Events {
            follow,
            clone,
            template,
            kind,
            since,
            json,
        } => {
            let since = match since {
                Some(d) => Some(now().saturating_sub(parse_duration(&d)?.as_secs() as i64)),
                None => None,
            };
            let filter = EventFilter {
                clone,
                template,
                kind,
                since,
                limit: None,
            };
            let show = |e: &EventRow| -> Result<()> {
                if json {
                    println!("{}", serde_json::to_string(e)?);
                } else {
                    println!(
                        "{}  {}  {}  {}",
                        events::local_time(e.ts),
                        e.kind,
                        events::row_label(e),
                        events::short_detail(&e.detail)
                    );
                }
                Ok(())
            };
            if follow {
                let mut failed = None;
                client::follow_events(&home, filter, |e| {
                    if failed.is_none() {
                        failed = show(&e).err();
                    }
                })
                .await?;
                if let Some(e) = failed {
                    return Err(e);
                }
            } else {
                match client::request(
                    &home,
                    Request::Events {
                        filter,
                        follow: false,
                    },
                )
                .await?
                {
                    Response::Events { events } if json => print_json(&events)?,
                    Response::Events { events } => {
                        for e in &events {
                            show(e)?;
                        }
                    }
                    other => return Err(unexpected(other)),
                }
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
                println!("version   {}", s.version);
                println!("service   {}", s.service);
                println!("home      {}", s.home.display());
                println!("clones    {}", s.clones);
                println!("templates {}", s.templates);
                println!(
                    "uptime    {}",
                    format_duration(Duration::from_secs(s.uptime_secs))
                );
                println!("router    {}", router_state(&s.router));
                if s.router.as_ref().is_some_and(|r| !r.listening) {
                    eprintln!(
                        "warning: the router is not listening; it retries every minute (direct URLs still work: mitosis env <clone> --direct)"
                    );
                }
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

/// Runs in this process, not the daemon; the blocking download and unpack go to a worker thread.
async fn pg(home: &Home, cmd: PgCmd) -> Result<()> {
    use crate::pg::managed;
    let h = home.clone();
    let blocking =
        |e: tokio::task::JoinError| MitosisError::new(ErrorCode::Internal, e.to_string());
    match cmd {
        PgCmd::Install { major, version } => {
            let src = managed::ReleaseSource::from_env();
            let i = tokio::task::spawn_blocking(move || {
                managed::install(&h, major, version.as_deref(), &src)
            })
            .await
            .map_err(blocking)??;
            println!("{}", i.dir.join("bin").display());
        }
        PgCmd::List => {
            let installs = tokio::task::spawn_blocking(move || managed::list(&h))
                .await
                .map_err(blocking)??;
            if installs.is_empty() {
                println!("no managed Postgres builds (mitosis pg install <major>)");
            } else {
                let rows = installs
                    .iter()
                    .map(|i| {
                        vec![
                            i.version.clone(),
                            if i.used { "yes" } else { "no" }.to_string(),
                            i.dir.display().to_string(),
                        ]
                    })
                    .collect();
                print_table(&["VERSION", "USED", "PATH"], rows);
            }
        }
        PgCmd::Remove { spec } => {
            let r = tokio::task::spawn_blocking(move || {
                managed::remove(&h, &spec, &|bin| managed::used_by_store(&h, bin))
            })
            .await
            .map_err(blocking)??;
            println!("removed Postgres {}", r.version);
        }
    }
    Ok(())
}

async fn template(home: &Home, cmd: TemplateCmd) -> Result<()> {
    match cmd {
        TemplateCmd::Init {
            name,
            from_url,
            basebackup,
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
            let source = match (from_url, basebackup, seed) {
                (Some(url), _, _) => TemplateSource::Url { url },
                (None, Some(url), _) => TemplateSource::Basebackup { url },
                // The seed runs where and with the PATH it would in this shell.
                (None, None, Some(command)) => TemplateSource::Seed {
                    command,
                    cwd: Some(std::env::current_dir()?),
                    path: std::env::var("PATH").ok(),
                },
                (None, None, None) => {
                    unreachable!("clap requires --from-url, --basebackup or --seed")
                }
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
                // Branch templates right under their base, indented; any whose base is gone last.
                let is_base = |t: &&TemplateInfo, b: &str| t.base.as_deref() == Some(b);
                let mut ordered: Vec<&TemplateInfo> = Vec::new();
                for t in templates.iter().filter(|t| t.base.is_none()) {
                    ordered.push(t);
                    ordered.extend(templates.iter().filter(|b| is_base(b, &t.name)));
                }
                ordered.extend(templates.iter().filter(|t| {
                    t.base
                        .as_deref()
                        .is_some_and(|b| !templates.iter().any(|x| x.name == b))
                }));
                print_table(
                    &[
                        "NAME", "GEN", "PAUSED", "PG", "DATABASE", "SIZE", "AGE", "SOURCE",
                    ],
                    ordered
                        .into_iter()
                        .map(|t| {
                            vec![
                                if t.base.is_some() {
                                    format!("  {}", t.name)
                                } else {
                                    t.name.clone()
                                },
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
            crate::templates::validate_template_ref(&name)?;
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
        TemplateCmd::Upgrade { name, to, as_name } => {
            validate_name("template", &name)?;
            if let Some(n) = &as_name {
                validate_name("template", n)?;
            }
            let req = Request::TemplateUpgrade {
                name: name.clone(),
                to,
                as_name,
            };
            match client::request(home, req).await? {
                Response::TemplateBuilt(t) => println!(
                    "upgraded {name} to Postgres {}: template {} generation {} ({})",
                    t.pg_major,
                    t.name,
                    t.current_gen,
                    human_bytes(t.size_bytes)
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
        TemplateSource::Basebackup { url } => format!("basebackup: {url}"),
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
            "URL",
            "AGE",
            "TTL LEFT",
            "RAM",
            "EXTRA DISK",
            "FLAGS",
        ],
        clones
            .iter()
            .map(|c| {
                vec![
                    c.id.clone(),
                    c.name.clone().unwrap_or_else(|| "-".into()),
                    c.template.clone(),
                    c.port.to_string(),
                    // The password stays out of the table; `mitosis env` prints the full URL.
                    crate::templates::redact_url(&c.url),
                    secs(now - c.created_at),
                    if c.protected {
                        "never".into()
                    } else {
                        secs(c.expires_at - now)
                    },
                    opt_bytes(c.rss_bytes),
                    opt_bytes(c.own_disk_bytes),
                    flags(c),
                ]
            })
            .collect(),
    );
}

/// `sleeping`, `!` for a clone past its disk warning, then `protected`.
fn flags(c: &CloneInfo) -> String {
    let mut f = Vec::new();
    if c.status == "sleeping" {
        f.push("sleeping");
    }
    if c.disk_warn {
        f.push("!");
    }
    if c.protected {
        f.push("protected");
    }
    f.join(" ")
}

/// Puts the terminal in no-echo, no-line-buffer mode on the alternate screen, and puts it back on
/// drop, so every way out of `top` (q, Ctrl-C, an error, a panic) leaves a usable terminal.
struct RawTerminal {
    saved: nix::sys::termios::Termios,
}

impl RawTerminal {
    fn enter() -> Result<RawTerminal> {
        use nix::sys::termios::{
            tcgetattr, tcsetattr, LocalFlags, SetArg, SpecialCharacterIndices as C,
        };
        let stdin = std::io::stdin();
        let saved = tcgetattr(&stdin).map_err(|_| {
            MitosisError::new(ErrorCode::InvalidRequest, "mitosis top needs a terminal")
                .with_hint("run it in a terminal, or use `mitosis list --json`")
        })?;
        let mut raw = saved.clone();
        // ISIG off: Ctrl-C arrives as a byte, so this process can clean up before it exits.
        raw.local_flags
            .remove(LocalFlags::ICANON | LocalFlags::ECHO | LocalFlags::ISIG);
        // A read returns after 2 s (in tenths of a second) or at the first key.
        raw.control_chars[C::VMIN as usize] = 0;
        raw.control_chars[C::VTIME as usize] = 20;
        tcsetattr(&stdin, SetArg::TCSANOW, &raw)
            .map_err(|e| MitosisError::new(ErrorCode::Internal, format!("terminal: {e}")))?;
        let term = RawTerminal { saved };
        print!("\x1b[?1049h\x1b[?25l");
        let _ = std::io::Write::flush(&mut std::io::stdout());
        Ok(term)
    }
}

impl Drop for RawTerminal {
    fn drop(&mut self) {
        let _ = nix::sys::termios::tcsetattr(
            std::io::stdin(),
            nix::sys::termios::SetArg::TCSANOW,
            &self.saved,
        );
        print!("\x1b[?25h\x1b[?1049l");
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

/// Waits up to 2 s for a key; true when it asks to quit (`q` or Ctrl-C). Reads everything
/// waiting at once, so a multi-byte key (an arrow) causes one redraw, not one per byte.
fn quit_key() -> bool {
    use std::io::Read;
    let mut b = [0u8; 64];
    matches!(std::io::stdin().read(&mut b), Ok(n) if asks_to_quit(&b[..n]))
}

/// True when the bytes read from the terminal hold `q`, `Q` or Ctrl-C.
fn asks_to_quit(bytes: &[u8]) -> bool {
    bytes.iter().any(|b| matches!(b, b'q' | b'Q' | 3))
}

/// `off`, `listening on 127.0.0.1:54300`, or why the router is not listening.
pub fn router_state(r: &Option<crate::protocol::RouterStatus>) -> String {
    match r {
        None => "off".into(),
        Some(r) => {
            let host = if r.listen.is_empty() {
                "127.0.0.1"
            } else {
                &r.listen
            };
            match &r.error {
                None if r.listening => format!("listening on {host}:{}", r.port),
                None => format!("not listening on {host}:{}", r.port),
                Some(e) => format!("not listening: {e}"),
            }
        }
    }
}

fn draw_top(u: &budget::Usage, router: &Option<crate::protocol::RouterStatus>) {
    let now = now();
    let secs = |s: i64| format_duration(Duration::from_secs(u64::try_from(s).unwrap_or(0)));
    let opt_bytes = |b: Option<u64>| b.map_or("-".to_string(), human_bytes);
    let of = |used: String, cap: Option<u64>| match cap {
        Some(c) => format!("{used} / {}", human_bytes(c)),
        None => format!("{used} / no limit"),
    };
    print!("\x1b[H\x1b[2J");
    println!("mitosis top    q to quit, refreshes every 2s\n");
    print_table(
        &[
            "ID",
            "NAME",
            "TEMPLATE",
            "STATUS",
            "TTL LEFT",
            "RAM",
            "EXTRA DISK",
            "FLAGS",
        ],
        u.clones
            .iter()
            .map(|c| {
                vec![
                    c.id.clone(),
                    c.name.clone().unwrap_or_else(|| "-".into()),
                    c.template.clone(),
                    c.status.clone(),
                    if c.protected {
                        "never".into()
                    } else {
                        secs(c.expires_at - now)
                    },
                    // A sleeping clone has no server: it uses no memory.
                    if c.status == "sleeping" {
                        human_bytes(0)
                    } else {
                        opt_bytes(c.rss_bytes)
                    },
                    opt_bytes(c.own_disk_bytes),
                    [(c.disk_warn, "!"), (c.protected, "protected")]
                        .iter()
                        .filter(|(on, _)| *on)
                        .map(|(_, t)| *t)
                        .collect::<Vec<_>>()
                        .join(" "),
                ]
            })
            .collect(),
    );
    println!(
        "\nclones  {} / {}\nram     {}\ndisk    {}\nfree    {}\nqueued  {}\nrouter  {}",
        u.clones.len(),
        u.max_clones,
        of(human_bytes(u.ram_bytes), u.ram_budget),
        of(human_bytes(u.disk_bytes), u.disk_budget),
        human_bytes(u.free_disk_bytes),
        u.queued,
        router_state(router)
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

async fn top(home: &Home) -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let terminal = RawTerminal::enter()?;
    // ISIG is off, but `kill`, a closed terminal or a logout still send these. Without a handler
    // the process dies with the terminal left raw.
    let sig = |k: SignalKind| {
        signal(k).map_err(|e| MitosisError::new(ErrorCode::Internal, format!("signals: {e}")))
    };
    let mut term = sig(SignalKind::terminate())?;
    let mut hup = sig(SignalKind::hangup())?;
    let mut int = sig(SignalKind::interrupt())?;
    let signo = tokio::select! {
        r = top_loop(home) => return r,
        _ = term.recv() => libc::SIGTERM,
        _ = hup.recv() => libc::SIGHUP,
        _ = int.recv() => libc::SIGINT,
    };
    drop(terminal);
    // The usual exit status of a process ended by that signal. Exits at once: the key-reading
    // thread may still be blocked in a read.
    std::process::exit(128 + signo);
}

/// `top`'s draw-and-wait loop; ends on `q` or Ctrl-C.
async fn top_loop(home: &Home) -> Result<()> {
    loop {
        let router = match client::request(home, Request::Status).await? {
            Response::Status(s) => s.router,
            other => return Err(unexpected(other)),
        };
        match client::request(home, Request::Usage).await? {
            Response::Usage(u) => draw_top(&u, &router),
            other => return Err(unexpected(other)),
        }
        let quit = tokio::task::spawn_blocking(quit_key)
            .await
            .map_err(|e| MitosisError::new(ErrorCode::Internal, format!("terminal: {e}")))?;
        if quit {
            return Ok(());
        }
    }
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
        let basebackup = |extra: &[&str]| {
            let mut args = vec!["mitosis", "template", "init", "--basebackup", "u"];
            args.extend_from_slice(extra);
            Cli::try_parse_from(args)
        };
        assert!(basebackup(&[]).is_ok());
        assert!(basebackup(&["--database", "shop"]).is_ok());
        assert!(basebackup(&["--seed", "x"]).is_err());
        assert!(basebackup(&["--from-url", "y"]).is_err());
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

    #[test]
    fn top_reads_a_whole_key_at_once() {
        assert!(asks_to_quit(b"q") && asks_to_quit(b"Q") && asks_to_quit(&[3]));
        // An arrow key is three bytes: one read, no quit.
        assert!(!asks_to_quit(b"\x1b[A"));
        assert!(
            asks_to_quit(b"\x1b[Aq"),
            "a q behind other keys still quits"
        );
        assert!(!asks_to_quit(b""));
    }
}
