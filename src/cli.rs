use crate::client;
use crate::ctx::Config;
use crate::daemon;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::mcp;
use crate::names::{format_duration, parse_duration, validate_name};
use crate::paths::Home;
use crate::protocol::{CloneCreateArgs, CloneInfo, Request, Response, TemplateInfo, TemplateInitArgs, TemplateSource};
use crate::templates::now;
use clap::{Parser, Subcommand};
use std::process::ExitCode;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "mitosis", version, about = "Copy-on-write Postgres clones for AI agent tasks")]
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
    /// Remove expired clones and leftovers.
    Gc,
    /// Run the daemon in the foreground, or manage it.
    Daemon {
        #[command(subcommand)]
        command: Option<DaemonCmd>,
    },
    /// Serve the MCP tools over stdio.
    Mcp,
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
        /// Age after which clones warn, like 7d.
        #[arg(long)]
        max_age: Option<String>,
        #[arg(long)]
        pg_version: Option<u32>,
    },
    /// Rebuild a template from its recorded source.
    Refresh {
        #[arg(default_value = "main")]
        name: String,
    },
    /// List templates.
    List {
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
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
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
    ExitCode::FAILURE
}

fn unexpected(r: Response) -> MitosisError {
    MitosisError::new(ErrorCode::ProtocolMismatch, format!("unexpected daemon reply: {r:?}"))
        .with_hint("restart the daemon: mitosis daemon stop")
}

fn print_json<T: serde::Serialize>(v: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

async fn run(cli: Cli) -> Result<ExitCode> {
    let home = Home::from_env()?;
    match cli.command {
        Cmd::Daemon { command: None } => daemon::run(home, Config::default()).await?,
        Cmd::Mcp => mcp::serve_stdio(home).await?,
        Cmd::Template { command } => template(&home, command).await?,
        Cmd::Clone { template, name, ttl, durable, json } => {
            validate_name("template", &template)?;
            if let Some(n) = &name {
                validate_name("clone", n)?;
            }
            let ttl_secs = parse_duration(&ttl)?.as_secs();
            let args = CloneCreateArgs { template: template.clone(), name, ttl_secs, durable, parent_pid: None };
            let info = match client::request(&home, Request::CloneCreate(args)).await? {
                Response::CloneCreated(info) => info,
                other => return Err(unexpected(other)),
            };
            // Warnings are best effort: the clone already exists.
            if let Ok(Response::Templates { templates }) = client::request(&home, Request::TemplateList).await {
                for w in templates.iter().filter(|t| t.name == template).flat_map(|t| &t.warnings) {
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
        Cmd::Destroy { id_or_name } => match client::request(&home, Request::CloneDestroy { id_or_name }).await? {
            Response::Destroyed { id } => println!("{id}"),
            other => return Err(unexpected(other)),
        },
        Cmd::Gc => match client::request(&home, Request::Gc).await? {
            Response::Gc(r) => println!(
                "adopted {}, destroyed {}, orphan folders removed {}, records dropped {}",
                r.adopted.len(),
                r.destroyed.len(),
                r.orphan_folders_removed.len(),
                r.records_dropped.len()
            ),
            other => return Err(unexpected(other)),
        },
        Cmd::Daemon { command: Some(DaemonCmd::Status) } => {
            match client::request_if_running(&home, Request::Status).await? {
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
                    println!("uptime    {}", format_duration(Duration::from_secs(s.uptime_secs)));
                }
                Some(other) => return Err(unexpected(other)),
            }
        }
        Cmd::Daemon { command: Some(DaemonCmd::Stop) } => {
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

async fn template(home: &Home, cmd: TemplateCmd) -> Result<()> {
    match cmd {
        TemplateCmd::Init { name, from_url, seed, database, migrations, max_age, pg_version } => {
            validate_name("template", &name)?;
            let max_age_secs = max_age.as_deref().map(parse_duration).transpose()?.map(|d| d.as_secs());
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
                (None, Some(command)) => TemplateSource::Seed { command, cwd: Some(std::env::current_dir()?) },
                (None, None) => unreachable!("clap requires --from-url or --seed"),
            };
            let args = TemplateInitArgs { name, source, database, migrations, max_age_secs, pg_major: pg_version };
            match client::request(home, Request::TemplateInit(args)).await? {
                Response::TemplateBuilt(t) => print_built(&t),
                other => return Err(unexpected(other)),
            }
        }
        TemplateCmd::Refresh { name } => {
            validate_name("template", &name)?;
            match client::request(home, Request::TemplateRefresh { name }).await? {
                Response::TemplateBuilt(t) => print_built(&t),
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
            }
            if json {
                print_json(&templates)?;
            } else {
                print_table(
                    &["NAME", "PG", "DATABASE", "SIZE", "AGE"],
                    templates
                        .iter()
                        .map(|t| {
                            vec![
                                t.name.clone(),
                                t.pg_major.to_string(),
                                t.database.clone(),
                                human_bytes(t.size_bytes),
                                format_duration(Duration::from_secs(t.age_secs)),
                            ]
                        })
                        .collect(),
                );
            }
        }
    }
    Ok(())
}

fn print_built(t: &TemplateInfo) {
    println!("template {} built (Postgres {}, {})", t.name, t.pg_major, human_bytes(t.size_bytes));
}

fn print_clones(clones: &[CloneInfo]) {
    let now = now();
    let secs = |s: i64| format_duration(Duration::from_secs(u64::try_from(s).unwrap_or(0)));
    let opt_bytes = |b: Option<u64>| b.map_or("-".to_string(), human_bytes);
    print_table(
        &["ID", "NAME", "TEMPLATE", "PORT", "AGE", "TTL LEFT", "RAM", "EXTRA DISK"],
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
        let parts: Vec<String> = cells.iter().zip(&widths).map(|(c, w)| format!("{c:<w$}")).collect();
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
    fn init_needs_exactly_one_source() {
        assert!(Cli::try_parse_from(["mitosis", "template", "init"]).is_err());
        assert!(Cli::try_parse_from(["mitosis", "template", "init", "--seed", "x", "--from-url", "y"]).is_err());
        assert!(Cli::try_parse_from(["mitosis", "template", "init", "--seed", "x"]).is_ok());
    }
}
