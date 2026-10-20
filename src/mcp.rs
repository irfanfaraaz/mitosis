//! `mitosis mcp`: the clone tools served over MCP on stdio. stdout carries only MCP frames.

use crate::client;
use crate::envfile;
use crate::error::{self, ErrorCode, MitosisError};
use crate::names::{format_duration, parse_duration, validate_name};
use crate::paths::Home;
use crate::protocol::{BranchArgs, CloneCreateArgs, CloneInfo, Request, Response};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::schemars::JsonSchema;
use rmcp::{tool, tool_handler, tool_router, ServerHandler, ServiceExt};
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct CloneCreateParams {
    /// Template to clone. Default "main".
    pub template: Option<String>,
    /// Give the clone a stable name (a-z, 0-9, '_' or '-'). Use it when the same database must keep
    /// its URL across sessions: asking again for a running clone of that name returns it
    /// (`existing: true`) instead of a new one. Omit for a throwaway clone.
    pub name: Option<String>,
    /// How long the clone lives, like 30m, 2h or 1d. Default "2h".
    pub ttl: Option<String>,
    /// Keep crash-safe writes (slower). Default false.
    pub durable: Option<bool>,
    /// With `name`: destroy the running clone of that name and make a new one from the current
    /// template, so the data is reset. Default false.
    pub fresh: Option<bool>,
    /// A `.env` file to point at the clone: its URL is written there. A relative path is resolved
    /// against the folder the MCP server runs in. Default: no file is touched.
    pub env_file: Option<String>,
    /// Variable to set in `env_file`. Default "DATABASE_URL".
    pub env_var: Option<String>,
    /// Seconds to wait for room when the clone limits are full. 0 fails at once. Default: the
    /// daemon's setting.
    pub wait_secs: Option<u64>,
    /// Clone a template built for this git branch, so the database has the branch's schema: the
    /// template's migrate command runs in the git checkout the MCP server runs in. "" means the
    /// branch checked out there; it must be the branch checked out. Default: the template itself.
    pub branch: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct CloneDestroyParams {
    /// Clone id or name.
    pub id: String,
}

#[derive(Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct CloneDiffParams {
    /// Clone id or name.
    pub id_or_name: String,
    /// A table (`name` or `schema.name`) to compare row by row; it needs a primary key. Omit for
    /// the schema changes and row counts of every table.
    pub table: Option<String>,
    /// With `table`: rows shown per kind (inserted, deleted, updated). Default 20.
    pub limit: Option<u32>,
}

#[derive(Clone)]
pub struct Mitosis {
    home: Home,
}

// The tool macros use the plain `Result`, so the crate alias stays qualified as `error::Result`.
// Daemon errors arrive as `<message>\nhint: <hint>` (MitosisError's Display), never with a clone URL.
fn text(r: error::Result<String>) -> Result<String, String> {
    r.map_err(|e| e.to_string())
}

// Deliberately not `{r:?}`: a reply can hold a password-bearing URL. Same in `unexpected` in cli.rs.
fn unexpected() -> MitosisError {
    MitosisError::new(ErrorCode::ProtocolMismatch, "unexpected daemon reply")
        .with_hint("restart the daemon: mitosis daemon stop")
}

#[tool_router]
impl Mitosis {
    #[tool(
        description = "Create a private, throwaway Postgres database cloned from a template in about a second. Use it for migrations, seeds and destructive tests. It is deleted when this session ends or the TTL passes."
    )]
    async fn clone_create(
        &self,
        Parameters(p): Parameters<CloneCreateParams>,
    ) -> Result<String, String> {
        text(self.create(p).await)
    }

    #[tool(description = "List the clones this session created, as JSON.")]
    async fn clone_list(&self) -> Result<String, String> {
        text(
            async {
                let mine: Vec<CloneInfo> = self
                    .all_clones()
                    .await?
                    .into_iter()
                    .filter(is_mine)
                    .collect();
                Ok(serde_json::to_string_pretty(&mine)?)
            }
            .await,
        )
    }

    #[tool(description = "Destroy a clone this session created, by id or name.")]
    async fn clone_destroy(
        &self,
        Parameters(p): Parameters<CloneDestroyParams>,
    ) -> Result<String, String> {
        text(self.destroy(p.id).await)
    }

    #[tool(
        description = "Show what you changed in one of your clones, compared with the template generation it was made from, as JSON: schema changes (tables, columns, indexes, constraints, views, functions, sequences, extensions), row counts per table before and after, and with `table` the inserted, deleted and updated rows by primary key."
    )]
    async fn clone_diff(
        &self,
        Parameters(p): Parameters<CloneDiffParams>,
    ) -> Result<String, String> {
        text(self.diff(p).await)
    }
}

/// Clones made through this MCP server name this process as their parent.
fn is_mine(c: &CloneInfo) -> bool {
    c.parent_pid == Some(std::process::id() as i32)
}

/// The clone `id_or_name` names, matched as the daemon does: an id wins over a name.
fn find<'a>(clones: &'a [CloneInfo], id_or_name: &str) -> Option<&'a CloneInfo> {
    clones.iter().find(|c| c.id == id_or_name).or_else(|| {
        clones
            .iter()
            .find(|c| c.name.as_deref() == Some(id_or_name))
    })
}

impl Mitosis {
    async fn all_clones(&self) -> error::Result<Vec<CloneInfo>> {
        match client::request(&self.home, Request::CloneList).await? {
            Response::Clones { clones } => Ok(clones),
            _ => Err(unexpected()),
        }
    }

    /// Destroys a clone of this session; another session's clone (or a CLI one) is refused.
    async fn destroy(&self, id_or_name: String) -> error::Result<String> {
        let clones = self.all_clones().await?;
        let c = find(&clones, &id_or_name).ok_or_else(|| {
            MitosisError::new(
                ErrorCode::CloneNotFound,
                format!("clone {id_or_name} not found"),
            )
            .with_hint("see this session's clones with the clone_list tool")
        })?;
        if !is_mine(c) {
            return Err(MitosisError::new(
                ErrorCode::InvalidRequest,
                format!(
                    "clone {} was not created by this session; it may be another agent's database",
                    c.id
                ),
            )
            .with_hint("this tool only destroys clones from clone_create in this session; a person can run `mitosis destroy <id>`"));
        }
        // By id, so a name reused meanwhile cannot redirect the destroy.
        match client::request(
            &self.home,
            Request::CloneDestroy {
                id_or_name: c.id.clone(),
                force: false,
                forget: false,
            },
        )
        .await?
        {
            Response::Destroyed { id } => Ok(format!("destroyed {id}")),
            _ => Err(unexpected()),
        }
    }

    /// Diffs a clone of this session; another session's clone (or a CLI one) is refused.
    async fn diff(&self, p: CloneDiffParams) -> error::Result<String> {
        let clones = self.all_clones().await?;
        let c = find(&clones, &p.id_or_name).ok_or_else(|| {
            MitosisError::new(
                ErrorCode::CloneNotFound,
                format!("clone {} not found", p.id_or_name),
            )
            .with_hint("see this session's clones with the clone_list tool")
        })?;
        if !is_mine(c) {
            return Err(MitosisError::new(
                ErrorCode::InvalidRequest,
                format!(
                    "clone {} was not created by this session; it may be another agent's database",
                    c.id
                ),
            )
            .with_hint("this tool only reads clones from clone_create in this session"));
        }
        // By id, so a name reused meanwhile cannot redirect the diff; the daemon checks the
        // session again.
        let req = Request::CloneDiff {
            id_or_name: c.id.clone(),
            table: p.table,
            limit: p.limit,
            parent_pid: Some(std::process::id() as i32),
        };
        match client::request(&self.home, req).await? {
            Response::Diff(report) => Ok(serde_json::to_string_pretty(&report)?),
            _ => Err(unexpected()),
        }
    }

    async fn create(&self, p: CloneCreateParams) -> error::Result<String> {
        let template = p.template.unwrap_or_else(|| "main".into());
        validate_name("template", &template)?;
        if let Some(n) = &p.name {
            validate_name("clone", n)?;
        }
        let ttl_secs = parse_duration(p.ttl.as_deref().unwrap_or("2h"))?.as_secs();
        let args = CloneCreateArgs {
            template: template.clone(),
            name: p.name,
            ttl_secs,
            durable: p.durable.unwrap_or(false),
            fresh: p.fresh.unwrap_or(false),
            // Agents must not make undeletable clones.
            protect: false,
            env: None,
            wait_secs: p.wait_secs,
            // `create_clone` resolves the branch and the repository root from our cwd.
            branch: match p.branch {
                Some(name) => Some(BranchArgs {
                    name,
                    repo_root: std::env::current_dir()?,
                    path: None,
                }),
                None => None,
            },
            // This process is the parent: when the MCP session ends, the daemon destroys the clone.
            parent_pid: Some(std::process::id() as i32),
        };
        // Also finishes pending `.env` restores first, and resolves a relative path against our cwd.
        let info =
            envfile::create_clone(&self.home, args, p.env_file.map(PathBuf::from), p.env_var)
                .await?;
        // Warnings are best effort: the clone already exists.
        let mut warnings: Vec<String> =
            match client::request(&self.home, Request::TemplateList).await {
                Ok(Response::Templates { templates }) => templates
                    .into_iter()
                    .filter(|t| t.name == info.template)
                    .flat_map(|t| t.warnings)
                    .collect(),
                _ => Vec::new(),
            };
        warnings.extend(info.warnings.iter().cloned());
        let mut out = serde_json::to_value(&info)?;
        if let Some(obj) = out.as_object_mut() {
            obj.insert(
                "template_age".into(),
                format_duration(Duration::from_secs(info.template_age_secs)).into(),
            );
            obj.insert("warnings".into(), warnings.into());
        }
        Ok(serde_json::to_string_pretty(&out)?)
    }
}

#[tool_handler(
    name = "mitosis",
    instructions = "Mitosis gives this session private, throwaway Postgres databases, cloned copy-on-write from a template in about a second. clone_create makes one and returns its connection URL; clone_list shows the clones this session made; clone_destroy deletes one of them; clone_diff shows what you changed in one. Clones are deleted when this session ends or their TTL passes."
)]
impl ServerHandler for Mitosis {}

/// Serves the clone tools on stdin/stdout until stdin closes.
pub async fn serve_stdio(home: Home) -> error::Result<()> {
    let internal = |e: String| MitosisError::new(ErrorCode::Internal, format!("mcp: {e}"));
    // Restores a dead clone's `.env` value; warnings are best effort and stdout is MCP-only.
    for w in envfile::finish_pending(&home).await {
        eprintln!("mitosis: {w}");
    }
    let service = Mitosis { home }
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|e| internal(e.to_string()))?;
    service
        .waiting()
        .await
        .map_err(|e| internal(e.to_string()))?;
    Ok(())
}
