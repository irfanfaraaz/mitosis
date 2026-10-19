//! `mitosis mcp`: the clone tools served over MCP on stdio. stdout carries only MCP frames.

use crate::client;
use crate::error::{self, ErrorCode, MitosisError};
use crate::names::{format_duration, parse_duration, validate_name};
use crate::paths::Home;
use crate::protocol::{CloneCreateArgs, CloneInfo, Request, Response};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::schemars::JsonSchema;
use rmcp::{tool, tool_handler, tool_router, ServerHandler, ServiceExt};
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct CloneCreateParams {
    /// Template to clone. Default "main".
    pub template: Option<String>,
    /// Optional clone name: a-z, 0-9, '_' or '-'.
    pub name: Option<String>,
    /// How long the clone lives, like 30m, 2h or 1d. Default "2h".
    pub ttl: Option<String>,
    /// Keep crash-safe writes (slower). Default false.
    pub durable: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct CloneDestroyParams {
    /// Clone id or name.
    pub id: String,
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
            fresh: false,
            protect: false,
            env: None,
            wait_secs: None,
            // This process is the parent: when the MCP session ends, the daemon destroys the clone.
            parent_pid: Some(std::process::id() as i32),
        };
        let info = match client::request(&self.home, Request::CloneCreate(args)).await? {
            Response::CloneCreated(info) => info,
            _ => return Err(unexpected()),
        };
        // Warnings are best effort: the clone already exists.
        let warnings: Vec<String> = match client::request(&self.home, Request::TemplateList).await {
            Ok(Response::Templates { templates }) => templates
                .into_iter()
                .filter(|t| t.name == template)
                .flat_map(|t| t.warnings)
                .collect(),
            _ => Vec::new(),
        };
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
    instructions = "Mitosis gives this session private, throwaway Postgres databases, cloned copy-on-write from a template in about a second. clone_create makes one and returns its connection URL; clone_list shows the clones this session made; clone_destroy deletes one of them. Clones are deleted when this session ends or their TTL passes."
)]
impl ServerHandler for Mitosis {}

/// Serves the clone tools on stdin/stdout until stdin closes.
pub async fn serve_stdio(home: Home) -> error::Result<()> {
    let internal = |e: String| MitosisError::new(ErrorCode::Internal, format!("mcp: {e}"));
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
