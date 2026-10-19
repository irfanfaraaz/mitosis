//! `mitosis mcp`: the clone tools served over MCP on stdio. stdout carries only MCP frames.

use crate::client;
use crate::error::{self, ErrorCode, MitosisError};
use crate::names::{format_duration, parse_duration, validate_name};
use crate::paths::Home;
use crate::protocol::{CloneCreateArgs, Request, Response};
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

    #[tool(description = "List all Mitosis clones as JSON.")]
    async fn clone_list(&self) -> Result<String, String> {
        text(
            async {
                match client::request(&self.home, Request::CloneList).await? {
                    Response::Clones { clones } => Ok(serde_json::to_string_pretty(&clones)?),
                    _ => Err(unexpected()),
                }
            }
            .await,
        )
    }

    #[tool(description = "Destroy a clone by id or name.")]
    async fn clone_destroy(
        &self,
        Parameters(p): Parameters<CloneDestroyParams>,
    ) -> Result<String, String> {
        text(
            async {
                match client::request(&self.home, Request::CloneDestroy { id_or_name: p.id })
                    .await?
                {
                    Response::Destroyed { id } => Ok(format!("destroyed {id}")),
                    _ => Err(unexpected()),
                }
            }
            .await,
        )
    }
}

impl Mitosis {
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
    instructions = "Mitosis gives this session private, throwaway Postgres databases, cloned copy-on-write from a template in about a second. clone_create makes one and returns its connection URL; clone_list shows all clones; clone_destroy deletes one. Clones are deleted when this session ends or their TTL passes."
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
