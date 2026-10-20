//! Helpers for tests that drive the `mitosis` binary and a real daemon (CLI and MCP). Include it
//! with `#[path = "common/cli.rs"] mod cli;` next to `mod common;`.
#![allow(dead_code)] // each test binary uses a different subset

use crate::common::tmp_home;
use mitosis::protocol::CloneInfo;
use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use rmcp::{RoleClient, ServiceExt};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard};

/// Each test runs a real daemon on the default port range, so the tests of one binary run one at
/// a time: parallel daemons race for the same lowest free port.
pub fn serial_home() -> (MutexGuard<'static, ()>, tempfile::TempDir) {
    static SERIAL: Mutex<()> = Mutex::new(());
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    (guard, tmp_home())
}

/// This binary's router port: not the default 54300, which a real daemon on this Mac may hold,
/// and above the 60000-60249 slice ports of `TestEnv`. Clone URLs here go through the router.
// shortcut: two test binaries pick from the same 1200 ports at random; cargo runs binaries one
// after another, so they only meet under a parallel runner (then give each binary its own range).
pub fn router_port() -> String {
    static PORT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PORT.get_or_init(|| {
        let r = u16::from_str_radix(&mitosis::names::random_hex(2), 16).unwrap();
        (60_250 + r % 1200).to_string()
    })
    .clone()
}

/// `mitosis <args>` for `home`, on this binary's router port.
pub fn mitosis(home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_mitosis"))
        .args(args)
        .env("MITOSIS_HOME", home)
        .env("MITOSIS_ROUTER_PORT", router_port())
        .output()
        .expect("run mitosis")
}

pub fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

pub fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

/// `mitosis <args>`, which must succeed; its stdout.
pub fn ok(home: &Path, args: &[&str]) -> String {
    let o = mitosis(home, args);
    assert!(
        o.status.success(),
        "mitosis {args:?} failed: {}",
        stderr(&o)
    );
    stdout(&o)
}

pub fn list(home: &Path) -> Vec<CloneInfo> {
    serde_json::from_str(&ok(home, &["list", "--json"])).expect("list --json parses")
}

/// Destroys every clone and stops the daemon, even when the test panics.
pub struct Cleanup<'a>(pub &'a Path);

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        if self.0.join("daemon.sock").exists() {
            let out = mitosis(self.0, &["list", "--json"]);
            for c in serde_json::from_slice::<Vec<CloneInfo>>(&out.stdout).unwrap_or_default() {
                let _ = mitosis(self.0, &["destroy", &c.id]);
            }
        }
        let _ = mitosis(self.0, &["daemon", "stop"]);
    }
}

pub type McpClient = RunningService<RoleClient, ()>;

/// Starts `mitosis mcp` on `home` as a child process and completes the MCP handshake.
pub async fn mcp_client(home: &Path) -> McpClient {
    mcp_client_in(home, None).await
}

/// Like `mcp_client`, with the server's working folder set (relative `env_file` paths resolve there).
pub async fn mcp_client_in(home: &Path, cwd: Option<&Path>) -> McpClient {
    let cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_mitosis")).configure(|c| {
        c.arg("mcp")
            .env("MITOSIS_HOME", home)
            .env("MITOSIS_ROUTER_PORT", router_port());
        if let Some(d) = cwd {
            c.current_dir(d);
        }
    });
    ().serve(TokioChildProcess::new(cmd).expect("spawn mitosis mcp"))
        .await
        .expect("mcp handshake")
}

pub async fn tool_names(client: &McpClient) -> Vec<String> {
    let mut names: Vec<String> = client
        .list_all_tools()
        .await
        .expect("list tools")
        .into_iter()
        .map(|t| t.name.into_owned())
        .collect();
    names.sort();
    names
}

pub async fn call(
    client: &McpClient,
    tool: &'static str,
    args: serde_json::Value,
) -> CallToolResult {
    let args = args.as_object().cloned().unwrap_or_default();
    client
        .call_tool(CallToolRequestParams::new(tool).with_arguments(args))
        .await
        .expect("call tool")
}

pub fn result_text(r: &CallToolResult) -> String {
    r.content
        .iter()
        .filter_map(|c| c.as_text())
        .map(|t| t.text.as_str())
        .collect()
}
