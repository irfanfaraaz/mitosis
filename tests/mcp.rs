// The serial_home guard only orders whole tests, each on its own runtime thread; no task in the
// same runtime ever waits on it.
#![allow(clippy::await_holding_lock)]

mod common;

use common::{seed, tmp_home};
use mitosis::pg::discover;
use mitosis::protocol::{CloneInfo, TemplateSource};
use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use rmcp::{RoleClient, ServiceExt};
use serde_json::json;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard};

const SEED: &str = "create table t(id int); insert into t values (1)";

/// A real daemon on the default port range: one test at a time.
fn serial_home() -> (MutexGuard<'static, ()>, tempfile::TempDir) {
    static SERIAL: Mutex<()> = Mutex::new(());
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    (guard, tmp_home())
}

/// This binary's router port: not the default 54300, which a real daemon on this Mac may hold.
fn router_port() -> String {
    static PORT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PORT.get_or_init(|| {
        let r = u16::from_str_radix(&mitosis::names::random_hex(2), 16).unwrap();
        (60_250 + r % 700).to_string()
    })
    .clone()
}

fn mitosis(home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_mitosis"))
        .args(args)
        .env("MITOSIS_HOME", home)
        .env("MITOSIS_ROUTER_PORT", router_port())
        .output()
        .expect("run mitosis")
}

fn ok(home: &Path, args: &[&str]) -> String {
    let o = mitosis(home, args);
    assert!(
        o.status.success(),
        "mitosis {args:?} failed: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    String::from_utf8_lossy(&o.stdout).into_owned()
}

/// Destroys every clone and stops the daemon, even when the test panics.
struct Cleanup<'a>(&'a Path);

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

type McpClient = RunningService<RoleClient, ()>;

async fn mcp_client(home: &Path) -> McpClient {
    let cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_mitosis")).configure(|c| {
        c.arg("mcp")
            .env("MITOSIS_HOME", home)
            .env("MITOSIS_ROUTER_PORT", router_port());
    });
    ().serve(TokioChildProcess::new(cmd).expect("spawn mitosis mcp"))
        .await
        .expect("mcp handshake")
}

async fn call(client: &McpClient, tool: &'static str, args: serde_json::Value) -> CallToolResult {
    let args = args.as_object().cloned().unwrap_or_default();
    client
        .call_tool(CallToolRequestParams::new(tool).with_arguments(args))
        .await
        .expect("call tool")
}

fn result_text(r: &CallToolResult) -> String {
    r.content
        .iter()
        .filter_map(|c| c.as_text())
        .map(|t| t.text.as_str())
        .collect()
}

/// The password in a clone URL.
fn password(url: &str) -> &str {
    let rest = url.strip_prefix("postgres://postgres:").unwrap();
    &rest[..rest.find('@').unwrap()]
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_tool_docs_cover_router_sleep_and_diff() {
    let (_serial, tmp) = serial_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);
    let client = mcp_client(home).await;
    let tools = client.list_all_tools().await.expect("list tools");
    let doc = |name: &str| {
        tools
            .iter()
            .find(|t| t.name == name)
            .and_then(|t| t.description.as_deref())
            .unwrap_or_else(|| panic!("no description for {name}"))
            .to_string()
    };
    let create = doc("clone_create");
    for word in ["router", "sleep", "direct_url", "branch", "clone_diff"] {
        assert!(
            create.contains(word),
            "clone_create does not mention {word}: {create}"
        );
    }
    assert!(doc("clone_list").contains("sleeping"));
    let info = client.peer_info().expect("server info");
    let instructions = info.instructions.clone().unwrap_or_default();
    assert!(
        instructions.contains("router URL") && instructions.contains("clone_diff"),
        "{instructions}"
    );
    client.cancel().await.expect("stop mcp client");
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_clone_create_returns_router_url() {
    if discover().is_empty() {
        println!("skipping: no Postgres installed");
        return;
    }
    let (_serial, tmp) = serial_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);
    let TemplateSource::Seed { command, .. } = seed(SEED) else {
        unreachable!()
    };
    ok(home, &["template", "init", "--seed", &command]);

    let client = mcp_client(home).await;
    let port = router_port();

    // An unnamed clone is reached by its id, a named one by its name.
    let r = call(&client, "clone_create", json!({})).await;
    let text = result_text(&r);
    assert_ne!(r.is_error, Some(true), "{text}");
    let unnamed: CloneInfo = serde_json::from_str(&text).expect("clone_create text parses");
    assert_eq!(
        unnamed.url,
        format!(
            "postgres://postgres:{}@127.0.0.1:{port}/{}",
            password(&unnamed.url),
            unnamed.id
        )
    );
    assert!(
        unnamed
            .direct_url
            .contains(&format!("@127.0.0.1:{}/", unnamed.port)),
        "{}",
        unnamed.direct_url
    );
    assert_ne!(unnamed.url, unnamed.direct_url);
    assert_eq!(unnamed.status, "ready");

    let r = call(&client, "clone_create", json!({ "name": "feat" })).await;
    let named: CloneInfo = serde_json::from_str(&result_text(&r)).expect("named clone parses");
    assert!(
        named.url.ends_with(&format!(":{port}/feat")),
        "{}",
        named.url
    );

    // The listing carries the same fields.
    let r = call(&client, "clone_list", json!({})).await;
    let listed: Vec<CloneInfo> = serde_json::from_str(&result_text(&r)).expect("clone_list parses");
    let again = listed.iter().find(|c| c.id == named.id).unwrap();
    assert_eq!(again.url, named.url);
    assert_eq!(again.status, "ready");
    client.cancel().await.expect("stop mcp client");
}
