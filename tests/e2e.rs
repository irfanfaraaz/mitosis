mod common;

use common::{seed, tmp_home};
use mitosis::pg::{discover, newest};
use mitosis::protocol::{CloneInfo, TemplateSource};
use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use rmcp::{RoleClient, ServiceExt};
use serde_json::json;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const SEED: &str = "create table t(id int); insert into t values (1)";

fn mitosis(home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_mitosis"))
        .args(args)
        .env("MITOSIS_HOME", home)
        .output()
        .expect("run mitosis")
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

fn ok(home: &Path, args: &[&str]) -> String {
    let o = mitosis(home, args);
    assert!(
        o.status.success(),
        "mitosis {args:?} failed: {}",
        stderr(&o)
    );
    stdout(&o)
}

fn list(home: &Path) -> Vec<CloneInfo> {
    serde_json::from_str(&ok(home, &["list", "--json"])).expect("list --json parses")
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

#[test]
fn cli_autostarts_daemon_and_clones() {
    if discover().is_empty() {
        println!("skipping: no Postgres installed");
        return;
    }
    let tmp = tmp_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);

    let TemplateSource::Seed { command, .. } = seed(SEED) else {
        unreachable!()
    };
    ok(home, &["template", "init", "--seed", &command]);

    let out = ok(home, &["clone"]);
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 1, "clone printed more than the URL: {out:?}");
    let url = lines[0];
    assert!(url.starts_with("postgres://postgres:"), "{url}");

    let installs = discover();
    let psql = newest(&installs).unwrap().bin("psql");
    let q = Command::new(psql)
        .arg(url)
        .args([
            "-v",
            "ON_ERROR_STOP=1",
            "-X",
            "-Atc",
            "select count(*) from t",
        ])
        .env("PGCONNECT_TIMEOUT", "5")
        .output()
        .unwrap();
    assert!(q.status.success(), "psql failed: {}", stderr(&q));
    assert_eq!(stdout(&q).trim(), "1");

    let clones = list(home);
    assert_eq!(clones.len(), 1);
    assert_eq!(ok(home, &["destroy", &clones[0].id]).trim(), clones[0].id);
    assert!(list(home).is_empty());

    ok(home, &["daemon", "stop"]);
    assert!(!home.join("daemon.sock").exists());
}

#[test]
fn cli_error_has_hint() {
    let tmp = tmp_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);
    let o = mitosis(home, &["clone", "--template", "nope"]);
    assert_eq!(o.status.code(), Some(1));
    let err = stderr(&o);
    assert!(err.contains("error:"), "{err}");
    assert!(err.contains("hint: mitosis template init nope"), "{err}");
    assert!(stdout(&o).is_empty());
}

#[test]
fn cli_bad_ttl() {
    let tmp = tmp_home();
    let home = tmp.path();
    let o = mitosis(home, &["clone", "--ttl", "0m"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("error:"), "{}", stderr(&o));
    assert!(!home.join("daemon.sock").exists(), "a daemon was started");
}

type McpClient = RunningService<RoleClient, ()>;

/// Starts `mitosis mcp` on `home` as a child process and completes the MCP handshake.
async fn mcp_client(home: &Path) -> McpClient {
    let cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_mitosis")).configure(|c| {
        c.arg("mcp").env("MITOSIS_HOME", home);
    });
    ().serve(TokioChildProcess::new(cmd).expect("spawn mitosis mcp"))
        .await
        .expect("mcp handshake")
}

async fn tool_names(client: &McpClient) -> Vec<String> {
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

#[tokio::test(flavor = "multi_thread")]
async fn mcp_lists_three_tools() {
    let tmp = tmp_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);
    let client = mcp_client(home).await;
    assert_eq!(
        tool_names(&client).await,
        ["clone_create", "clone_destroy", "clone_list"]
    );
    client.cancel().await.expect("stop mcp client");
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_error_has_hint() {
    let tmp = tmp_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);
    let client = mcp_client(home).await;
    let r = call(&client, "clone_create", json!({"template": "nope"})).await;
    let text = result_text(&r);
    assert_eq!(r.is_error, Some(true), "{text}");
    assert!(text.contains("hint:"), "{text}");
    client.cancel().await.expect("stop mcp client");
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_tools_roundtrip() {
    if discover().is_empty() {
        println!("skipping: no Postgres installed");
        return;
    }
    let tmp = tmp_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);

    let TemplateSource::Seed { command, .. } = seed(SEED) else {
        unreachable!()
    };
    ok(home, &["template", "init", "--seed", &command]);

    let client = mcp_client(home).await;
    assert_eq!(
        tool_names(&client).await,
        ["clone_create", "clone_destroy", "clone_list"]
    );

    let r = call(&client, "clone_create", json!({})).await;
    let text = result_text(&r);
    assert_ne!(r.is_error, Some(true), "{text}");
    let info: CloneInfo =
        serde_json::from_str(&text).expect("clone_create text parses as CloneInfo");
    let extra: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(extra["template_age"].is_string(), "{text}");
    assert!(extra["warnings"].is_array(), "{text}");

    let installs = discover();
    let psql = newest(&installs).unwrap().bin("psql");
    let q = Command::new(psql)
        .arg(&info.url)
        .args(["-v", "ON_ERROR_STOP=1", "-X", "-Atc", "select 1"])
        .env("PGCONNECT_TIMEOUT", "5")
        .output()
        .unwrap();
    assert!(q.status.success(), "psql failed: {}", stderr(&q));
    assert_eq!(stdout(&q).trim(), "1");

    let r = call(&client, "clone_list", json!({})).await;
    let clones: Vec<CloneInfo> = serde_json::from_str(&result_text(&r)).expect("clone_list parses");
    assert_eq!(clones.len(), 1);
    assert_eq!(clones[0].id, info.id);

    // Ending the session ends `mitosis mcp`, the clone's parent, so the daemon destroys the clone.
    client.cancel().await.expect("stop mcp client");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !list(home).is_empty() {
        assert!(
            Instant::now() < deadline,
            "clone outlived its mcp session by 5 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
