// The serial_home guard only orders whole tests, each on its own runtime thread; no task in the
// same runtime ever waits on it.
#![allow(clippy::await_holding_lock)]

#[path = "common/cli.rs"]
mod cli;
mod common;

use cli::*;
use common::seed;
use mitosis::pg::{discover, newest};
use mitosis::protocol::{CloneInfo, TemplateSource};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

const SEED: &str = "create table t(id int); insert into t values (1)";

#[test]
fn cli_autostarts_daemon_and_clones() {
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
    let (_serial, tmp) = serial_home();
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
    let (_serial, tmp) = serial_home();
    let home = tmp.path();
    let o = mitosis(home, &["clone", "--ttl", "0m"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("error:"), "{}", stderr(&o));
    assert!(!home.join("daemon.sock").exists(), "a daemon was started");
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_lists_tools() {
    let (_serial, tmp) = serial_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);
    let client = mcp_client(home).await;
    assert_eq!(
        tool_names(&client).await,
        ["clone_create", "clone_destroy", "clone_diff", "clone_list"]
    );
    client.cancel().await.expect("stop mcp client");
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_error_has_hint() {
    let (_serial, tmp) = serial_home();
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
    let (_serial, tmp) = serial_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);

    let TemplateSource::Seed { command, .. } = seed(SEED) else {
        unreachable!()
    };
    ok(home, &["template", "init", "--seed", &command]);

    let client = mcp_client(home).await;
    assert_eq!(
        tool_names(&client).await,
        ["clone_create", "clone_destroy", "clone_diff", "clone_list"]
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

    // A clone made outside this session (here by the CLI) is neither listed nor destroyable.
    let other: CloneInfo =
        serde_json::from_str(&ok(home, &["clone", "--json", "--name", "not-mine"])).unwrap();

    let r = call(&client, "clone_list", json!({})).await;
    let clones: Vec<CloneInfo> = serde_json::from_str(&result_text(&r)).expect("clone_list parses");
    assert_eq!(clones.len(), 1);
    assert_eq!(clones[0].id, info.id);

    for target in [other.id.as_str(), "not-mine"] {
        let r = call(&client, "clone_destroy", json!({ "id": target })).await;
        let text = result_text(&r);
        assert_eq!(r.is_error, Some(true), "{text}");
        assert!(text.contains("not created by this session"), "{text}");
    }
    assert!(list(home).iter().any(|c| c.id == other.id));

    // clone_destroy removes this session's own clone.
    let r = call(&client, "clone_create", json!({})).await;
    let second: CloneInfo = serde_json::from_str(&result_text(&r)).expect("second clone");
    let r = call(&client, "clone_destroy", json!({ "id": second.id })).await;
    let text = result_text(&r);
    assert_ne!(r.is_error, Some(true), "{text}");
    assert_eq!(text, format!("destroyed {}", second.id));
    assert!(!list(home).iter().any(|c| c.id == second.id));

    // Ending the session ends `mitosis mcp`, the clone's parent, so the daemon destroys the clone.
    client.cancel().await.expect("stop mcp client");
    let deadline = Instant::now() + Duration::from_secs(5);
    while list(home).iter().any(|c| c.id == info.id) {
        assert!(
            Instant::now() < deadline,
            "clone outlived its mcp session by 5 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        list(home).iter().any(|c| c.id == other.id),
        "the CLI clone is not tied to the session"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_named_clone_with_env_file() {
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

    let cwd = tempfile::tempdir().unwrap();
    std::fs::write(cwd.path().join(".env"), "KEEP=1\n").unwrap();
    let client = mcp_client_in(home, Some(cwd.path())).await;
    let r = call(
        &client,
        "clone_create",
        json!({"name": "api-db", "env_file": ".env"}),
    )
    .await;
    let text = result_text(&r);
    assert_ne!(r.is_error, Some(true), "{text}");
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    let info: CloneInfo = serde_json::from_str(&text).unwrap();
    assert!(v["generation"].as_u64().unwrap() >= 1, "{text}");
    assert_eq!(v["name"], "api-db");
    let env = std::fs::read_to_string(cwd.path().join(".env")).unwrap();
    assert!(env.contains("KEEP=1"), "{env}");
    assert!(env.contains(&format!("DATABASE_URL={}", info.url)), "{env}");
    client.cancel().await.expect("stop mcp client");
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_clone_destroy_tool() {
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
    let r = call(&client, "clone_create", json!({})).await;
    let c: CloneInfo = serde_json::from_str(&result_text(&r)).expect("clone");
    assert!(list(home).iter().any(|x| x.id == c.id));
    let r = call(&client, "clone_destroy", json!({ "id": c.id })).await;
    assert_ne!(r.is_error, Some(true), "{}", result_text(&r));
    assert!(list(home).is_empty(), "clone still listed");
    client.cancel().await.expect("stop mcp client");
}

/// A daemon from release 0.1: answers every v2 request with the v1 `protocol_mismatch` error and
/// exits when it gets a v1 shutdown.
fn fake_v1_daemon(home: &Path) -> std::thread::JoinHandle<()> {
    use std::io::{BufRead, BufReader, Write};
    let sock = home.join("daemon.sock");
    let listener = std::os::unix::net::UnixListener::bind(&sock).expect("bind fake daemon");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut line = String::new();
            if BufReader::new(&stream).read_line(&mut line).unwrap_or(0) == 0 {
                continue;
            }
            if line.contains(r#""v":1"#) && line.contains("shutdown") {
                let _ = stream.write_all(b"{\"v\":1,\"result\":{\"Ok\":{\"type\":\"ok\"}}}\n");
                let _ = std::fs::remove_file(&sock);
                return; // dropping the listener refuses new connections
            }
            let reply = r#"{"v":1,"result":{"Err":{"code":"protocol_mismatch","message":"client speaks protocol 2, daemon speaks protocol 1","hint":"run the same mitosis version for the client and the daemon"}}}"#;
            let _ = stream.write_all(format!("{reply}\n").as_bytes());
        }
    })
}

#[test]
fn older_daemon_is_replaced() {
    let (_serial, tmp) = serial_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);
    let fake = fake_v1_daemon(home);

    let out = ok(home, &["list", "--json"]);
    assert_eq!(out.trim(), "[]");
    fake.join().expect("fake daemon exited on shutdown");
    assert!(
        home.join("daemon.sock").exists(),
        "a new daemon owns the socket"
    );
}

/// `top` puts the terminal back when a signal ends it, not only on `q`.
#[test]
fn top_restores_the_terminal_on_sigterm() {
    use nix::sys::termios::{tcgetattr, LocalFlags};
    use std::io::Read;
    use std::process::Stdio;
    let (_serial, tmp) = serial_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);
    let pty = nix::pty::openpty(None, None).expect("openpty");
    let slave = std::fs::File::from(pty.slave);
    let mut master = std::fs::File::from(pty.master);
    let mut child = Command::new(env!("CARGO_BIN_EXE_mitosis"))
        .arg("top")
        .env("MITOSIS_HOME", home)
        .env("MITOSIS_ROUTER_PORT", router_port())
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave.try_clone().unwrap()))
        .spawn()
        .expect("run mitosis top");
    // Drains the screen output, so `top` never blocks on a full pty.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let s = seen.clone();
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = master.read(&mut buf) {
            if n == 0 {
                break;
            }
            s.lock()
                .unwrap()
                .push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    });
    let deadline = Instant::now() + Duration::from_secs(40);
    while !seen.lock().unwrap().contains("mitosis top") {
        assert!(
            child.try_wait().unwrap().is_none(),
            "top exited: {}",
            seen.lock().unwrap()
        );
        assert!(Instant::now() < deadline, "top never drew");
        std::thread::sleep(Duration::from_millis(50));
    }
    let raw = tcgetattr(&slave).unwrap();
    assert!(
        !raw.local_flags.contains(LocalFlags::ICANON),
        "top runs raw"
    );

    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(child.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(Instant::now() < deadline, "top did not exit on SIGTERM");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(status.code(), Some(128 + 15), "{status:?}");
    let back = tcgetattr(&slave).unwrap();
    assert!(
        back.local_flags
            .contains(LocalFlags::ICANON | LocalFlags::ECHO | LocalFlags::ISIG),
        "the terminal is back to normal: {:?}",
        back.local_flags
    );
}

/// An MCP session that calls no tool starts no daemon (pending `.env` restores wait for one).
#[tokio::test(flavor = "multi_thread")]
async fn mcp_start_does_not_start_the_daemon() {
    let (_serial, tmp) = serial_home();
    let home = tmp.path();
    let _cleanup = Cleanup(home);
    let client = mcp_client(home).await;
    assert!(!tool_names(&client).await.is_empty());
    assert!(!home.join("daemon.sock").exists(), "a daemon was started");
    client.cancel().await.expect("stop mcp client");
}

/// A repeated named `clone_create` returns the running clone; `fresh` needs a name and, with one,
/// makes a new clone on the same URL.
#[tokio::test(flavor = "multi_thread")]
async fn mcp_named_repeat_and_fresh() {
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
    let create = |args: serde_json::Value| {
        let client = &client;
        async move {
            let r = call(client, "clone_create", args).await;
            (r.is_error == Some(true), result_text(&r))
        }
    };
    let (err, text) = create(json!({"name": "n"})).await;
    assert!(!err, "{text}");
    let first: CloneInfo = serde_json::from_str(&text).unwrap();
    assert!(!first.existing);
    let (_, text) = create(json!({"name": "n"})).await;
    let again: CloneInfo = serde_json::from_str(&text).unwrap();
    assert!(again.existing && again.id == first.id);

    let (err, text) = create(json!({"fresh": true})).await;
    assert!(err, "{text}");
    assert!(text.contains("--fresh needs a clone name"), "{text}");

    let (err, text) = create(json!({"name": "n", "fresh": true})).await;
    assert!(!err, "{text}");
    let fresh: CloneInfo = serde_json::from_str(&text).unwrap();
    assert!(!fresh.existing && fresh.id != first.id);
    assert_eq!(
        fresh.url, first.url,
        "same name and password: same router URL"
    );
    client.cancel().await.expect("stop mcp client");
}

/// `mitosis env --var` takes only names a .env line can hold, checked before the daemon.
#[test]
fn env_refuses_a_bad_variable_name() {
    let (_serial, tmp) = serial_home();
    let home = tmp.path();
    let o = mitosis(home, &["env", "x", "--var", "1BAD-NAME"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(
        stderr(&o).contains("not a valid variable name"),
        "{}",
        stderr(&o)
    );
    assert!(!home.join("daemon.sock").exists());
}
