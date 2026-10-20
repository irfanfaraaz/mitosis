//! The router: one TCP port for every clone, the clone picked by the database name of the startup
//! message. Auth stays end to end: the router forwards the client's startup message (with the
//! database renamed) to the clone's TCP port and then copies bytes. It never connects over the
//! clone's Unix socket, which trusts every local user. It never logs message bodies.

pub mod wire;

use crate::clones;
use crate::ctx::Ctx;
use crate::events;
use crate::lifecycle::log;
use crate::names::format_duration;
use crate::protocol::RouterStatus;
use crate::state::{CloneRow, CloneStatus};
use crate::templates::now;
use serde_json::json;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use wire::FirstMessage;

/// How long a client may take to send its startup message (Postgres' `authentication_timeout`).
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Between bind attempts while the port is taken; `MITOSIS_ROUTER_RETRY_MS` shortens it in tests.
const RETRY_MS: u64 = 60_000;

/// What the router knows about open connections; read through `Ctx::router`.
pub struct RouterState {
    /// Clone id → open router connections.
    pub conns: HashMap<String, usize>,
    /// Clone id → unix time of the last router connect or disconnect.
    pub last_active: HashMap<String, i64>,
    /// `(pid, secret key)` from `BackendKeyData` → clone id, for cancel requests.
    pub cancel: HashMap<(i32, Vec<u8>), String>,
    pub status: RouterStatus,
    /// Connection number → (clone id, notified to close that connection).
    closers: HashMap<u64, (String, Arc<Notify>)>,
    next_conn: u64,
}

impl RouterState {
    pub fn new(port: u16) -> RouterState {
        RouterState {
            conns: HashMap::new(),
            last_active: HashMap::new(),
            cancel: HashMap::new(),
            status: RouterStatus {
                listening: false,
                listen: String::new(),
                port,
                error: None,
            },
            closers: HashMap::new(),
            next_conn: 0,
        }
    }

    pub fn open_conns(&self, id: &str) -> usize {
        self.conns.get(id).copied().unwrap_or(0)
    }

    pub fn last_active(&self, id: &str) -> Option<i64> {
        self.last_active.get(id).copied()
    }

    /// Closes every client connection to clone `id` (on destroy or sleep).
    pub fn close_clone(&mut self, id: &str) {
        for (clone, close) in self.closers.values() {
            if clone == id {
                // A stored permit: the connection sees it even if it is not waiting yet.
                close.notify_one();
            }
        }
    }
}

/// The warning `clone` responses carry while the router is on but not listening.
pub fn warning(ctx: &Ctx) -> Option<String> {
    if !ctx.config().router {
        return None;
    }
    let error = ctx.router().status.error.clone()?;
    Some(format!(
        "the router is not listening ({error}); it retries every minute. Until then use the \
         direct URL (mitosis env <clone> --direct, or direct_url)"
    ))
}

fn retry_interval() -> Duration {
    let ms = std::env::var("MITOSIS_ROUTER_RETRY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(RETRY_MS);
    Duration::from_millis(ms)
}

/// Binds `router_listen:router_port` and serves connections. While the bind fails the status
/// holds the error, `router.failed` is recorded once, and the bind is retried every minute.
/// Abort the handle to stop accepting; open connections keep going.
pub fn spawn(ctx: Arc<Ctx>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let cfg = ctx.config();
        let addr = SocketAddr::new(cfg.router_listen, cfg.router_port);
        let retry = retry_interval();
        let listener = loop {
            match TcpListener::bind(addr).await {
                Ok(l) => break l,
                Err(e) => {
                    let error = format!("cannot listen on {addr}: {e}");
                    let first = {
                        let mut r = ctx.router();
                        let first = r.status.error.as_deref() != Some(error.as_str());
                        r.status = RouterStatus {
                            listening: false,
                            listen: addr.ip().to_string(),
                            port: addr.port(),
                            error: Some(error.clone()),
                        };
                        first
                    };
                    if first {
                        log(
                            &ctx,
                            &format!("router: {error}; retrying every {}", format_duration(retry)),
                        );
                        events::emit(
                            &ctx,
                            events::ROUTER_FAILED,
                            None,
                            None,
                            json!({ "listen": addr.to_string(), "error": e.to_string() }),
                        );
                    }
                    tokio::time::sleep(retry).await;
                }
            }
        };
        ctx.router().status = RouterStatus {
            listening: true,
            listen: addr.ip().to_string(),
            port: addr.port(),
            error: None,
        };
        log(&ctx, &format!("router listening on {addr}"));
        events::emit(
            &ctx,
            events::ROUTER_STARTED,
            None,
            None,
            json!({ "listen": addr.to_string() }),
        );
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    tokio::spawn(connection(ctx.clone(), stream));
                }
                // EMFILE and the like: back off instead of spinning.
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
}

/// One client connection; errors only end it (a client hanging up is normal).
async fn connection(ctx: Arc<Ctx>, mut client: TcpStream) {
    let _ = client.set_nodelay(true);
    let first = match tokio::time::timeout(STARTUP_TIMEOUT, first_message(&mut client)).await {
        Ok(Ok(m)) => m,
        _ => return,
    };
    match first {
        FirstMessage::Ssl | FirstMessage::GssEnc => {}
        FirstMessage::DirectTls => log(
            &ctx,
            "router: closed a direct TLS connection (no TLS through the router; use sslmode=disable or prefer)",
        ),
        FirstMessage::Cancel { pid, key } => {
            let _ = cancel(&ctx, pid, key).await;
        }
        FirstMessage::Startup {
            major,
            minor,
            params,
        } => {
            let _ = startup(ctx, client, major, minor, params).await;
        }
    }
}

/// The first message after at most one SSL and one GSS refusal (`N`, no TLS through the router).
async fn first_message(client: &mut TcpStream) -> io::Result<FirstMessage> {
    let mut first = wire::read_first(client).await?;
    for _ in 0..2 {
        if !matches!(first, FirstMessage::Ssl | FirstMessage::GssEnc) {
            break;
        }
        client.write_all(b"N").await?;
        first = wire::read_first(client).await?;
    }
    Ok(first)
}

/// Sends a cancel request on to the clone whose backend handed out `(pid, key)`; unknown keys are
/// dropped silently, as Postgres does.
async fn cancel(ctx: &Ctx, pid: i32, key: Vec<u8>) -> io::Result<()> {
    let Some(id) = ctx.router().cancel.get(&(pid, key.clone())).cloned() else {
        return Ok(());
    };
    let Some(port) = ctx.store().get_clone(&id).ok().flatten().map(|r| r.port) else {
        return Ok(());
    };
    let mut server =
        tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(("127.0.0.1", port))).await??;
    server.write_all(&wire::cancel_bytes(pid, &key)).await?;
    // Postgres closes once it has read the request; waiting for that keeps it from being cut off.
    let _ = tokio::time::timeout(CONNECT_TIMEOUT, server.read(&mut [0u8; 1])).await;
    Ok(())
}

/// The clone named `db`, else the clone with id `db`.
fn lookup(ctx: &Ctx, db: &str) -> crate::error::Result<Option<CloneRow>> {
    let rows = ctx.store().list_clones()?;
    Ok(rows
        .iter()
        .find(|c| c.name.as_deref() == Some(db))
        .or_else(|| rows.iter().find(|c| c.id == db))
        .cloned())
}

async fn refuse(
    client: &mut TcpStream,
    sqlstate: &str,
    msg: &str,
    hint: Option<&str>,
) -> io::Result<()> {
    client
        .write_all(&wire::error_response(sqlstate, msg, hint))
        .await
}

/// Counts a connection to a clone while it lives; dropping it (also on a panic or abort) undoes
/// the count, records `last_active` and forgets the connection's cancel key.
struct Tracked {
    ctx: Arc<Ctx>,
    conn: u64,
    clone: String,
    key: Option<(i32, Vec<u8>)>,
}

impl Tracked {
    fn begin(ctx: &Arc<Ctx>, clone: &str) -> (Tracked, Arc<Notify>) {
        let close = Arc::new(Notify::new());
        let mut r = ctx.router();
        r.next_conn += 1;
        let conn = r.next_conn;
        r.closers.insert(conn, (clone.to_string(), close.clone()));
        *r.conns.entry(clone.to_string()).or_default() += 1;
        r.last_active.insert(clone.to_string(), now());
        drop(r);
        let tracked = Tracked {
            ctx: ctx.clone(),
            conn,
            clone: clone.to_string(),
            key: None,
        };
        (tracked, close)
    }

    fn record_key(&mut self, key: (i32, Vec<u8>)) {
        let mut r = self.ctx.router();
        if let Some(old) = self.key.take() {
            r.cancel.remove(&old);
        }
        r.cancel.insert(key.clone(), self.clone.clone());
        self.key = Some(key);
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        let mut r = self.ctx.router();
        r.closers.remove(&self.conn);
        if let Some(n) = r.conns.get_mut(&self.clone) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                r.conns.remove(&self.clone);
            }
        }
        r.last_active.insert(self.clone.clone(), now());
        if let Some(k) = self.key.take() {
            r.cancel.remove(&k);
        }
    }
}

async fn startup(
    ctx: Arc<Ctx>,
    mut client: TcpStream,
    major: u16,
    minor: u16,
    mut params: Vec<(String, String)>,
) -> io::Result<()> {
    if major != 3 {
        let msg = format!("unsupported frontend protocol {major}.{minor}: the router speaks 3.x");
        return refuse(&mut client, "0A000", &msg, None).await;
    }
    let param = |name: &str| {
        params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    // Postgres' rule: no database means the user name.
    let db = param("database")
        .filter(|d| !d.is_empty())
        .or_else(|| param("user"))
        .unwrap_or_default();
    let row = match lookup(&ctx, &db) {
        Ok(Some(row)) => row,
        Ok(None) => {
            let hint = format!(
                "connect as postgres://postgres:<password>@127.0.0.1:{}/<clone name or id>",
                ctx.config().router_port
            );
            let msg = format!("no clone named \"{db}\" (the database name picks the clone)");
            return refuse(&mut client, "3D000", &msg, Some(&hint)).await;
        }
        Err(e) => return refuse(&mut client, "58000", &e.message, None).await,
    };
    // Counted before the status check: a sleep that has not marked the clone yet sees the count
    // and backs off; one that has is seen here, and the clone is woken again.
    let mut tries = 0;
    let (row, mut tracked, close) = loop {
        tries += 1;
        let row = match clones::ensure_running(&ctx, &row.id).await {
            Ok(row) => row,
            Err(e) => return refuse(&mut client, "57P03", &e.message, e.hint.as_deref()).await,
        };
        let (tracked, close) = Tracked::begin(&ctx, &row.id);
        let same = ctx
            .store()
            .get_clone(&row.id)
            .ok()
            .flatten()
            .is_some_and(|r| r.status == CloneStatus::Ready && r.port == row.port);
        if same || tries >= 3 {
            break (row, tracked, close);
        }
    };
    match params.iter_mut().find(|(k, _)| k == "database") {
        Some((_, v)) => *v = row.database.clone(),
        None => params.push(("database".into(), row.database.clone())),
    }
    let label = row.name.as_deref().unwrap_or(&row.id).to_string();
    let server =
        match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(("127.0.0.1", row.port)))
            .await
        {
            Ok(Ok(s)) => s,
            _ => {
                let msg = format!("clone {label} does not answer on its port {}", row.port);
                return refuse(&mut client, "57P03", &msg, None).await;
            }
        };
    let _ = server.set_nodelay(true);

    let (mut cr, mut cw) = client.into_split();
    let (mut sr, mut sw) = server.into_split();
    sw.write_all(&wire::startup_bytes(major, minor, &params))
        .await?;
    let up = async {
        let _ = tokio::io::copy(&mut cr, &mut sw).await;
        let _ = sw.shutdown().await;
    };
    // The server's startup messages go through one by one, so the cancel key can be read; from
    // ReadyForQuery on it is a plain copy.
    let down = async {
        loop {
            let (tag, body) = wire::read_message(&mut sr).await?;
            if tag == b'K' {
                if let Some(key) = wire::parse_backend_key(&body) {
                    tracked.record_key(key);
                }
            }
            cw.write_all(&wire::message(tag, &body)).await?;
            if tag == b'Z' {
                break;
            }
        }
        tokio::io::copy(&mut sr, &mut cw).await?;
        io::Result::Ok(())
    };
    tokio::pin!(up, down);
    // The connection ends when the server side ends (the client hanging up shuts the server's
    // write side, so the server ends next), or when the clone is closed.
    tokio::select! {
        _ = close.notified() => {}
        _ = &mut down => {}
        _ = &mut up => {
            tokio::select! {
                _ = close.notified() => {}
                _ = &mut down => {}
            }
        }
    }
    Ok(())
}
