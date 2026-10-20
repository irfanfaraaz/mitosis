//! A small raw Postgres client for the router tests, built on `mitosis::router::wire`: startup
//! (protocol 3.0 or 3.2), SCRAM-SHA-256 auth, a simple query and a cancel request.

use mitosis::names::random_hex;
use mitosis::router::wire;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct PgConn {
    pub stream: TcpStream,
    pub pid: i32,
    pub key: Vec<u8>,
}

/// `(SQLSTATE, message)` of an `ErrorResponse` body.
fn error_fields(body: &[u8]) -> (String, String) {
    let (mut code, mut msg) = (String::new(), String::new());
    let mut rest = body;
    while let Some((&t, tail)) = rest.split_first().filter(|(t, _)| **t != 0) {
        let end = tail.iter().position(|&c| c == 0).unwrap_or(tail.len());
        let v = String::from_utf8_lossy(&tail[..end]).into_owned();
        match t {
            b'C' => code = v,
            b'M' => msg = v,
            _ => {}
        }
        rest = &tail[(end + 1).min(tail.len())..];
    }
    (code, msg)
}

fn sha256(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}

fn hmac(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.map(|b| b ^ 0x36));
    inner.update(msg);
    let mut outer = Sha256::new();
    outer.update(k.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// PBKDF2-HMAC-SHA-256 with one output block (SCRAM's `Hi`).
fn hi(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut u = hmac(password, &[salt, &1u32.to_be_bytes()].concat());
    let mut out = u;
    for _ in 1..iterations {
        u = hmac(password, &u);
        out.iter_mut().zip(u).for_each(|(o, x)| *o ^= x);
    }
    out
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn b64_encode(b: &[u8]) -> String {
    let mut out = String::new();
    for chunk in b.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &c)| n | (u32::from(c) << (16 - 8 * i)));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(B64[(n >> (18 - 6 * i)) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn b64_decode(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let (mut n, mut bits) = (0u32, 0);
    for c in s.bytes().take_while(|&c| c != b'=') {
        let v = B64.iter().position(|&x| x == c).expect("base64 digit") as u32;
        n = (n << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((n >> bits) as u8);
        }
    }
    out
}

/// `key=value` attributes of a SCRAM message.
fn attr<'a>(msg: &'a str, key: &str) -> &'a str {
    msg.split(',')
        .find_map(|p| p.strip_prefix(key).and_then(|v| v.strip_prefix('=')))
        .unwrap_or_else(|| panic!("no {key} in {msg:?}"))
}

/// Connects as `postgres` to `database` on `port` with protocol `3.minor`, authenticates with
/// SCRAM, and reads up to ReadyForQuery. An error reads `SQLSTATE <code>: <message>`.
pub async fn connect(
    port: u16,
    database: &str,
    password: &str,
    minor: u16,
) -> Result<PgConn, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let params = [
        ("user".to_string(), "postgres".to_string()),
        ("database".to_string(), database.to_string()),
    ];
    let io = |e: std::io::Error| format!("io: {e}");
    stream
        .write_all(&wire::startup_bytes(3, minor, &params))
        .await
        .map_err(io)?;
    let nonce = random_hex(12);
    let client_first_bare = format!("n=,r={nonce}");
    let mut backend = None;
    loop {
        let (tag, body) = wire::read_message(&mut stream).await.map_err(io)?;
        match tag {
            b'R' => match i32::from_be_bytes(body[..4].try_into().unwrap()) {
                0 | 12 => {}
                10 => {
                    let first = format!("n,,{client_first_bare}");
                    let mut b = b"SCRAM-SHA-256\0".to_vec();
                    b.extend_from_slice(&(first.len() as i32).to_be_bytes());
                    b.extend_from_slice(first.as_bytes());
                    stream
                        .write_all(&wire::message(b'p', &b))
                        .await
                        .map_err(io)?;
                }
                11 => {
                    let server_first = String::from_utf8_lossy(&body[4..]).into_owned();
                    let r = attr(&server_first, "r");
                    assert!(r.starts_with(&nonce), "server nonce does not extend ours");
                    let salt = b64_decode(attr(&server_first, "s"));
                    let iterations: u32 = attr(&server_first, "i").parse().unwrap();
                    let salted = hi(password.as_bytes(), &salt, iterations);
                    let client_key = hmac(&salted, b"Client Key");
                    let without_proof = format!("c=biws,r={r}");
                    let auth_message =
                        format!("{client_first_bare},{server_first},{without_proof}");
                    let sig = hmac(&sha256(&client_key), auth_message.as_bytes());
                    let proof: Vec<u8> = client_key.iter().zip(sig).map(|(a, b)| a ^ b).collect();
                    let fin = format!("{without_proof},p={}", b64_encode(&proof));
                    stream
                        .write_all(&wire::message(b'p', fin.as_bytes()))
                        .await
                        .map_err(io)?;
                }
                other => return Err(format!("unexpected auth request {other}")),
            },
            b'K' => backend = wire::parse_backend_key(&body),
            b'E' => {
                let (code, msg) = error_fields(&body);
                return Err(format!("SQLSTATE {code}: {msg}"));
            }
            b'Z' => break,
            _ => {} // ParameterStatus, NegotiateProtocolVersion, notices
        }
    }
    let (pid, key) = backend.ok_or("no BackendKeyData")?;
    Ok(PgConn { stream, pid, key })
}

/// Runs `sql` with the simple query protocol; `Err((sqlstate, message))` on an error.
pub async fn query(conn: &mut PgConn, sql: &str) -> Result<(), (String, String)> {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    let io = |e: std::io::Error| ("io".to_string(), e.to_string());
    conn.stream
        .write_all(&wire::message(b'Q', &body))
        .await
        .map_err(io)?;
    let mut err = None;
    loop {
        let (tag, body) = wire::read_message(&mut conn.stream).await.map_err(io)?;
        match tag {
            b'E' => err = Some(error_fields(&body)),
            b'Z' => return err.map_or(Ok(()), Err),
            _ => {}
        }
    }
}

/// Sends a cancel request for `(pid, key)` to `port` and waits for the server to close.
pub async fn cancel(port: u16, pid: i32, key: &[u8]) {
    let mut s = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect for cancel");
    s.write_all(&wire::cancel_bytes(pid, key))
        .await
        .expect("send cancel");
    let _ = s.read(&mut [0u8; 1]).await;
}

#[test]
fn base64_round_trips() {
    for b in [&b""[..], b"f", b"fo", b"foo", b"foob", b"fooba", b"foobar"] {
        assert_eq!(b64_decode(&b64_encode(b)), b);
    }
    assert_eq!(b64_encode(b"foobar"), "Zm9vYmFy");
    assert_eq!(b64_encode(b"fo"), "Zm8=");
}

#[test]
fn hmac_matches_rfc4231() {
    // RFC 4231 test case 2.
    let mac = hmac(b"Jefe", b"what do ya want for nothing?");
    let hex: String = mac.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(
        hex,
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
}
