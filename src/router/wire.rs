//! Postgres wire protocol pieces the router needs: the untagged first message a client sends,
//! tagged messages, and the few messages the router builds itself. Pure parsing and building.

use std::io::{Error, ErrorKind, Result};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Longest first message accepted (a startup message with its parameters).
pub const MAX_FIRST: usize = 10_000;
/// Longest tagged message `read_message` accepts; only the startup exchange goes through it.
pub const MAX_MESSAGE: usize = 1 << 24;

const SSL_CODE: u32 = 80_877_103;
const GSSENC_CODE: u32 = 80_877_104;
const CANCEL_CODE: u32 = 80_877_102;
/// First byte of a TLS ClientHello (`sslnegotiation=direct`).
const TLS_HANDSHAKE: u8 = 0x16;

#[derive(Debug, PartialEq, Eq)]
pub enum FirstMessage {
    Ssl,
    GssEnc,
    Cancel {
        pid: i32,
        key: Vec<u8>,
    },
    Startup {
        major: u16,
        minor: u16,
        params: Vec<(String, String)>,
    },
    DirectTls,
}

fn bad(msg: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidData, msg.into())
}

/// Reads one length-prefixed first message (startup, SSL/GSS request or cancel).
pub async fn read_first<R: AsyncRead + Unpin>(r: &mut R) -> Result<FirstMessage> {
    let mut head = [0u8; 4];
    r.read_exact(&mut head[..1]).await?;
    // A real length would have to be over 350 MB to start with this byte.
    if head[0] == TLS_HANDSHAKE {
        return Ok(FirstMessage::DirectTls);
    }
    r.read_exact(&mut head[1..]).await?;
    let len = u32::from_be_bytes(head) as usize;
    if !(8..=MAX_FIRST).contains(&len) {
        return Err(bad(format!("first message length {len} out of range")));
    }
    let mut body = vec![0u8; len - 4];
    r.read_exact(&mut body).await?;
    parse_first(&body)
}

/// `body` is the message after its length.
fn parse_first(body: &[u8]) -> Result<FirstMessage> {
    let code = u32::from_be_bytes(body[..4].try_into().expect("4 bytes"));
    let rest = &body[4..];
    match code {
        SSL_CODE if rest.is_empty() => Ok(FirstMessage::Ssl),
        GSSENC_CODE if rest.is_empty() => Ok(FirstMessage::GssEnc),
        CANCEL_CODE if rest.len() >= 4 => {
            let pid = i32::from_be_bytes(rest[..4].try_into().expect("4 bytes"));
            let key = rest[4..].to_vec();
            if !(4..=256).contains(&key.len()) {
                return Err(bad("cancel key length out of range"));
            }
            Ok(FirstMessage::Cancel { pid, key })
        }
        _ => {
            let (major, minor) = ((code >> 16) as u16, (code & 0xffff) as u16);
            if major == 1234 {
                return Err(bad(format!("unknown request code {code}")));
            }
            Ok(FirstMessage::Startup {
                major,
                minor,
                params: parse_params(rest)?,
            })
        }
    }
}

/// `name\0value\0 ... \0`.
fn parse_params(mut b: &[u8]) -> Result<Vec<(String, String)>> {
    let cstr = |b: &mut &[u8]| -> Result<String> {
        let end = b
            .iter()
            .position(|&c| c == 0)
            .ok_or_else(|| bad("startup parameters are not terminated"))?;
        let s = String::from_utf8(b[..end].to_vec())
            .map_err(|_| bad("startup parameter is not UTF-8"))?;
        *b = &b[end + 1..];
        Ok(s)
    };
    let mut params = Vec::new();
    loop {
        let name = cstr(&mut b)?;
        if name.is_empty() {
            return if b.is_empty() {
                Ok(params)
            } else {
                Err(bad("bytes after the startup parameters"))
            };
        }
        let value = cstr(&mut b)?;
        params.push((name, value));
    }
}

fn with_len(mut b: Vec<u8>, at: usize) -> Vec<u8> {
    let len = u32::try_from(b.len() - at).expect("message under 4 GB");
    b[at..at + 4].copy_from_slice(&len.to_be_bytes());
    b
}

/// A startup message for protocol `major.minor` with `params`.
pub fn startup_bytes(major: u16, minor: u16, params: &[(String, String)]) -> Vec<u8> {
    let mut b = vec![0u8; 4];
    b.extend_from_slice(&((u32::from(major) << 16) | u32::from(minor)).to_be_bytes());
    for (k, v) in params {
        b.extend_from_slice(k.as_bytes());
        b.push(0);
        b.extend_from_slice(v.as_bytes());
        b.push(0);
    }
    b.push(0);
    with_len(b, 0)
}

/// A cancel request; its length follows the key (4 bytes in 3.0, up to 256 in 3.2).
pub fn cancel_bytes(pid: i32, key: &[u8]) -> Vec<u8> {
    let mut b = vec![0u8; 4];
    b.extend_from_slice(&CANCEL_CODE.to_be_bytes());
    b.extend_from_slice(&pid.to_be_bytes());
    b.extend_from_slice(key);
    with_len(b, 0)
}

/// A tagged message: `tag`, length, `body`.
pub fn message(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut b = vec![tag, 0, 0, 0, 0];
    b.extend_from_slice(body);
    with_len(b, 1)
}

/// Reads one tagged message; returns its tag and body.
pub async fn read_message<R: AsyncRead + Unpin>(r: &mut R) -> Result<(u8, Vec<u8>)> {
    let mut head = [0u8; 5];
    r.read_exact(&mut head).await?;
    let len = u32::from_be_bytes(head[1..].try_into().expect("4 bytes")) as usize;
    if !(4..=MAX_MESSAGE).contains(&len) {
        return Err(bad(format!("message length {len} out of range")));
    }
    let mut body = vec![0u8; len - 4];
    r.read_exact(&mut body).await?;
    Ok((head[0], body))
}

/// A FATAL `ErrorResponse` with SQLSTATE `sqlstate`.
pub fn error_response(sqlstate: &str, message_text: &str, hint: Option<&str>) -> Vec<u8> {
    let mut body = Vec::new();
    let mut field = |code: u8, value: &str| {
        body.push(code);
        // A NUL would end the field early.
        body.extend(value.bytes().filter(|&c| c != 0));
        body.push(0);
    };
    field(b'S', "FATAL");
    field(b'V', "FATAL");
    field(b'C', sqlstate);
    field(b'M', message_text);
    if let Some(h) = hint {
        field(b'H', h);
    }
    body.push(0);
    message(b'E', &body)
}

/// The process id and secret key of a `BackendKeyData` body.
pub fn parse_backend_key(body: &[u8]) -> Option<(i32, Vec<u8>)> {
    let pid = i32::from_be_bytes(body.get(..4)?.try_into().ok()?);
    let key = &body[4..];
    (4..=256).contains(&key.len()).then(|| (pid, key.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn first(b: &[u8]) -> Result<FirstMessage> {
        read_first(&mut &b[..]).await
    }

    fn params() -> Vec<(String, String)> {
        vec![
            ("user".into(), "postgres".into()),
            ("database".into(), "feat".into()),
        ]
    }

    #[tokio::test]
    async fn parses_startup_30_and_32() {
        for minor in [0, 2] {
            let b = startup_bytes(3, minor, &params());
            assert_eq!(
                u32::from_be_bytes(b[..4].try_into().unwrap()) as usize,
                b.len()
            );
            assert_eq!(
                first(&b).await.unwrap(),
                FirstMessage::Startup {
                    major: 3,
                    minor,
                    params: params()
                }
            );
        }
        // Unterminated parameters.
        let mut b = startup_bytes(3, 0, &params());
        b.pop();
        let len = (b.len() as u32).to_be_bytes();
        b[..4].copy_from_slice(&len);
        assert!(first(&b).await.is_err());
    }

    #[tokio::test]
    async fn parses_ssl_gss_cancel_3_0_and_3_2() {
        let req = |code: u32| {
            let mut b = 8u32.to_be_bytes().to_vec();
            b.extend_from_slice(&code.to_be_bytes());
            b
        };
        assert_eq!(first(&req(SSL_CODE)).await.unwrap(), FirstMessage::Ssl);
        assert_eq!(
            first(&req(GSSENC_CODE)).await.unwrap(),
            FirstMessage::GssEnc
        );
        for key in [vec![1, 2, 3, 4], (0..32).collect::<Vec<u8>>()] {
            let b = cancel_bytes(77, &key);
            assert_eq!(b.len(), 12 + key.len());
            assert_eq!(
                first(&b).await.unwrap(),
                FirstMessage::Cancel {
                    pid: 77,
                    key: key.clone()
                }
            );
        }
        assert!(first(&cancel_bytes(1, &[1, 2])).await.is_err());
        assert_eq!(
            first(&[0x16, 3, 1, 0, 0]).await.unwrap(),
            FirstMessage::DirectTls
        );
        assert!(first(&req((1234 << 16) | 9999)).await.is_err());
    }

    #[tokio::test]
    async fn rejects_oversized_first_message() {
        let mut b = 10_001u32.to_be_bytes().to_vec();
        b.extend(vec![0u8; 10_000]);
        assert!(first(&b).await.is_err());
        assert!(first(&4u32.to_be_bytes()).await.is_err());
        // Exactly the limit is fine when well formed.
        let big = vec![("x".to_string(), "y".repeat(MAX_FIRST - 12))];
        let b = startup_bytes(3, 0, &big);
        assert_eq!(b.len(), MAX_FIRST);
        assert!(first(&b).await.is_ok());
    }

    #[test]
    fn backend_key_4_and_32_bytes() {
        for n in [4usize, 32, 256] {
            let mut body = 42i32.to_be_bytes().to_vec();
            body.extend(vec![7u8; n]);
            assert_eq!(parse_backend_key(&body), Some((42, vec![7u8; n])));
        }
        for n in [0usize, 3, 257] {
            let mut body = 42i32.to_be_bytes().to_vec();
            body.extend(vec![7u8; n]);
            assert_eq!(parse_backend_key(&body), None);
        }
        assert_eq!(parse_backend_key(&[1, 2]), None);
    }

    #[tokio::test]
    async fn error_response_shape() {
        let b = error_response("3D000", "no clone named \"x\"", Some("connect as y"));
        let (tag, body) = read_message(&mut &b[..]).await.unwrap();
        assert_eq!(tag, b'E');
        let mut fields = Vec::new();
        let mut rest = &body[..];
        while rest[0] != 0 {
            let end = rest.iter().position(|&c| c == 0).unwrap();
            fields.push((rest[0], String::from_utf8(rest[1..end].to_vec()).unwrap()));
            rest = &rest[end + 1..];
        }
        assert_eq!(rest, [0]);
        assert_eq!(
            fields,
            vec![
                (b'S', "FATAL".into()),
                (b'V', "FATAL".into()),
                (b'C', "3D000".into()),
                (b'M', "no clone named \"x\"".into()),
                (b'H', "connect as y".into()),
            ]
        );
        let b = error_response("57P03", "m", None);
        assert!(!b.contains(&b'H'));
    }
}
