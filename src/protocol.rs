use crate::error::MitosisError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TemplateSource {
    Url { url: String },
    Seed { command: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TemplateInitArgs {
    pub name: String,
    pub source: TemplateSource,
    pub database: Option<String>,
    pub migrations: Option<PathBuf>,
    pub max_age_secs: Option<u64>,
    pub pg_major: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CloneCreateArgs {
    pub template: String,
    pub name: Option<String>,
    pub ttl_secs: u64,
    pub durable: bool,
    pub parent_pid: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    TemplateInit(TemplateInitArgs),
    TemplateRefresh { name: String },
    TemplateList,
    CloneCreate(CloneCreateArgs),
    CloneList,
    CloneDestroy { id_or_name: String },
    Gc,
    Status,
    Shutdown,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TemplateInfo {
    pub name: String,
    pub pg_major: u32,
    pub source: TemplateSource,
    pub database: String,
    pub size_bytes: u64,
    pub built_at: i64,
    pub age_secs: u64,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CloneInfo {
    pub id: String,
    pub name: Option<String>,
    pub template: String,
    pub url: String,
    pub port: u16,
    pub socket_dir: PathBuf,
    pub template_age_secs: u64,
    pub created_at: i64,
    pub expires_at: i64,
    pub durable: bool,
    pub parent_pid: Option<i32>,
    pub rss_bytes: Option<u64>,
    pub own_disk_bytes: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GcReport {
    pub adopted: Vec<String>,
    pub destroyed: Vec<String>,
    pub orphan_folders_removed: Vec<PathBuf>,
    pub records_dropped: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DaemonStatus {
    pub pid: u32,
    pub home: PathBuf,
    pub clones: usize,
    pub templates: usize,
    pub uptime_secs: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    TemplateBuilt(TemplateInfo),
    Templates { templates: Vec<TemplateInfo> },
    CloneCreated(CloneInfo),
    Clones { clones: Vec<CloneInfo> },
    Destroyed { id: String },
    Gc(GcReport),
    Status(DaemonStatus),
    Ok,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RequestEnvelope {
    pub v: u32,
    pub request: Request,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResponseEnvelope {
    pub v: u32,
    pub result: std::result::Result<Response, MitosisError>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn gc_is_tagged_only() {
        assert_eq!(serde_json::to_string(&Request::Gc).unwrap(), r#"{"type":"gc"}"#);
    }

    #[test]
    fn request_envelope_round_trip() {
        let env = RequestEnvelope {
            v: PROTOCOL_VERSION,
            request: Request::CloneCreate(CloneCreateArgs {
                template: "main".into(),
                name: Some("feat".into()),
                ttl_secs: 7200,
                durable: false,
                parent_pid: Some(42),
            }),
        };
        let line = serde_json::to_string(&env).unwrap();
        assert!(!line.contains('\n'));
        let back: RequestEnvelope = serde_json::from_str(&line).unwrap();
        assert_eq!(back.v, 1);
        match back.request {
            Request::CloneCreate(a) => {
                assert_eq!(a.template, "main");
                assert_eq!(a.name.as_deref(), Some("feat"));
                assert_eq!(a.ttl_secs, 7200);
                assert_eq!(a.parent_pid, Some(42));
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn err_response_round_trip() {
        let env = ResponseEnvelope {
            v: PROTOCOL_VERSION,
            result: Err(MitosisError::new(ErrorCode::CapReached, "too many").with_hint("destroy one")),
        };
        let line = serde_json::to_string(&env).unwrap();
        let back: ResponseEnvelope = serde_json::from_str(&line).unwrap();
        let e = back.result.unwrap_err();
        assert_eq!(e.code, ErrorCode::CapReached);
        assert_eq!(e.hint.as_deref(), Some("destroy one"));
    }

    fn template_info() -> TemplateInfo {
        TemplateInfo {
            name: "main".into(),
            pg_major: 16,
            source: TemplateSource::Url { url: "postgres://x".into() },
            database: "app".into(),
            size_bytes: 1024,
            built_at: 1_700_000_000,
            age_secs: 5,
            warnings: vec!["w".into()],
        }
    }

    fn clone_info() -> CloneInfo {
        CloneInfo {
            id: "abc123".into(),
            name: Some("feat".into()),
            template: "main".into(),
            url: "postgres://localhost:5500/app".into(),
            port: 5500,
            socket_dir: "/tmp/s".into(),
            template_age_secs: 5,
            created_at: 1,
            expires_at: 2,
            durable: false,
            parent_pid: Some(7),
            rss_bytes: None,
            own_disk_bytes: Some(9),
        }
    }

    fn round_trip(r: Response) {
        let env = ResponseEnvelope { v: PROTOCOL_VERSION, result: Ok(r) };
        let line = serde_json::to_string(&env).unwrap();
        let back: ResponseEnvelope = serde_json::from_str(&line).unwrap();
        assert_eq!(serde_json::to_value(&env).unwrap(), serde_json::to_value(&back).unwrap());
    }

    #[test]
    fn list_and_newtype_responses_round_trip() {
        round_trip(Response::Templates { templates: vec![template_info()] });
        round_trip(Response::Clones { clones: vec![clone_info()] });
        round_trip(Response::TemplateBuilt(template_info()));
        round_trip(Response::CloneCreated(clone_info()));
    }

    #[test]
    fn ok_response_round_trip() {
        let env = ResponseEnvelope { v: PROTOCOL_VERSION, result: Ok(Response::Ok) };
        let line = serde_json::to_string(&env).unwrap();
        let back: ResponseEnvelope = serde_json::from_str(&line).unwrap();
        assert!(matches!(back.result, Ok(Response::Ok)));
    }
}
