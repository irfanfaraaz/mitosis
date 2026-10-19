use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    NotApfs,
    CrossVolume,
    NoMatchingPostgres,
    MissingExtension,
    TemplateNotFound,
    TemplateBuildFailed,
    CloneNotFound,
    CapReached,
    LowDisk,
    PortsExhausted,
    PortInUse,
    PostgresStartFailed,
    AlreadyRunning,
    DaemonUnavailable,
    ProtocolMismatch,
    InvalidRequest,
    Internal,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MitosisError {
    pub code: ErrorCode,
    pub message: String,
    pub hint: Option<String>,
}

impl MitosisError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        MitosisError {
            code,
            message: message.into(),
            hint: None,
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

impl fmt::Display for MitosisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(hint) = &self.hint {
            write!(f, "\nhint: {hint}")?;
        }
        Ok(())
    }
}

impl std::error::Error for MitosisError {}

impl From<std::io::Error> for MitosisError {
    fn from(e: std::io::Error) -> Self {
        MitosisError::new(ErrorCode::Internal, e.to_string())
    }
}

impl From<rusqlite::Error> for MitosisError {
    fn from(e: rusqlite::Error) -> Self {
        MitosisError::new(ErrorCode::Internal, e.to_string())
    }
}

impl From<serde_json::Error> for MitosisError {
    fn from(e: serde_json::Error) -> Self {
        MitosisError::new(ErrorCode::Internal, e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, MitosisError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_with_and_without_hint() {
        let e = MitosisError::new(ErrorCode::Internal, "boom");
        assert_eq!(e.to_string(), "boom");
        assert_eq!(
            e.with_hint("try again").to_string(),
            "boom\nhint: try again"
        );
    }

    #[test]
    fn code_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&ErrorCode::NoMatchingPostgres).unwrap(),
            "\"no_matching_postgres\""
        );
    }
}
