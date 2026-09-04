//! Typed errors from `#[http_client]` calls.

use reqwest::StatusCode;

/// An RFC 7807 problem+json body, when one could be parsed.
#[derive(Debug, Clone)]
pub struct ProblemDetail {
    pub status: Option<u16>,
    pub title: Option<String>,
    pub detail: Option<String>,
}

/// Errors produced by an `#[http_client]` request.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("failed to build request: {0}")]
    Build(String),
    #[error("HTTP {status}: {body}")]
    Http { status: StatusCode, body: String },
    #[error("failed to parse response body as JSON: {0}")]
    Json(String),
    #[error("{0}")]
    Config(String),
    /// The circuit breaker for this client is open: the request was not
    /// attempted, and no connection was opened.
    #[error("circuit breaker for client '{client}' is open; request not attempted")]
    CircuitOpen { client: String },
    #[error("client error: {0}")]
    Other(String),
}

impl ClientError {
    /// The HTTP status code, if this error came from a non-2xx response.
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            ClientError::Http { status, .. } => Some(*status),
            _ => None,
        }
    }
}
