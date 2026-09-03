use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProblemDetail {
    #[serde(rename = "type")]
    pub problem_type: String,
    pub title: String,
    pub status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
}

/// An HTTP error that renders as an RFC 7807 `application/problem+json` body.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    problem: ProblemDetail,
}

impl ApiError {
    pub fn new(status: StatusCode, title: impl Into<String>) -> Self {
        Self {
            status,
            problem: ProblemDetail {
                problem_type: "about:blank".to_string(),
                title: title.into(),
                status: status.as_u16(),
                detail: None,
                instance: None,
            },
        }
    }

    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "Not Found")
    }

    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "Bad Request").with_detail(detail)
    }

    pub fn conflict(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "Conflict").with_detail(detail)
    }

    /// Deliberately carries no detail: internal causes must not leak to clients.
    pub fn internal_server_error() -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.problem.detail = Some(detail.into());
        self
    }

    pub fn with_type(mut self, problem_type: impl Into<String>) -> Self {
        self.problem.problem_type = problem_type.into();
        self
    }

    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.problem.instance = Some(instance.into());
        self
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn problem(&self) -> &ProblemDetail {
        &self.problem
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, [(header::CONTENT_TYPE, "application/problem+json")], axum::Json(self.problem)).into_response()
    }
}

/// Implement on domain error types to use them with `?` in controller methods.
pub trait IntoProblemDetail {
    fn into_problem(self) -> ApiError;
}

impl<E: IntoProblemDetail> From<E> for ApiError {
    fn from(e: E) -> Self {
        e.into_problem()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WebError {
    #[error("cannot bind HTTP server to port {port}: {source}")]
    Bind {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "controller '{type_name}' (route '{path}') is not registered as a component — add #[component] to the struct"
    )]
    ControllerNotRegistered { type_name: &'static str, path: &'static str },
    #[error("duplicate route registered: {method} {path}")]
    DuplicateRoute { method: &'static str, path: &'static str },
    #[error("route '{path}' is reserved: '/actuator' is reserved for the framework's own endpoints")]
    ReservedPath { path: &'static str },
    #[error(
        "{name} did not drain in-flight requests within {grace_seconds}s and was forcibly aborted; \
         in-flight requests were cut off mid-flight"
    )]
    DrainAborted { name: &'static str, grace_seconds: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    #[test]
    fn problem_detail_serializes_rfc7807_shape_and_skips_empty_fields() {
        let err = ApiError::not_found();
        let json = serde_json::to_value(err.problem()).unwrap();
        assert_eq!(json["type"], "about:blank");
        assert_eq!(json["title"], "Not Found");
        assert_eq!(json["status"], 404);
        assert!(json.get("detail").is_none());
        assert!(json.get("instance").is_none());
    }

    #[test]
    fn builders_fill_optional_fields() {
        let err = ApiError::bad_request("email is malformed")
            .with_type("https://errors.example.com/validation")
            .with_instance("/api/users");
        let json = serde_json::to_value(err.problem()).unwrap();
        assert_eq!(json["status"], 400);
        assert_eq!(json["detail"], "email is malformed");
        assert_eq!(json["type"], "https://errors.example.com/validation");
        assert_eq!(json["instance"], "/api/users");
    }

    #[tokio::test]
    async fn into_response_sets_status_and_problem_json_content_type() {
        let res = ApiError::conflict("already exists").into_response();
        assert_eq!(res.status(), StatusCode::CONFLICT);
        assert_eq!(res.headers()["content-type"], "application/problem+json");
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["status"], 409);
        assert_eq!(json["detail"], "already exists");
    }

    #[test]
    fn custom_errors_convert_via_into_problem_detail() {
        enum DomainError {
            NotFound,
        }
        impl IntoProblemDetail for DomainError {
            fn into_problem(self) -> ApiError {
                match self {
                    DomainError::NotFound => ApiError::not_found(),
                }
            }
        }
        fn fallible() -> Result<(), ApiError> {
            Err(DomainError::NotFound)?
        }
        assert_eq!(fallible().unwrap_err().status(), StatusCode::NOT_FOUND);
    }
}
