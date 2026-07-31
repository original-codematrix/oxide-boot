//! Actuator-style health endpoints (`/actuator/health*`).

use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn status_body(up: bool) -> (StatusCode, Json<serde_json::Value>) {
    if up {
        (StatusCode::OK, Json(serde_json::json!({ "status": "UP" })))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({ "status": "OUT_OF_SERVICE" })))
    }
}

/// Builds the health routes. `/actuator/health` and `/actuator/health/readiness`
/// reflect the given readiness flag; `/actuator/health/liveness` always reports UP.
pub(crate) fn health_routes(readiness: Arc<AtomicBool>) -> Router {
    let r1 = Arc::clone(&readiness);
    Router::new()
        .route(
            "/actuator/health",
            get(move || {
                let r = Arc::clone(&r1);
                async move { status_body(r.load(Ordering::SeqCst)) }
            }),
        )
        .route("/actuator/health/liveness", get(|| async { status_body(true) }))
        .route(
            "/actuator/health/readiness",
            get(move || {
                let r = Arc::clone(&readiness);
                async move { status_body(r.load(Ordering::SeqCst)) }
            }),
        )
}

/// Health routes for a context with no `WebServer` readiness source to
/// report on (e.g. the management server running standalone). Only
/// liveness is exposed — there is nothing to be "ready" or "not ready" for.
pub(crate) fn liveness_only_routes() -> Router {
    Router::new().route("/actuator/health/liveness", get(|| async { status_body(true) }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn readiness_false_yields_503_out_of_service() {
        let readiness = Arc::new(AtomicBool::new(false));
        let router = health_routes(readiness);

        let request = Request::builder()
            .uri("/actuator/health/readiness")
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "OUT_OF_SERVICE");
    }
}
