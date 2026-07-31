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
