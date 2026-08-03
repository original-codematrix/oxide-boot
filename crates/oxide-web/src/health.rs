//! Actuator-style health endpoints (`/actuator/health*`).

use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use oxide_core::Context;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The health timeout applied to each registered check, per request. A
/// check that doesn't resolve within this window counts as `Down` with
/// detail `"timed out"`.
const CHECK_TIMEOUT: Duration = Duration::from_secs(2);

/// Outcome of a single health check, in the Spring Actuator vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    Up,
    Down { detail: String },
}

/// A health check resolved and moved into a closure, ready to be polled
/// per request. See [`HealthIndicator`] for how these come to exist.
pub type RegisteredCheck = Box<dyn Fn() -> futures::future::BoxFuture<'static, HealthStatus> + Send + Sync>;

/// One health indicator contributed to `/actuator/health`'s aggregation
/// (usually via `inventory::submit!`).
///
/// Contract: at management-router build time, `register` is called ONCE
/// with the application `&Context`. It resolves whatever `Inject` handles
/// it needs and moves them into the returned closure. Returning `None`
/// means the indicator isn't active in this app (e.g. the component it
/// checks isn't in the context) — it is simply skipped, not treated as
/// Down. The registered closure itself then runs fresh on every
/// `/actuator/health` request, wrapped in a timeout.
pub struct HealthIndicator {
    pub name: &'static str,
    pub register: fn(&Context) -> Option<RegisteredCheck>,
}

inventory::collect!(HealthIndicator);

/// Resolves every registered `HealthIndicator` against `ctx`, once, at
/// management-router build time. Indicators whose `register` returns `None`
/// are skipped.
pub(crate) fn resolve_indicators(ctx: &Context) -> Vec<(&'static str, RegisteredCheck)> {
    inventory::iter::<HealthIndicator>()
        .filter_map(|indicator| (indicator.register)(ctx).map(|check| (indicator.name, check)))
        .collect()
}

fn status_body(up: bool) -> (StatusCode, Json<serde_json::Value>) {
    if up {
        (StatusCode::OK, Json(serde_json::json!({ "status": "UP" })))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({ "status": "OUT_OF_SERVICE" })))
    }
}

/// Runs every registered check concurrently (each wrapped in a 2s timeout),
/// aggregates them with `readiness_up`, and renders the Spring-Actuator-style
/// body: overall UP only if readiness is UP AND every indicator is Up.
async fn aggregated_health_body(
    readiness_up: bool,
    checks: &[(&'static str, RegisteredCheck)],
) -> (StatusCode, Json<serde_json::Value>) {
    let results = futures::future::join_all(checks.iter().map(|(name, check)| async move {
        let status = match tokio::time::timeout(CHECK_TIMEOUT, check()).await {
            Ok(status) => status,
            Err(_) => HealthStatus::Down { detail: "timed out".to_string() },
        };
        (*name, status)
    }))
    .await;

    let all_indicators_up = results.iter().all(|(_, status)| matches!(status, HealthStatus::Up));
    let (status_code, Json(mut body)) = status_body(readiness_up && all_indicators_up);

    let components: serde_json::Map<String, serde_json::Value> = results
        .into_iter()
        .map(|(name, status)| {
            let component = match status {
                HealthStatus::Up => serde_json::json!({ "status": "UP" }),
                HealthStatus::Down { detail } => serde_json::json!({ "status": "DOWN", "detail": detail }),
            };
            (name.to_string(), component)
        })
        .collect();
    body["components"] = serde_json::Value::Object(components);

    (status_code, Json(body))
}

/// Builds the health routes. `/actuator/health` aggregates the given
/// readiness flag with every resolved indicator `check` (Spring-Actuator
/// `components` shape); `/actuator/health/readiness` reflects only the
/// readiness flag; `/actuator/health/liveness` always reports UP.
pub(crate) fn health_routes(readiness: Arc<AtomicBool>, checks: Vec<(&'static str, RegisteredCheck)>) -> Router {
    let checks = Arc::new(checks);
    let r1 = Arc::clone(&readiness);
    Router::new()
        .route(
            "/actuator/health",
            get(move || {
                let readiness = Arc::clone(&r1);
                let checks = Arc::clone(&checks);
                async move { aggregated_health_body(readiness.load(Ordering::SeqCst), &checks).await }
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
        let router = health_routes(readiness, Vec::new());

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
