//! Actuator-style health endpoints (`/actuator/health*`).

use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use oxide_core::Context;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

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

/// Wraps an async health check into the `Box`+`Pin` shape `RegisteredCheck`
/// requires. A `HealthIndicator::register` implementation typically resolves
/// its dependencies from `Context` once, then returns `Some(simple_indicator
/// (move || { ... }))` — this helper is just the boxing/pinning boilerplate
/// that was otherwise duplicated at every call site (`oxide-kafka` and
/// `oxide-mongo`'s health modules).
pub fn simple_indicator<F, Fut>(check: F) -> RegisteredCheck
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HealthStatus> + Send + 'static,
{
    Box::new(move || Box::pin(check()))
}

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

/// The rendered result of running every indicator once: whether all of them
/// were Up, and the per-component JSON to splice into the response body.
/// Cached so concurrent/rapid `/actuator/health` requests within the TTL
/// don't each re-run every indicator.
#[derive(Clone)]
struct CachedIndicators {
    all_up: bool,
    components: serde_json::Map<String, serde_json::Value>,
}

/// Shared cache of the last indicator run, keyed by nothing (one aggregator
/// per health-routes instance): the instant it was recorded, and the
/// rendered result. `None` until the first request.
type HealthCache = Mutex<Option<(Instant, CachedIndicators)>>;

/// Runs every registered check concurrently (each wrapped in a 2s timeout)
/// and renders the Spring-Actuator-style `components` map. `detail` is
/// included on `DOWN` components only when `show_details` is true —
/// otherwise driver error text (hosts, URIs) never reaches the response.
async fn run_indicators(checks: &[(&'static str, RegisteredCheck)], show_details: bool) -> CachedIndicators {
    let results = futures::future::join_all(checks.iter().map(|(name, check)| async move {
        let status = match tokio::time::timeout(CHECK_TIMEOUT, check()).await {
            Ok(status) => status,
            Err(_) => HealthStatus::Down { detail: "timed out".to_string() },
        };
        (*name, status)
    }))
    .await;

    let all_up = results.iter().all(|(_, status)| matches!(status, HealthStatus::Up));

    let components = results
        .into_iter()
        .map(|(name, status)| {
            let component = match status {
                HealthStatus::Up => serde_json::json!({ "status": "UP" }),
                HealthStatus::Down { detail } if show_details => {
                    serde_json::json!({ "status": "DOWN", "detail": detail })
                }
                HealthStatus::Down { .. } => serde_json::json!({ "status": "DOWN" }),
            };
            (name.to_string(), component)
        })
        .collect();

    CachedIndicators { all_up, components }
}

/// Aggregates `readiness_up` with the indicator results, rendering the
/// Spring-Actuator-style body: overall UP only if readiness is UP AND every
/// indicator is Up. The indicator run itself is cached for `cache_ttl`: a
/// request inside the TTL reuses the last render instead of re-running every
/// check, and the lock is held across the (re-)run so concurrent requests on
/// a cache miss don't each trigger their own run.
async fn aggregated_health_body(
    readiness_up: bool,
    checks: &[(&'static str, RegisteredCheck)],
    cache: &HealthCache,
    cache_ttl: Duration,
    show_details: bool,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut guard = cache.lock().await;
    let cached = match &*guard {
        Some((recorded_at, cached)) if recorded_at.elapsed() < cache_ttl => cached.clone(),
        _ => {
            let fresh = run_indicators(checks, show_details).await;
            *guard = Some((Instant::now(), fresh.clone()));
            fresh
        }
    };
    drop(guard);

    let (status_code, Json(mut body)) = status_body(readiness_up && cached.all_up);
    body["components"] = serde_json::Value::Object(cached.components);

    (status_code, Json(body))
}

/// Builds the health routes. `/actuator/health` aggregates the given
/// readiness flag with every resolved indicator `check` (Spring-Actuator
/// `components` shape), caching the indicator run for `cache_ttl` and
/// including `detail` on `DOWN` components only when `show_details` is true;
/// `/actuator/health/readiness` reflects only the readiness flag;
/// `/actuator/health/liveness` always reports UP.
pub(crate) fn health_routes(
    readiness: Arc<AtomicBool>,
    checks: Vec<(&'static str, RegisteredCheck)>,
    cache_ttl: Duration,
    show_details: bool,
) -> Router {
    let checks = Arc::new(checks);
    let cache: Arc<HealthCache> = Arc::new(Mutex::new(None));
    let r1 = Arc::clone(&readiness);
    Router::new()
        .route(
            "/actuator/health",
            get(move || {
                let readiness = Arc::clone(&r1);
                let checks = Arc::clone(&checks);
                let cache = Arc::clone(&cache);
                async move {
                    aggregated_health_body(readiness.load(Ordering::SeqCst), &checks, &cache, cache_ttl, show_details)
                        .await
                }
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
        let router = health_routes(readiness, Vec::new(), Duration::from_millis(1000), false);

        let request = Request::builder().uri("/actuator/health/readiness").body(Body::empty()).unwrap();
        let response = router.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "OUT_OF_SERVICE");
    }

    #[tokio::test]
    async fn indicators_run_once_within_the_cache_ttl() {
        static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let checks: Vec<(&'static str, RegisteredCheck)> = vec![(
            "demo",
            Box::new(|| {
                CALLS.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { HealthStatus::Up })
            }),
        )];
        let cache: HealthCache = tokio::sync::Mutex::new(None);
        let ttl = Duration::from_millis(1000);

        let _ = aggregated_health_body(true, &checks, &cache, ttl, false).await;
        let _ = aggregated_health_body(true, &checks, &cache, ttl, false).await;

        assert_eq!(CALLS.load(Ordering::SeqCst), 1);
    }
}
