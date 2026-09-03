//! Request timeout, body-size limit and optional CORS, applied as tower
//! layers driven by `ServerProps`/`ManagementProps`. Registered on the main
//! application router as a [`crate::routing::RouterLayer`] (see
//! [`inventory::submit!`] below); the management router applies the same
//! helper directly from `ManagementProps` since it doesn't go through the
//! `RouterLayer` registry.

use axum::Router;
use axum::http::{HeaderValue, StatusCode};
use std::time::Duration;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

// tower-http 0.7's `TimeoutLayer::with_status_code` responds directly with the
// given status when the inner service doesn't complete in time — no
// `HandleErrorLayer`/`ServiceBuilder` error-mapping dance needed, since it
// never turns the timeout into a tower error in the first place.

fn default_allowed_origins() -> Vec<String> {
    Vec::new()
}

/// Configuration bound to the `cors` sub-key of `oxide.server`/`oxide.management`.
/// CORS is only mounted when `allowed_origins` is non-empty.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CorsProps {
    #[serde(rename = "allowed-origins", default = "default_allowed_origins")]
    pub allowed_origins: Vec<String>,
}

impl Default for CorsProps {
    fn default() -> Self {
        Self { allowed_origins: default_allowed_origins() }
    }
}

/// Applies the request timeout, body-size limit and (if configured) CORS
/// layers to `router`. Shared by the main application router (via the
/// `RouterLayer` registered below) and the management router, which calls
/// this directly with its own `ManagementProps`-derived values.
pub(crate) fn apply(router: Router, request_timeout_seconds: u64, max_body_bytes: usize, cors: &CorsProps) -> Router {
    let router = router.layer(TimeoutLayer::with_status_code(
        StatusCode::SERVICE_UNAVAILABLE,
        Duration::from_secs(request_timeout_seconds),
    ));
    let router = router.layer(RequestBodyLimitLayer::new(max_body_bytes));

    if cors.allowed_origins.is_empty() {
        return router;
    }
    let origins: Vec<HeaderValue> = cors
        .allowed_origins
        .iter()
        .filter_map(|origin| match origin.parse::<HeaderValue>() {
            Ok(value) => Some(value),
            Err(err) => {
                tracing::warn!(origin, %err, "ignoring invalid CORS allowed-origin");
                None
            }
        })
        .collect();
    router.layer(CorsLayer::new().allow_origin(origins))
}

inventory::submit! {
    crate::routing::RouterLayer {
        name: "oxide-web/hardening",
        // `RouterLayer::apply` returns `Router`, not `Result<Router>` — there
        // is no error channel to propagate into. The `expect` below is safe
        // by construction, not merely by convention: `assemble` runs every
        // component's `construct` closure (which is where `ServerProps`,
        // itself a `#[derive(ConfigProperties)]` component, gets inserted)
        // before *any* `post_construct` hook runs, and this layer is only
        // ever invoked from `WebServer`'s `post_construct` (via
        // `routing::build_router`). So by the time this closure runs,
        // `ServerProps` construction has unconditionally already happened.
        apply: |ctx, router| {
            let props = ctx.get::<crate::server::ServerProps>().expect("ServerProps is always seeded before WebServer boots");
            apply(router, props.request_timeout_seconds, props.max_body_bytes, &props.cors)
        },
    }
}
