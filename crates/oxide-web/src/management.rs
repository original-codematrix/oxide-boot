//! `ManagementProps` configuration and the `ManagementServer` lifecycle
//! component: a second HTTP listener, separate from the main `WebServer`,
//! that carries the framework's own operational endpoints (health, info,
//! and whatever else registers a [`ManagementRoute`]) off the application's
//! main port.

use crate::serve::HttpServing;
use crate::{WebError, health};
use axum::{Json, Router};
use oxide_core::{AppConfig, ComponentDescriptor, ComponentResult, Context};
use std::net::SocketAddr;
use std::time::Duration;

fn default_port() -> u16 {
    8081
}
fn default_enabled() -> bool {
    true
}
fn default_address() -> String {
    "0.0.0.0".to_string()
}
fn default_exposure() -> String {
    "health,info".to_string()
}

/// Drain grace period for the management server. Management endpoints
/// (health, info, metrics) don't hold long-running requests, so a short,
/// fixed grace is enough — unlike the main server this isn't user-configurable.
const MANAGEMENT_SHUTDOWN_GRACE_SECONDS: u64 = 5;

/// Configuration bound to the `oxide.management` prefix. The section may be
/// absent entirely (falls back to `Default`).
#[derive(Debug, Clone, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.management", default)]
pub struct ManagementProps {
    #[serde(default = "default_address")]
    pub address: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(rename = "endpoints", default)]
    pub endpoints: EndpointsProps,
}

impl Default for ManagementProps {
    fn default() -> Self {
        Self {
            address: default_address(),
            port: default_port(),
            enabled: default_enabled(),
            endpoints: EndpointsProps::default(),
        }
    }
}

/// Which actuator endpoints get mounted, bound to `oxide.management.endpoints`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct EndpointsProps {
    #[serde(default = "default_exposure")]
    pub exposure: String,
}

impl Default for EndpointsProps {
    fn default() -> Self {
        Self { exposure: default_exposure() }
    }
}

/// Whether `endpoint` (e.g. `"health"`, `"info"`, `"prometheus"`) is
/// mounted, per `exposure` (a comma-separated list, or `"*"` for
/// everything). Entries are trimmed before comparison.
pub fn is_exposed(exposure: &str, endpoint: &str) -> bool {
    let exposure = exposure.trim();
    if exposure == "*" {
        return true;
    }
    exposure.split(',').map(str::trim).any(|entry| entry == endpoint)
}

/// One route contributed to the management server by another part of the
/// framework (e.g. `/actuator/prometheus` in Task 7). `register` resolves
/// the owning component from the context ONCE at router build time, same
/// contract as `RouteDescriptor`.
pub struct ManagementRoute {
    pub path: &'static str,
    /// The actuator endpoint name (e.g. `"health"`, `"info"`, `"prometheus"`)
    /// this route belongs to, checked against `oxide.management.endpoints.exposure`
    /// via [`is_exposed`] before mounting.
    pub endpoint: &'static str,
    pub register: fn(&Context, Router) -> Result<Router, WebError>,
}

inventory::collect!(ManagementRoute);

/// Rejects duplicate `path` registrations (adjacent-duplicate check on the
/// sorted list, same pattern as `routing::validate_routes`).
fn validate_management_routes(routes: &[&ManagementRoute]) -> Result<(), WebError> {
    for pair in routes.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if a.path == b.path {
            return Err(WebError::DuplicateRoute { method: "GET", path: a.path });
        }
    }
    Ok(())
}

async fn info(config: oxide_core::Inject<AppConfig>) -> Json<serde_json::Value> {
    let name: String = config.get_opt("oxide.application.name").ok().flatten().unwrap_or_else(|| "oxide-app".into());
    Json(serde_json::json!({
        "application": {
            "name": name,
            "profiles": config.profiles(),
        },
        "framework": {
            "name": "oxide-boot",
            "version": env!("CARGO_PKG_VERSION"),
        },
    }))
}

/// Assembles the management router: health (readiness-aware if a
/// `WebServer` is in the context, liveness-only otherwise) + `/actuator/info`
/// + all registered `ManagementRoute`s.
fn build_management_router(ctx: &Context, exposure: &str) -> Result<Router, WebError> {
    let mut router = Router::new();
    if is_exposed(exposure, "health") {
        router = router.merge(match ctx.get::<crate::server::WebServer>() {
            Some(server) => health::health_routes(server.readiness(), health::resolve_indicators(ctx)),
            None => health::liveness_only_routes(),
        });
    }

    if is_exposed(exposure, "info") {
        let config = ctx.get::<AppConfig>().expect("AppConfig is always seeded by OxideApplication");
        router = router.route(
            "/actuator/info",
            axum::routing::get(move || {
                let config = config.clone();
                async move { info(config).await }
            }),
        );
    }

    let mut routes: Vec<&ManagementRoute> = inventory::iter::<ManagementRoute>().collect();
    routes.sort_by_key(|r| r.path);
    validate_management_routes(&routes)?;
    for route in routes {
        if !is_exposed(exposure, route.endpoint) {
            continue;
        }
        router = (route.register)(ctx, router)?;
        tracing::info!(path = route.path, "management route mounted");
    }

    Ok(router)
}

/// Second HTTP server lifecycle component, listening on `oxide.management.port`
/// (default `8081`). When `enabled: false`, nothing binds and `local_addr()`
/// stays `None` for the component's lifetime.
pub struct ManagementServer {
    props: ManagementProps,
    serving: HttpServing,
}

impl ManagementServer {
    fn new(props: ManagementProps) -> Self {
        let serving = HttpServing::new(
            props.address.clone(),
            props.port,
            "management server",
            Duration::from_secs(MANAGEMENT_SHUTDOWN_GRACE_SECONDS),
        );
        Self { props, serving }
    }

    /// The bound local address, once the server has started. `None` before
    /// `post_construct` runs, after `pre_destroy` has torn it down, or for
    /// the lifetime of a component with `enabled: false`.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.serving.local_addr()
    }

    async fn start(&self, router: Router) -> ComponentResult<()> {
        self.serving.start(router).await?;
        Ok(())
    }

    async fn stop(&self) -> ComponentResult<()> {
        self.serving.stop().await
    }
}

inventory::submit! {
    ComponentDescriptor::builder(
        "ManagementServer",
        std::any::TypeId::of::<ManagementServer>,
        |b| {
            let props = b.resolve::<ManagementProps>()?;
            b.insert(ManagementServer::new((*props).clone()));
            Ok(())
        },
    )
    .deps(|| vec![(std::any::TypeId::of::<ManagementProps>(), "ManagementProps")])
    .post_construct(|ctx: &Context| {
        let me = ctx.get::<ManagementServer>().expect("component was just constructed");
        if !me.props.enabled {
            return Box::pin(async move { Ok(()) });
        }
        let router = build_management_router(ctx, &me.props.endpoints.exposure);
        Box::pin(async move { me.start(router?).await })
    })
    .pre_destroy(|ctx: &Context| {
        let me = ctx.get::<ManagementServer>().expect("component exists during shutdown");
        Box::pin(async move {
            if !me.props.enabled {
                return Ok(());
            }
            me.stop().await
        })
    })
    .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_props_default_has_correct_values() {
        let props = ManagementProps::default();
        assert_eq!(props.port, 8081);
        assert!(props.enabled);
    }

    #[test]
    fn exposure_matches_listed_endpoints_only() {
        assert!(is_exposed("health,info", "health"));
        assert!(is_exposed(" health , info ", "info"));
        assert!(!is_exposed("health,info", "prometheus"));
    }

    #[test]
    fn exposure_wildcard_matches_everything() {
        assert!(is_exposed("*", "prometheus"));
    }

    #[test]
    fn management_props_default_exposes_health_and_info_only() {
        assert_eq!(ManagementProps::default().endpoints.exposure, "health,info");
    }

    #[test]
    fn management_props_deserialize_empty_uses_defaults() {
        let props: ManagementProps = serde_json::from_str("{}").expect("empty object should deserialize with defaults");
        assert_eq!(props.port, 8081);
        assert!(props.enabled);
    }

    fn dummy_register(_ctx: &Context, router: Router) -> Result<Router, WebError> {
        Ok(router)
    }

    #[test]
    fn management_routes_reject_duplicate_paths() {
        let a = ManagementRoute { path: "/actuator/prometheus", endpoint: "prometheus", register: dummy_register };
        let b = ManagementRoute { path: "/actuator/prometheus", endpoint: "prometheus", register: dummy_register };
        let routes = vec![&a, &b];
        let err = validate_management_routes(&routes).unwrap_err();
        assert!(matches!(err, WebError::DuplicateRoute { path: "/actuator/prometheus", .. }));
    }

    #[test]
    fn management_routes_accept_distinct_paths() {
        let a = ManagementRoute { path: "/actuator/prometheus", endpoint: "prometheus", register: dummy_register };
        let b = ManagementRoute { path: "/actuator/other", endpoint: "other", register: dummy_register };
        let routes = vec![&a, &b];
        assert!(validate_management_routes(&routes).is_ok());
    }
}
