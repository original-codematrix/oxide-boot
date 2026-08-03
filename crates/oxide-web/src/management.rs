//! `ManagementProps` configuration and the `ManagementServer` lifecycle
//! component: a second HTTP listener, separate from the main `WebServer`,
//! that carries the framework's own operational endpoints (health, info,
//! and whatever else registers a [`ManagementRoute`]) off the application's
//! main port.

use crate::serve::HttpServing;
use crate::{health, WebError};
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

/// Drain grace period for the management server. Management endpoints
/// (health, info, metrics) don't hold long-running requests, so a short,
/// fixed grace is enough — unlike the main server this isn't user-configurable.
const MANAGEMENT_SHUTDOWN_GRACE_SECONDS: u64 = 5;

/// Configuration bound to the `oxide.management` prefix. The section may be
/// absent entirely (falls back to `Default`).
#[derive(Debug, Clone, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.management", default)]
pub struct ManagementProps {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl Default for ManagementProps {
    fn default() -> Self {
        Self { port: default_port(), enabled: default_enabled() }
    }
}

/// One route contributed to the management server by another part of the
/// framework (e.g. `/actuator/prometheus` in Task 7). `register` resolves
/// the owning component from the context ONCE at router build time, same
/// contract as `RouteDescriptor`.
pub struct ManagementRoute {
    pub path: &'static str,
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
    let name: String =
        config.get_opt("oxide.application.name").ok().flatten().unwrap_or_else(|| "oxide-app".into());
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
fn build_management_router(ctx: &Context) -> Result<Router, WebError> {
    let mut router = match ctx.get::<crate::server::WebServer>() {
        Some(server) => health::health_routes(server.readiness(), health::resolve_indicators(ctx)),
        None => health::liveness_only_routes(),
    };

    let config = ctx.get::<AppConfig>().expect("AppConfig is always seeded by OxideApplication");
    router = router.route(
        "/actuator/info",
        axum::routing::get(move || {
            let config = config.clone();
            async move { info(config).await }
        }),
    );

    let mut routes: Vec<&ManagementRoute> = inventory::iter::<ManagementRoute>().collect();
    routes.sort_by_key(|r| r.path);
    validate_management_routes(&routes)?;
    for route in routes {
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
        let serving =
            HttpServing::new(props.port, "management server", Duration::from_secs(MANAGEMENT_SHUTDOWN_GRACE_SECONDS));
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
        let router = build_management_router(ctx);
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
    fn management_props_deserialize_empty_uses_defaults() {
        let props: ManagementProps =
            serde_json::from_str("{}").expect("empty object should deserialize with defaults");
        assert_eq!(props.port, 8081);
        assert!(props.enabled);
    }

    fn dummy_register(_ctx: &Context, router: Router) -> Result<Router, WebError> {
        Ok(router)
    }

    #[test]
    fn management_routes_reject_duplicate_paths() {
        let a = ManagementRoute { path: "/actuator/prometheus", register: dummy_register };
        let b = ManagementRoute { path: "/actuator/prometheus", register: dummy_register };
        let routes = vec![&a, &b];
        let err = validate_management_routes(&routes).unwrap_err();
        assert!(matches!(err, WebError::DuplicateRoute { path: "/actuator/prometheus", .. }));
    }

    #[test]
    fn management_routes_accept_distinct_paths() {
        let a = ManagementRoute { path: "/actuator/prometheus", register: dummy_register };
        let b = ManagementRoute { path: "/actuator/other", register: dummy_register };
        let routes = vec![&a, &b];
        assert!(validate_management_routes(&routes).is_ok());
    }
}
