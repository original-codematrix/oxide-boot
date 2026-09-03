//! Route registry and deterministic router assembly from collected RouteDescriptors.

use crate::WebError;
use axum::Router;
use oxide_core::Context;

/// One HTTP route contributed by a controller (usually macro-generated).
/// `register` resolves the owning controller from the context ONCE at router
/// build time and moves the cloned handle into the axum handler closure.
pub struct RouteDescriptor {
    pub method: &'static str,
    pub path: &'static str,
    pub register: fn(&Context, Router) -> Result<Router, WebError>,
}

inventory::collect!(RouteDescriptor);

/// Rejects duplicate `(method, path)` registrations and any route that
/// collides with the `/actuator` prefix reserved for the framework's own
/// health endpoints. `routes` must already be sorted by `(path, method)`.
fn validate_routes(routes: &[&RouteDescriptor]) -> Result<(), WebError> {
    for pair in routes.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if a.path == b.path && a.method == b.method {
            return Err(WebError::DuplicateRoute { method: a.method, path: a.path });
        }
    }
    for route in routes {
        if route.path == "/actuator" || route.path.starts_with("/actuator/") {
            return Err(WebError::ReservedPath { path: route.path });
        }
    }
    Ok(())
}

/// A cross-cutting layer applied to the fully-mounted router (e.g. metrics,
/// tracing, request timeout/body limit/CORS). `apply` receives the context
/// (so config-driven layers can resolve their own `*Props`) and the router
/// with all `RouteDescriptor`s already mounted — this is required for axum's
/// `MatchedPath` extension to be visible to any `middleware::from_fn` the
/// layer installs. Collected layers are applied in ascending `name` order
/// for deterministic behavior when several layers are present.
pub struct RouterLayer {
    pub name: &'static str,
    pub apply: fn(&Context, Router) -> Router,
}

inventory::collect!(RouterLayer);

/// Assembles the HTTP router by collecting all registered RouteDescriptors
/// and sorting them deterministically. Health endpoints live on the
/// management server (see `management.rs`), not here — the `/actuator`
/// prefix stays reserved so user routes can't collide with it. Registered
/// `RouterLayer`s are applied last, after every route is mounted.
pub(crate) fn build_router(ctx: &Context) -> Result<Router, WebError> {
    let mut routes: Vec<&RouteDescriptor> = inventory::iter::<RouteDescriptor>().collect();
    routes.sort_by_key(|r| (r.path, r.method));
    validate_routes(&routes)?;
    let mut router = Router::new();
    for route in routes {
        router = (route.register)(ctx, router)?;
        tracing::info!(method = route.method, path = route.path, "route mounted");
    }

    let mut layers: Vec<&RouterLayer> = inventory::iter::<RouterLayer>().collect();
    layers.sort_by_key(|l| l.name);
    for layer in layers {
        router = (layer.apply)(ctx, router);
        tracing::debug!(name = layer.name, "router layer applied");
    }

    Ok(router)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_register(_ctx: &Context, router: Router) -> Result<Router, WebError> {
        Ok(router)
    }

    fn descriptor(method: &'static str, path: &'static str) -> RouteDescriptor {
        RouteDescriptor { method, path, register: dummy_register }
    }

    #[test]
    fn accepts_distinct_sorted_routes() {
        let a = descriptor("GET", "/api/notes");
        let b = descriptor("POST", "/api/notes");
        let routes = vec![&a, &b];
        assert!(validate_routes(&routes).is_ok());
    }

    #[test]
    fn rejects_duplicate_method_and_path() {
        let a = descriptor("GET", "/api/notes");
        let b = descriptor("GET", "/api/notes");
        let routes = vec![&a, &b];
        let err = validate_routes(&routes).unwrap_err();
        match err {
            WebError::DuplicateRoute { method, path } => {
                assert_eq!(method, "GET");
                assert_eq!(path, "/api/notes");
            }
            other => panic!("expected DuplicateRoute, got {other:?}"),
        }
    }

    #[test]
    fn allows_same_path_different_method() {
        let a = descriptor("GET", "/api/notes");
        let b = descriptor("DELETE", "/api/notes");
        let routes = vec![&a, &b];
        assert!(validate_routes(&routes).is_ok());
    }

    #[test]
    fn rejects_exact_actuator_path() {
        let a = descriptor("GET", "/actuator");
        let routes = vec![&a];
        let err = validate_routes(&routes).unwrap_err();
        assert!(matches!(err, WebError::ReservedPath { path: "/actuator" }));
    }

    #[test]
    fn rejects_actuator_prefixed_path() {
        let a = descriptor("GET", "/actuator/custom");
        let routes = vec![&a];
        let err = validate_routes(&routes).unwrap_err();
        assert!(matches!(err, WebError::ReservedPath { path: "/actuator/custom" }));
    }

    #[test]
    fn allows_path_that_merely_contains_actuator_as_a_substring() {
        let a = descriptor("GET", "/api/actuatorish");
        let routes = vec![&a];
        assert!(validate_routes(&routes).is_ok());
    }
}
