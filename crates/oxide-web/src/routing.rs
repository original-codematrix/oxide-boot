//! Route registry and deterministic router assembly from collected RouteDescriptors.

use crate::{health, server::WebServer, WebError};
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

/// Assembles the HTTP router by collecting all registered RouteDescriptors,
/// sorting them deterministically, and merging the health endpoints.
pub(crate) fn build_router(ctx: &Context) -> Result<Router, WebError> {
    let mut routes: Vec<&RouteDescriptor> = inventory::iter::<RouteDescriptor>().collect();
    routes.sort_by_key(|r| (r.path, r.method));
    let mut router = Router::new();
    for route in routes {
        router = (route.register)(ctx, router)?;
        tracing::info!(method = route.method, path = route.path, "route mounted");
    }
    if let Some(server) = ctx.get::<WebServer>() {
        router = router.merge(health::health_routes(server.readiness()));
    }
    Ok(router)
}
