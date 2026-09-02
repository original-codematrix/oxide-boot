//! `ServerProps` configuration and the `WebServer` lifecycle component:
//! binds a TCP listener, serves an axum `Router`, and shuts down gracefully.

use crate::serve::HttpServing;
use oxide_core::{ComponentDescriptor, ComponentResult, Context};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn default_port() -> u16 {
    8080
}
fn default_grace() -> u64 {
    30
}

/// Configuration bound to the `oxide.server` prefix. The section may be
/// absent entirely (falls back to `Default`).
#[derive(Debug, Clone, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.server", default)]
pub struct ServerProps {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(rename = "shutdown-grace-seconds", default = "default_grace")]
    pub shutdown_grace_seconds: u64,
}

impl Default for ServerProps {
    fn default() -> Self {
        Self { port: default_port(), shutdown_grace_seconds: default_grace() }
    }
}

/// HTTP server lifecycle component. Binds on `post_construct`, drains and
/// shuts down on `pre_destroy`.
pub struct WebServer {
    readiness: Arc<AtomicBool>,
    serving: HttpServing,
}

impl WebServer {
    fn new(props: ServerProps) -> Self {
        Self {
            readiness: Arc::new(AtomicBool::new(false)),
            serving: HttpServing::new(
                props.port,
                "HTTP server",
                std::time::Duration::from_secs(props.shutdown_grace_seconds),
            ),
        }
    }

    /// The bound local address, once the server has started. `None` before
    /// `post_construct` runs or after `pre_destroy` has torn it down.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.serving.local_addr()
    }

    /// Shared readiness flag, handed to the health routes.
    pub(crate) fn readiness(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.readiness)
    }

    async fn start(&self, router: axum::Router) -> ComponentResult<()> {
        self.serving.start(router).await?;
        self.readiness.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn stop_server(&self) -> ComponentResult<()> {
        self.readiness.store(false, Ordering::SeqCst);
        self.serving.stop().await
    }
}

inventory::submit! {
    ComponentDescriptor::builder(
        "WebServer",
        std::any::TypeId::of::<WebServer>,
        |b| {
            let props = b.resolve::<ServerProps>()?;
            b.insert(WebServer::new((*props).clone()));
            Ok(())
        },
    )
    .deps(|| vec![(std::any::TypeId::of::<ServerProps>(), "ServerProps")])
    .post_construct(|ctx: &Context| {
        let me = ctx.get::<WebServer>().expect("component was just constructed");
        let router = crate::routing::build_router(ctx);
        Box::pin(async move { me.start(router?).await })
    })
    .pre_destroy(|ctx: &Context| {
        let me = ctx.get::<WebServer>().expect("component exists during shutdown");
        Box::pin(async move { me.stop_server().await })
    })
    .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_props_default_has_correct_values() {
        let props = ServerProps::default();
        assert_eq!(props.port, 8080);
        assert_eq!(props.shutdown_grace_seconds, 30);
    }

    #[test]
    fn server_props_deserialize_empty_uses_defaults() {
        let props: ServerProps = serde_json::from_str("{}").expect("empty object should deserialize with defaults");
        assert_eq!(props.port, 8080);
        assert_eq!(props.shutdown_grace_seconds, 30);
    }
}
