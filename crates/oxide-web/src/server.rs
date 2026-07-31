//! `ServerProps` configuration and the `WebServer` lifecycle component:
//! binds a TCP listener, serves an axum `Router`, and shuts down gracefully.

use oxide_core::{ComponentDescriptor, ComponentResult, Context};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

fn default_port() -> u16 {
    8080
}
fn default_grace() -> u64 {
    30
}

/// Configuration bound to the `oxide.server` prefix. The section may be
/// absent entirely (falls back to `Default`).
#[derive(Debug, Clone, serde::Deserialize)]
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

struct ServerRuntime {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
    local_addr: Option<SocketAddr>,
}

/// HTTP server lifecycle component. Binds on `post_construct`, drains and
/// shuts down on `pre_destroy`.
pub struct WebServer {
    props: ServerProps,
    readiness: Arc<AtomicBool>,
    runtime: Mutex<ServerRuntime>,
    // local_addr readable without awaiting the runtime lock:
    addr: std::sync::Mutex<Option<SocketAddr>>,
}

impl WebServer {
    fn new(props: ServerProps) -> Self {
        Self {
            props,
            readiness: Arc::new(AtomicBool::new(false)),
            runtime: Mutex::new(ServerRuntime { shutdown: None, handle: None, local_addr: None }),
            addr: std::sync::Mutex::new(None),
        }
    }

    /// The bound local address, once the server has started. `None` before
    /// `post_construct` runs or after `pre_destroy` has torn it down.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        *self.addr.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Shared readiness flag, handed to the health routes.
    pub(crate) fn readiness(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.readiness)
    }

    async fn start(&self, router: axum::Router) -> ComponentResult<()> {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", self.props.port))
            .await
            .map_err(|source| crate::WebError::Bind { port: self.props.port, source })?;
        let local_addr = listener
            .local_addr()
            .map_err(|source| crate::WebError::Bind { port: self.props.port, source })?;
        let (tx, rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = rx.await;
                })
                .await
        });
        {
            let mut rt = self.runtime.lock().await;
            rt.shutdown = Some(tx);
            rt.handle = Some(handle);
            rt.local_addr = Some(local_addr);
        }
        *self.addr.lock().unwrap_or_else(|p| p.into_inner()) = Some(local_addr);
        self.readiness.store(true, Ordering::SeqCst);
        tracing::info!(port = local_addr.port(), "HTTP server listening");
        Ok(())
    }

    async fn stop_server(&self) -> ComponentResult<()> {
        self.readiness.store(false, Ordering::SeqCst);
        let (shutdown, handle) = {
            let mut rt = self.runtime.lock().await;
            (rt.shutdown.take(), rt.handle.take())
        };
        if let Some(tx) = shutdown {
            let _ = tx.send(());
        }
        if let Some(handle) = handle {
            let grace = std::time::Duration::from_secs(self.props.shutdown_grace_seconds);
            match tokio::time::timeout(grace, handle).await {
                Ok(join) => {
                    join.map_err(|e| format!("server task panicked: {e}"))??;
                    tracing::info!("HTTP server stopped");
                }
                Err(_) => {
                    tracing::warn!(
                        grace_seconds = self.props.shutdown_grace_seconds,
                        "HTTP server drain timed out"
                    );
                }
            }
        }
        Ok(())
    }
}

inventory::submit! {
    ComponentDescriptor {
        type_name: "ServerProps",
        type_id: || std::any::TypeId::of::<ServerProps>(),
        deps: || vec![(std::any::TypeId::of::<oxide_core::AppConfig>(), "AppConfig")],
        construct: |b| {
            let config = b.resolve::<oxide_core::AppConfig>()?;
            let props: ServerProps = config
                .get_opt("oxide.server")
                .map_err(|e| oxide_core::ContextError::Construct {
                    type_name: "ServerProps",
                    message: format!("binding prefix 'oxide.server': {e}"),
                })?
                .unwrap_or_default();
            b.insert(props);
            Ok(())
        },
        post_construct: None,
        pre_destroy: None,
    }
}

inventory::submit! {
    ComponentDescriptor {
        type_name: "WebServer",
        type_id: || std::any::TypeId::of::<WebServer>(),
        deps: || vec![(std::any::TypeId::of::<ServerProps>(), "ServerProps")],
        construct: |b| {
            let props = b.resolve::<ServerProps>()?;
            b.insert(WebServer::new((*props).clone()));
            Ok(())
        },
        post_construct: Some(|ctx: &Context| {
            // Task 5 will replace this with `crate::routing::build_router(ctx)`;
            // for now the router is just the health endpoints.
            let me = ctx.get::<WebServer>().expect("component was just constructed");
            let router = crate::health::health_routes(me.readiness());
            Box::pin(async move { me.start(router).await })
        }),
        pre_destroy: Some(|ctx: &Context| {
            let me = ctx.get::<WebServer>().expect("component exists during shutdown");
            Box::pin(async move { me.stop_server().await })
        }),
    }
}
