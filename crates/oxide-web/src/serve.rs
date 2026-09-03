//! Shared HTTP-serving engine: bind, spawn, graceful shutdown with a drain
//! timeout, and forced abort past the deadline. Extracted from `WebServer`
//! so `ManagementServer` can reuse the exact same lifecycle behavior
//! (including `local_addr` clearing on stop) on a second, independent port.

use oxide_core::ComponentResult;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::{Mutex, oneshot};

struct ServingRuntime {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
}

/// Binds one axum `Router` to `port` and serves it until `stop` is called.
/// `name` is attached to log lines so the main and management servers are
/// distinguishable; `grace` bounds how long `stop` waits for in-flight
/// requests to drain before aborting the serve task.
pub(crate) struct HttpServing {
    address: String,
    port: u16,
    name: &'static str,
    grace: Duration,
    runtime: Mutex<ServingRuntime>,
    // local_addr readable without awaiting the runtime lock:
    addr: std::sync::Mutex<Option<SocketAddr>>,
}

impl HttpServing {
    pub(crate) fn new(address: String, port: u16, name: &'static str, grace: Duration) -> Self {
        Self {
            address,
            port,
            name,
            grace,
            runtime: Mutex::new(ServingRuntime { shutdown: None, handle: None }),
            addr: std::sync::Mutex::new(None),
        }
    }

    /// The bound local address, once `start` has run. `None` before the
    /// first `start` or after `stop` has torn the server down.
    pub(crate) fn local_addr(&self) -> Option<SocketAddr> {
        *self.addr.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) async fn start(&self, router: axum::Router) -> ComponentResult<SocketAddr> {
        let listener = tokio::net::TcpListener::bind((self.address.as_str(), self.port))
            .await
            .map_err(|source| crate::WebError::Bind { port: self.port, source })?;
        let local_addr = listener.local_addr().map_err(|source| crate::WebError::Bind { port: self.port, source })?;
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
        }
        *self.addr.lock().unwrap_or_else(|p| p.into_inner()) = Some(local_addr);
        tracing::info!(server = self.name, port = local_addr.port(), "listening");
        Ok(local_addr)
    }

    pub(crate) async fn stop(&self) -> ComponentResult<()> {
        *self.addr.lock().unwrap_or_else(|p| p.into_inner()) = None;
        let (shutdown, handle) = {
            let mut rt = self.runtime.lock().await;
            (rt.shutdown.take(), rt.handle.take())
        };
        if let Some(tx) = shutdown {
            let _ = tx.send(());
        }
        if let Some(mut handle) = handle {
            match tokio::time::timeout(self.grace, &mut handle).await {
                Ok(join) => {
                    join.map_err(|e| format!("{} task panicked: {e}", self.name))??;
                    tracing::info!(server = self.name, "stopped");
                }
                Err(_) => {
                    tracing::warn!(server = self.name, grace_seconds = self.grace.as_secs(), "drain timed out");
                    handle.abort();
                    return Err(
                        crate::WebError::DrainAborted { name: self.name, grace_seconds: self.grace.as_secs() }.into()
                    );
                }
            }
        }
        Ok(())
    }
}
