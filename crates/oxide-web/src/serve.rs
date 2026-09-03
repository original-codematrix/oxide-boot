//! Shared HTTP-serving engine: bind, spawn, graceful shutdown with a drain
//! timeout, and forced abort past the deadline. Extracted from `WebServer`
//! so `ManagementServer` can reuse the exact same lifecycle behavior
//! (including `local_addr` clearing on stop) on a second, independent port.

use oxide_core::ComponentResult;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::{Mutex, oneshot};

#[derive(Default)]
struct ServingRuntime {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
}

/// Binds one axum `Router` and serves it until `stop` is called.
///
/// Carries no address/port/name/grace fields of its own: those are only
/// ever needed inside `start`/`stop`, so they're taken as parameters there
/// rather than stored at construction — that's what lets `WebServer` and
/// `ManagementServer` hold a plain `#[default] serving: HttpServing` field
/// and pass their resolved `ServerProps`/`ManagementProps` values in at
/// `post_construct`/`pre_destroy` time instead of needing a custom
/// constructor the `#[component]` macro can't express.
#[derive(Default)]
pub(crate) struct HttpServing {
    runtime: Mutex<ServingRuntime>,
    // local_addr readable without awaiting the runtime lock:
    addr: std::sync::Mutex<Option<SocketAddr>>,
}

impl HttpServing {
    /// The bound local address, once `start` has run. `None` before the
    /// first `start` or after `stop` has torn the server down.
    pub(crate) fn local_addr(&self) -> Option<SocketAddr> {
        *self.addr.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// `name` is attached to log lines so the main and management servers
    /// are distinguishable.
    pub(crate) async fn start(
        &self,
        address: &str,
        port: u16,
        name: &'static str,
        router: axum::Router,
    ) -> ComponentResult<SocketAddr> {
        let listener = tokio::net::TcpListener::bind((address, port))
            .await
            .map_err(|source| crate::WebError::Bind { port, source })?;
        let local_addr = listener.local_addr().map_err(|source| crate::WebError::Bind { port, source })?;
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
        tracing::info!(server = name, port = local_addr.port(), "listening");
        Ok(local_addr)
    }

    /// `grace` bounds how long `stop` waits for in-flight requests to drain
    /// before aborting the serve task; `name` is the same log-line tag
    /// passed to `start`.
    pub(crate) async fn stop(&self, name: &'static str, grace: Duration) -> ComponentResult<()> {
        *self.addr.lock().unwrap_or_else(|p| p.into_inner()) = None;
        let (shutdown, handle) = {
            let mut rt = self.runtime.lock().await;
            (rt.shutdown.take(), rt.handle.take())
        };
        if let Some(tx) = shutdown {
            let _ = tx.send(());
        }
        if let Some(mut handle) = handle {
            match tokio::time::timeout(grace, &mut handle).await {
                Ok(join) => {
                    join.map_err(|e| format!("{name} task panicked: {e}"))??;
                    tracing::info!(server = name, "stopped");
                }
                Err(_) => {
                    tracing::warn!(server = name, grace_seconds = grace.as_secs(), "drain timed out");
                    handle.abort();
                    return Err(crate::WebError::DrainAborted { name, grace_seconds: grace.as_secs() }.into());
                }
            }
        }
        Ok(())
    }
}
