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
        self.start_with_tls(address, port, name, router, &crate::server::SslProps::default()).await
    }

    /// As [`start`](Self::start), serving over TLS when `ssl.enabled`.
    pub(crate) async fn start_with_tls(
        &self,
        address: &str,
        port: u16,
        name: &'static str,
        router: axum::Router,
        ssl: &crate::server::SslProps,
    ) -> ComponentResult<SocketAddr> {
        // Bind first in both cases, so a port conflict is reported the same
        // way with and without TLS, and so port 0 resolves before the
        // acceptor is built.
        let listener = tokio::net::TcpListener::bind((address, port))
            .await
            .map_err(|source| crate::WebError::Bind { port, source })?;
        let local_addr = listener.local_addr().map_err(|source| crate::WebError::Bind { port, source })?;
        let (tx, rx) = oneshot::channel::<()>();

        let handle = if ssl.enabled {
            let config = load_tls_config(ssl).await?;
            let std_listener = listener.into_std().map_err(|source| crate::WebError::Bind { port, source })?;
            // `axum_server` owns its own graceful shutdown; the oneshot is
            // translated into its handle so both paths stop the same way.
            let server_handle = axum_server::Handle::new();
            let shutdown_handle = server_handle.clone();
            tokio::spawn(async move {
                let _ = rx.await;
                // `None`, not a duration: `stop` already bounds the drain by
                // timing out the join handle and aborting. Giving axum_server
                // its own deadline as well would mean two grace periods that
                // could disagree, and the shorter one would win silently.
                shutdown_handle.graceful_shutdown(None);
            });
            tokio::spawn(async move {
                axum_server::from_tcp_rustls(std_listener, config)
                    .handle(server_handle)
                    .serve(router.into_make_service())
                    .await
            })
        } else {
            tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(async move {
                        let _ = rx.await;
                    })
                    .await
            })
        };
        {
            let mut rt = self.runtime.lock().await;
            rt.shutdown = Some(tx);
            rt.handle = Some(handle);
        }
        *self.addr.lock().unwrap_or_else(|p| p.into_inner()) = Some(local_addr);
        tracing::info!(server = name, port = local_addr.port(), tls = ssl.enabled, "listening");
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

/// Reads the PEM certificate chain and private key named in `ssl`.
///
/// Both paths are required when TLS is on: a server that silently started
/// without encryption because a path was missing is the worst possible
/// failure mode, so this is an error rather than a fallback to plaintext.
async fn load_tls_config(
    ssl: &crate::server::SslProps,
) -> Result<axum_server::tls_rustls::RustlsConfig, crate::WebError> {
    let cert = ssl.certificate.as_deref().ok_or_else(|| crate::WebError::Tls {
        message: "oxide.server.ssl.enabled is true but no `certificate` path is configured".to_string(),
    })?;
    let key = ssl.private_key.as_deref().ok_or_else(|| crate::WebError::Tls {
        message: "oxide.server.ssl.enabled is true but no `private-key` path is configured".to_string(),
    })?;

    // Installed once per process; a second call is a no-op. Without a
    // provider, rustls panics at handshake time rather than at startup --
    // which would turn a configuration problem into a runtime one.
    let _ = rustls::crypto::ring::default_provider().install_default();

    axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await.map_err(|source| crate::WebError::Tls {
        // The paths are named because "invalid certificate" without them is
        // unactionable when several are mounted.
        message: format!("failed to load TLS material (certificate: {cert}, private-key: {key}): {source}"),
    })
}
