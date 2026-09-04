//! `ServerProps` configuration and the `WebServer` lifecycle component:
//! binds a TCP listener, serves an axum `Router`, and shuts down gracefully.

use crate::serve::HttpServing;
use oxide_boot::component;
use oxide_core::{ComponentResult, Context};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Shared shape behind the two per-server `default_port` helpers
/// (`ServerProps`'s `8080`, `ManagementProps`'s `8081` in `management.rs`):
/// same default-value function, parameterized on the const rather than
/// duplicated per port.
pub(crate) fn default_port<const PORT: u16>() -> u16 {
    PORT
}
fn default_grace() -> u64 {
    30
}
fn default_readiness_grace_seconds() -> u64 {
    0
}
fn default_address() -> String {
    "0.0.0.0".to_string()
}
pub(crate) fn default_request_timeout_seconds() -> u64 {
    30
}
pub(crate) fn default_max_body_bytes() -> usize {
    2 * 1024 * 1024
}

/// Configuration bound to the `oxide.server` prefix. The section may be
/// absent entirely (falls back to `Default`).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.server", default)]
pub struct ServerProps {
    #[serde(default = "default_address")]
    pub address: String,
    #[serde(default = "default_port::<8080>")]
    pub port: u16,
    #[serde(rename = "shutdown-grace-seconds", default = "default_grace")]
    pub shutdown_grace_seconds: u64,
    #[serde(rename = "request-timeout-seconds", default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    #[serde(rename = "max-body-bytes", default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
    #[serde(rename = "cors", default)]
    pub cors: crate::hardening::CorsProps,
    #[serde(rename = "shutdown", default)]
    pub shutdown: ShutdownProps,
    /// TLS, under `oxide.server.ssl` -- the same path Spring Boot uses.
    #[serde(rename = "ssl", default)]
    pub ssl: SslProps,
}

/// TLS for the HTTP server.
///
/// Mirrors Spring's `server.ssl.*`, minus the Java keystore formats: PEM is
/// what every certificate tool and every Kubernetes TLS secret already
/// produces, so a JKS/PKCS12 loader would be a conversion step for nobody's
/// benefit.
///
/// ```yaml
/// oxide:
///   server:
///     ssl:
///       enabled: true
///       certificate: /etc/tls/tls.crt        # PEM, leaf first, chain after
///       private-key: /etc/tls/tls.key        # PEM, PKCS#8 or PKCS#1
/// ```
///
/// Terminating TLS at the pod is not always right -- an ingress or a service
/// mesh often does it -- which is why this is off by default rather than
/// self-signing something at boot.
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SslProps {
    #[serde(default)]
    pub enabled: bool,
    /// Path to the certificate chain, PEM.
    #[serde(default)]
    pub certificate: Option<String>,
    /// Path to the private key, PEM (PKCS#8 or PKCS#1).
    #[serde(default)]
    pub private_key: Option<String>,
}

impl Default for ServerProps {
    fn default() -> Self {
        Self {
            address: default_address(),
            port: default_port::<8080>(),
            shutdown_grace_seconds: default_grace(),
            request_timeout_seconds: default_request_timeout_seconds(),
            max_body_bytes: default_max_body_bytes(),
            cors: crate::hardening::CorsProps::default(),
            shutdown: ShutdownProps::default(),
            ssl: SslProps::default(),
        }
    }
}

/// Configuration bound to `oxide.server.shutdown`. Deliberately a separate
/// nested struct rather than a field alongside the existing flat
/// `shutdown-grace-seconds` on `ServerProps` — that flat property predates
/// this one and is left as-is to avoid a breaking rename; the two properties
/// are inconsistent in shape for that reason.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ShutdownProps {
    /// How long `pre_destroy` waits, after flipping readiness to `DOWN` and
    /// before starting the drain, so a load balancer has a chance to see the
    /// readiness flip and stop routing new traffic before connections start
    /// being refused. `0` (the default) preserves the previous behaviour:
    /// draining begins immediately.
    #[serde(rename = "readiness-grace-seconds", default = "default_readiness_grace_seconds")]
    pub readiness_grace_seconds: u64,
}

impl Default for ShutdownProps {
    fn default() -> Self {
        Self { readiness_grace_seconds: default_readiness_grace_seconds() }
    }
}

/// HTTP server lifecycle component. Binds on `post_construct`, drains and
/// shuts down on `pre_destroy`.
///
/// `serving` and `readiness` are `#[default]`-constructed (an unbound
/// `HttpServing`, readiness `false`) rather than derived from `props` at
/// construction time: `props.address`/`props.port`/`props.shutdown_grace_seconds`
/// are only needed inside `start`/`stop_server`, which now take them as
/// parameters read from `self.props` — see `HttpServing`'s doc comment in
/// `serve.rs`. `post_construct` needs the assembled `&Context` (to build
/// the router from every registered route), so it's declared
/// `start(ctx)`.
#[component(post_construct = start(ctx), pre_destroy = stop_server)]
pub struct WebServer {
    props: ServerProps,
    #[default]
    readiness: Arc<AtomicBool>,
    #[default]
    serving: HttpServing,
}

impl WebServer {
    /// The bound local address, once the server has started. `None` before
    /// `post_construct` runs or after `pre_destroy` has torn it down.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.serving.local_addr()
    }

    /// Shared readiness flag, handed to the health routes.
    pub(crate) fn readiness(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.readiness)
    }

    async fn start(&self, ctx: &Context) -> ComponentResult<()> {
        let router = crate::routing::build_router(ctx)?;
        self.serving
            .start_with_tls(&self.props.address, self.props.port, "HTTP server", router, &self.props.ssl)
            .await?;
        self.readiness.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn stop_server(&self) -> ComponentResult<()> {
        // Flip readiness DOWN and give a load balancer time to notice before
        // the drain begins refusing new connections.
        self.readiness.store(false, Ordering::SeqCst);
        let readiness_grace = std::time::Duration::from_secs(self.props.shutdown.readiness_grace_seconds);
        if !readiness_grace.is_zero() {
            tokio::time::sleep(readiness_grace).await;
        }
        self.serving.stop("HTTP server", std::time::Duration::from_secs(self.props.shutdown_grace_seconds)).await
    }
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

    #[test]
    fn server_props_default_address_is_all_interfaces() {
        assert_eq!(ServerProps::default().address, "0.0.0.0");
    }

    #[test]
    fn server_props_deserialize_reads_address() {
        let props: ServerProps = serde_json::from_str(r#"{"address":"127.0.0.1","port":1234}"#).unwrap();
        assert_eq!(props.address, "127.0.0.1");
        assert_eq!(props.port, 1234);
    }
}
