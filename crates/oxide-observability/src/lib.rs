//! oxide-observability — a starter crate for Oxide Boot applications.
//!
//! Wires up a `tracing_subscriber::registry()` subscriber (JSON in prod,
//! pretty otherwise, plus an optional OTLP export layer — see [`otel`]) via
//! the `oxide_boot::LoggingInitializer` seam, and installs a process-global
//! Prometheus metrics recorder so `metrics::counter!` / `metrics::gauge!` /
//! `metrics::histogram!` calls anywhere in the application render on demand
//! via [`prometheus_handle`].

mod actuator;
mod http;
mod logging;
mod metrics;
pub mod otel;

pub use crate::metrics::prometheus_handle;

use oxide_boot::component;

/// Anchor component: presence in the context signals that oxide-observability
/// is on the dependency graph. The `/actuator/prometheus` scrape endpoint
/// (Task 7) hangs off this component.
#[component]
pub struct MetricsEndpoint {}

/// Internal re-exports used by generated macro code. Not part of the public
/// API; do not use directly.
#[doc(hidden)]
pub mod __private {
    pub use inventory;
}
