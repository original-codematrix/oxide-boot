//! Process-global Prometheus metrics recorder.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Installs the process-global Prometheus recorder on first call; later calls
/// (further boots in the same test binary) reuse the existing handle.
///
/// The `expect` below fires only if a *foreign* recorder (installed by code
/// outside oxide-observability) got there first — that is a fail-fast
/// misconfiguration signal at boot, not a condition oxide-observability
/// itself can trigger, since it always goes through this `OnceLock`.
pub fn prometheus_handle() -> &'static PrometheusHandle {
    HANDLE.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("no other metrics recorder may be installed before oxide-observability")
    })
}
