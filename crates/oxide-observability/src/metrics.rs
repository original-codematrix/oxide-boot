//! Process-global Prometheus metrics recorder.

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// SLO-style default buckets (seconds) for `http_server_requests_seconds`,
/// spanning 5ms to 10s. Without an explicit bucket configuration the
/// `metrics-exporter-prometheus` builder renders this metric as a Prometheus
/// *summary* (client-side quantiles, no `_bucket` series), which is not
/// aggregatable across instances and breaks Grafana panels built on
/// `histogram_quantile()`. Registering buckets here forces histogram
/// rendering (`_bucket{le="..."}` series) so quantiles can be computed
/// server-side across all scraped instances.
const HTTP_SERVER_REQUESTS_SECONDS_BUCKETS: &[f64] =
    &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

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
            .set_buckets_for_metric(
                Matcher::Full("http_server_requests_seconds".to_string()),
                HTTP_SERVER_REQUESTS_SECONDS_BUCKETS,
            )
            .expect("bucket list for http_server_requests_seconds is non-empty")
            .install_recorder()
            .expect("no other metrics recorder may be installed before oxide-observability")
    })
}
