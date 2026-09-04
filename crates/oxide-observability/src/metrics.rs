//! Process-global Prometheus metrics recorder.

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Set during boot, before the recorder is first touched. Read by
/// [`prometheus_handle`] and by the request middleware.
pub(crate) static HTTP_METRICS: OnceLock<HttpMetricsProps> = OnceLock::new();

/// Configuration bound to `oxide.metrics.http`.
///
/// A latency histogram is by far the most expensive metric a framework emits
/// by default: one series per bucket per label combination, so a single
/// method/route/status triple costs 14 series (12 buckets, plus `_sum` and
/// `_count`) where a counter costs one. Twenty routes across the usual
/// status codes reach four figures. These knobs exist so that cost is a
/// decision rather than a surprise.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.metrics.http", default)]
#[serde(rename_all = "kebab-case")]
pub struct HttpMetricsProps {
    /// Emit request metrics at all.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Emit the latency histogram. With this off, only
    /// `http_server_requests_total` is emitted -- one series per
    /// method/route/status instead of fourteen. Rate, throughput and error
    /// ratio all still work; only latency percentiles are lost.
    #[serde(default = "default_true")]
    pub histogram: bool,
    /// Bucket bounds in seconds. Fewer buckets, fewer series: halving this
    /// list halves the histogram's cost.
    #[serde(default = "default_buckets")]
    pub buckets: Vec<f64>,
}

fn default_true() -> bool {
    true
}

fn default_buckets() -> Vec<f64> {
    DEFAULT_BUCKETS.to_vec()
}

impl Default for HttpMetricsProps {
    fn default() -> Self {
        Self { enabled: default_true(), histogram: default_true(), buckets: default_buckets() }
    }
}

impl HttpMetricsProps {
    /// The configured values, or the defaults when boot has not run -- which
    /// is the case in unit tests that touch the recorder directly.
    pub(crate) fn current() -> &'static Self {
        static FALLBACK: OnceLock<HttpMetricsProps> = OnceLock::new();
        HTTP_METRICS.get().unwrap_or_else(|| FALLBACK.get_or_init(HttpMetricsProps::default))
    }
}

/// SLO-style default buckets (seconds) for `http_server_requests_seconds`,
/// spanning 5ms to 10s. Without an explicit bucket configuration the
/// `metrics-exporter-prometheus` builder renders this metric as a Prometheus
/// *summary* (client-side quantiles, no `_bucket` series), which is not
/// aggregatable across instances and breaks Grafana panels built on
/// `histogram_quantile()`. Registering buckets here forces histogram
/// rendering (`_bucket{le="..."}` series) so quantiles can be computed
/// server-side across all scraped instances.
const DEFAULT_BUCKETS: &[f64] = &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

/// Installs the process-global Prometheus recorder on first call; later calls
/// (further boots in the same test binary) reuse the existing handle.
///
/// The `expect` below fires only if a *foreign* recorder (installed by code
/// outside oxide-observability) got there first — that is a fail-fast
/// misconfiguration signal at boot, not a condition oxide-observability
/// itself can trigger, since it always goes through this `OnceLock`.
pub fn prometheus_handle() -> &'static PrometheusHandle {
    HANDLE.get_or_init(|| {
        // An empty list would make the exporter fall back to summary
        // rendering, so a misconfigured `buckets: []` quietly changes the
        // metric's type; fall back to the defaults instead.
        let props = HttpMetricsProps::current();
        let buckets: &[f64] = if props.buckets.is_empty() { DEFAULT_BUCKETS } else { &props.buckets };
        PrometheusBuilder::new()
            .set_buckets_for_metric(Matcher::Full("http_server_requests_seconds".to_string()), buckets)
            .expect("bucket list for http_server_requests_seconds is non-empty")
            .install_recorder()
            .expect("no other metrics recorder may be installed before oxide-observability")
    })
}
