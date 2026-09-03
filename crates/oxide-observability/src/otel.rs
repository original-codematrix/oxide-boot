//! OTLP trace export, wired into `logging.rs`'s subscriber.
//!
//! Two things happen unconditionally, regardless of `oxide.otel.endpoint`:
//! - a process-global W3C `TraceContextPropagator` is installed (this is
//!   what makes `http.rs`'s `traceparent` extraction, added in Task 8,
//!   actually do something instead of being a noop),
//! - `OtelProps` binds `oxide.otel.*`, tolerating a missing section.
//!
//! With `endpoint` set, `build_layer` additionally builds an OTLP (grpc via
//! tonic) batch-exporting `TracerProvider` — `service.name` resource from
//! `oxide.application.name`, sampler `ParentBased(TraceIdRatioBased(sample_ratio))`
//! — registers it as the process-global tracer provider, and returns a
//! `tracing-opentelemetry` layer wrapping it for `logging.rs` to stack onto
//! the subscriber. Without an endpoint, `build_layer` returns `None` and
//! nothing else in this module does any work: no provider, no export, no
//! error.
//!
//! `OtelLifecycle` (the `#[component]` at the bottom) is the shutdown/flush
//! seam: its `pre_destroy` shuts down whatever provider ended up installed
//! (real or, in tests, in-memory), which flushes the batch processor. When
//! nothing was ever installed, it's a no-op.
//!
//! Both the provider and the propagator are genuine process-global
//! OpenTelemetry state (`opentelemetry::global::*`), installed at most once
//! per process via the `PROVIDER` `OnceLock` below — same "first boot in
//! this process wins" pattern as `metrics::prometheus_handle` and the
//! `try_init` calls in `logging.rs`.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{BatchSpanProcessor, Sampler, Tracer, TracerProvider};
use opentelemetry_sdk::{Resource, runtime};
use oxide_core::{AppConfig, ComponentDescriptor, Context};
use std::any::TypeId;
use std::sync::OnceLock;

fn default_sample_ratio() -> f64 {
    1.0
}

/// Configuration bound to the `oxide.otel` prefix. The section may be
/// absent entirely (falls back to `Default`) — no `endpoint` means tracing
/// export stays off.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.otel", default)]
pub struct OtelProps {
    #[serde(default)]
    pub endpoint: Option<String>,
    // kebab-case YAML key, matching the framework's established style for
    // multi-word config keys (see `ServerProps::shutdown_grace_seconds` /
    // `shutdown-grace-seconds`) — without this rename, `oxide.otel.sample-ratio`
    // (as documented in README.md) would silently fail to bind and the field
    // would always fall back to its default.
    #[serde(rename = "sample-ratio", default = "default_sample_ratio")]
    pub sample_ratio: f64,
}

impl Default for OtelProps {
    fn default() -> Self {
        Self { endpoint: None, sample_ratio: default_sample_ratio() }
    }
}

/// The process-global tracer provider, set at most once. `OtelLifecycle`
/// reads this on shutdown; `None` means nothing was ever installed (no
/// `endpoint` configured) and shutdown is a no-op.
static PROVIDER: OnceLock<TracerProvider> = OnceLock::new();

/// Concrete type of the layer `logging.rs` stacks onto its
/// `tracing_subscriber::registry()` — pinned to `Registry` since that's the
/// only subscriber base this crate ever builds.
pub(crate) type OtelLayer = tracing_opentelemetry::OpenTelemetryLayer<tracing_subscriber::Registry, Tracer>;

/// Builds the OTEL layer for `logging.rs`, and always sets the global W3C
/// propagator first (needed by `http.rs`'s extraction regardless of whether
/// export is configured). Returns `Ok(None)` when no provider is installed
/// and none should be built (`oxide.otel.endpoint` unset) — that is the
/// normal, error-free "tracing export is off" path.
pub(crate) fn build_layer(config: &AppConfig) -> Result<Option<OtelLayer>, Box<dyn std::error::Error + Send + Sync>> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    // A provider is already installed — either a previous boot in this same
    // process (test binaries booting more than once), or the in-memory test
    // seam (`testing::install_in_memory`) run before this boot. Reuse it
    // rather than trying to build a second one from config: `OnceLock`
    // wouldn't let a second provider become the process-global one anyway.
    if let Some(provider) = PROVIDER.get() {
        return Ok(Some(layer_from(provider.clone())));
    }

    let props: OtelProps = config.get_opt("oxide.otel")?.unwrap_or_default();
    let Some(endpoint) = props.endpoint else {
        return Ok(None);
    };
    let service_name: String = config.get_opt("oxide.application.name")?.unwrap_or_else(|| "oxide-app".into());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    let provider = build_provider(exporter, service_name, props.sample_ratio);
    Ok(Some(install(provider)))
}

/// Assembles a `TracerProvider` from any `SpanExporter` (real OTLP or the
/// in-memory test exporter share this) via a batch processor on the Tokio
/// runtime, the `service.name` resource, and parent-based ratio sampling.
fn build_provider<E>(exporter: E, service_name: String, sample_ratio: f64) -> TracerProvider
where
    E: opentelemetry_sdk::export::trace::SpanExporter + 'static,
{
    TracerProvider::builder()
        .with_span_processor(BatchSpanProcessor::builder(exporter, runtime::Tokio).build())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sample_ratio))))
        .with_resource(Resource::new(vec![KeyValue::new("service.name", service_name)]))
        .build()
}

/// Registers `provider` as the process-global tracer provider and parks it
/// in `PROVIDER` for `OtelLifecycle` to shut down later, then hands back the
/// `tracing-opentelemetry` layer wrapping it.
fn install(provider: TracerProvider) -> OtelLayer {
    let _ = global::set_tracer_provider(provider.clone());
    let _ = PROVIDER.set(provider.clone());
    layer_from(provider)
}

fn layer_from(provider: TracerProvider) -> OtelLayer {
    let tracer = provider.tracer("oxide-observability");
    tracing_opentelemetry::layer().with_tracer(tracer)
}

/// Shuts down (and thereby batch-flushes) the process-global tracer
/// provider. No-op when nothing was ever installed.
pub(crate) fn shutdown() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(provider) = PROVIDER.get() {
        provider.shutdown().map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    }
    Ok(())
}

/// Shutdown/flush component: presence in the context has no other effect,
/// its `pre_destroy` is the only reason it exists.
///
/// Hand-written descriptor rather than `#[oxide_boot::component]`: the
/// macro doesn't expose `destroy_order` (out of scope for this phase), and
/// this component's entire purpose is to run its flush last. `destroy_order
/// (100)` — the framework's reserved telemetry-flush slot — guarantees the
/// flush runs after every span-producing component has stopped, turning
/// what used to be an alphabetical-sort coincidence (reverse construction
/// order among order-0 components) into an explicit guarantee.
pub struct OtelLifecycle {}

impl OtelLifecycle {
    /// `TracerProvider::shutdown` is a *synchronous*, blocking call — it
    /// waits (via `futures_executor::block_on`, not a tokio-aware await) for
    /// the batch processor's background task to drain and reply. That
    /// background task is itself scheduled on the tokio runtime, so calling
    /// `shutdown()` directly from this async hook would monopolize the
    /// runtime's only worker thread on a single-threaded (e.g. `#[tokio::
    /// test]`-default) runtime and deadlock. `spawn_blocking` moves the
    /// blocking wait to a dedicated thread, leaving the async worker free to
    /// keep driving the background task.
    async fn flush(&self) -> oxide_core::ComponentResult<()> {
        tokio::task::spawn_blocking(shutdown).await??;
        Ok(())
    }
}

inventory::submit! {
    ComponentDescriptor::builder(
        "OtelLifecycle",
        TypeId::of::<OtelLifecycle>,
        |builder| {
            builder.insert(OtelLifecycle {});
            Ok(())
        },
    )
    .pre_destroy(|ctx: &Context| {
        let me = ctx.get::<OtelLifecycle>().expect("component exists during shutdown");
        Box::pin(async move { me.flush().await })
    })
    .destroy_order(100)
    .build()
}

/// Test-only seam. `#[doc(hidden)]` because it mutates process-global
/// OpenTelemetry state (propagator + tracer provider) — reaching for it
/// outside a dedicated test binary would race with whatever else in the
/// process also touches that global state.
#[doc(hidden)]
pub mod testing {
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
    use opentelemetry_sdk::testing::trace::InMemorySpanExporter;

    /// Wraps `InMemorySpanExporter` to swallow `shutdown()`.
    ///
    /// `InMemorySpanExporter::shutdown` is implemented as `reset()` — it
    /// clears the buffer. `BatchSpanProcessor`'s own shutdown handshake does
    /// exactly the right thing for a real exporter (flush pending spans,
    /// *then* tell the exporter to release its resources), but that same
    /// sequence, run against the real `InMemorySpanExporter`, flushes the
    /// spans in and then immediately wipes them via that trailing `shutdown`
    /// call — nothing would ever be left to read afterwards. This wrapper
    /// keeps `export`/`set_resource` real and makes `shutdown` a no-op, so
    /// `OtelLifecycle::flush` can exercise the *real* flush-on-shutdown path
    /// end to end and still leave the spans in place for the test to read.
    #[derive(Clone, Debug)]
    struct KeepFinishedSpans(InMemorySpanExporter);

    type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

    impl SpanExporter for KeepFinishedSpans {
        fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
            self.0.export(batch)
        }

        fn shutdown(&mut self) {
            // Deliberately not forwarded — see the type doc comment.
        }

        fn set_resource(&mut self, resource: &Resource) {
            self.0.set_resource(resource);
        }
    }

    /// Installs a tracer provider backed by an in-memory span exporter
    /// (same `Sampler`/`Resource`/`BatchSpanProcessor` wiring as a real OTLP
    /// provider, sample ratio 1.0 so every span is kept) as the
    /// process-global provider, and returns a clone of the exporter to read
    /// finished spans from after shutdown flushes them. Call this BEFORE
    /// booting the app — the boot's `logging.rs` init picks up the
    /// already-installed provider instead of building one from
    /// `oxide.otel.endpoint`.
    pub fn install_in_memory() -> InMemorySpanExporter {
        install_in_memory_with_ratio(1.0)
    }

    /// Same as [`install_in_memory`], but with an injectable `sample_ratio`
    /// — needed to distinguish `ParentBased(TraceIdRatioBased(ratio))` from
    /// a bare always-on sampler: a hardcoded ratio of 1.0 can't tell the two
    /// apart, since both would export every span regardless of the parent's
    /// sampled flag. `tests/otel_parent_sampling.rs` uses `ratio: 0.0` with
    /// a *sampled* parent header to prove the `ParentBased` wrapping is
    /// actually in effect (a bare `TraceIdRatioBased(0.0)` would drop the
    /// span; only respecting the parent's sampled decision keeps it).
    pub fn install_in_memory_with_ratio(ratio: f64) -> InMemorySpanExporter {
        let exporter = InMemorySpanExporter::default();
        let provider = super::build_provider(KeepFinishedSpans(exporter.clone()), "otel-test".into(), ratio);
        super::install(provider);
        exporter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn otel_props_default_has_no_endpoint_and_full_sample_ratio() {
        let props = OtelProps::default();
        assert_eq!(props.endpoint, None);
        assert_eq!(props.sample_ratio, 1.0);
    }

    #[test]
    fn otel_props_deserialize_empty_uses_defaults() {
        let props: OtelProps = serde_json::from_str("{}").expect("empty object should deserialize with defaults");
        assert_eq!(props.endpoint, None);
        assert_eq!(props.sample_ratio, 1.0);
    }

    #[test]
    fn otel_props_deserialize_reads_endpoint_and_sample_ratio() {
        let props: OtelProps = serde_json::from_str(r#"{"endpoint": "http://collector:4317", "sample-ratio": 0.1}"#)
            .expect("valid object should deserialize");
        assert_eq!(props.endpoint.as_deref(), Some("http://collector:4317"));
        assert_eq!(props.sample_ratio, 0.1);
    }

    /// Regression test for a docs/code mismatch found in review: README.md
    /// documents `oxide.otel.sample-ratio` (kebab-case, matching the
    /// framework's style for other multi-word keys); without the
    /// `#[serde(rename = "sample-ratio")]` above, that key would silently
    /// fail to bind and `sample_ratio` would always be the default (1.0).
    #[test]
    fn otel_props_deserialize_reads_kebab_case_sample_ratio_key() {
        let props: OtelProps = serde_json::from_str(r#"{"sample-ratio": 0.25}"#).expect("kebab key should bind");
        assert_eq!(props.sample_ratio, 0.25);
    }
}
