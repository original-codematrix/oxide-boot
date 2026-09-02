//! Integration test binary 4: producer→consumer trace chain + consumer lag.
//!
//! In its OWN binary (a process-global tracer provider / in-memory exporter
//! is installed). Proofs:
//! 1. A record produced inside an active span is delivered to a listener whose
//!    `kafka.process` span carries the SAME `trace_id` as the producer's
//!    `kafka.send` span (the W3C traceparent flows through Kafka headers).
//! 2. The `kafka_consumer_lag` gauge appears in the Prometheus render with a
//!    topic label.

#[path = "harness/mod.rs"]
mod harness;

use oxide_boot::OxideApplication;
use oxide_kafka::kafka_listeners;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize)]
struct Ev {
    id: u32,
}

static RECEIVED: AtomicUsize = AtomicUsize::new(0);

#[oxide_boot::component]
struct TracingListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl TracingListener {
    #[kafka_listener(topic = "tracing-e2e")]
    async fn on_event(&self, _ev: Ev) -> Result<(), String> {
        RECEIVED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn boot(group: &str) -> (tempfile::TempDir, oxide_boot::Application) {
    let dir = tempfile::tempdir().expect("tempdir");
    let yml = format!(
        "oxide:\n  application:\n    name: \"{group}\"\n  kafka:\n    bootstrap-servers: \"{}\"\n",
        harness::bootstrap_servers().await
    );
    std::fs::write(dir.path().join("application.yml"), yml).expect("write application.yml");
    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
        .start()
        .await
        .expect("boot with tracing");
    (dir, app)
}

async fn wait_until(cond: impl Fn() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cond()
}

#[test]
fn producer_consumer_trace_chain_and_consumer_lag() {
    // This binary must run its single async scenario inside a fresh tokio
    // runtime so the OTel flush and container handling are deterministic.
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        // Install the W3C propagator and an in-memory tracer provider BEFORE
        // boot, so the app's logging stacks the OTel layer on this provider.
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let exporter = oxide_observability::otel::testing::install_in_memory();

        let group = harness::unique_topic("tracing-e2e-g");
        RECEIVED.store(0, Ordering::SeqCst);
        let (_dir, app) = boot(&group).await;
        let producer = app.context().get::<oxide_kafka::KafkaProducer>().expect("producer");

        // Produce inside an active span — the injected traceparent must be the
        // same trace the consumer's kafka.process span ends up in.
        {
            let span = tracing::info_span!("test-outer");
            let _guard = span.enter();
            producer.send("tracing-e2e", "k", &Ev { id: 1 }).await.expect("produce");
        }

        assert!(
            wait_until(|| RECEIVED.load(Ordering::SeqCst) >= 1, Duration::from_secs(30)).await,
            "listener should receive the traced record"
        );

        // --- Consumer lag ---
        // While the consumer is alive, its stats callback (interval 2s) emits
        // the kafka_consumer_lag gauge. Poll the render until it shows up.
        let lag_seen = wait_until(
            || {
                let rendered = oxide_observability::prometheus_handle().render();
                rendered.contains("kafka_consumer_lag") && rendered.contains("topic=\"tracing-e2e\"")
            },
            Duration::from_secs(15),
        )
        .await;
        if !lag_seen {
            let rendered = oxide_observability::prometheus_handle().render();
            panic!("consumer lag gauge with topic label expected, got:\n{rendered}");
        }

        // Shutdown flushes OTel (OtelLifecycle runs at destroy_order 100 last).
        app.stop().await.expect("stop flushes tracing");

        // --- Trace chain ---
        let spans = exporter.get_finished_spans().expect("finished spans");
        let send = spans
            .iter()
            .find(|s| s.name == "kafka.send")
            .unwrap_or_else(|| panic!("expected a kafka.send span, saw: {:?}", spans.iter().map(|s| &s.name).collect::<Vec<_>>()));
        let process = spans
            .iter()
            .find(|s| s.name == "kafka.process")
            .unwrap_or_else(|| panic!("expected a kafka.process span, saw: {:?}", spans.iter().map(|s| &s.name).collect::<Vec<_>>()));
        assert_eq!(
            send.span_context.trace_id(),
            process.span_context.trace_id(),
            "kafka.send and kafka.process must share one trace (W3C traceparent chain)"
        );
        // The consumer span's parent is the producer span's context.
        assert_eq!(process.parent_span_id, send.span_context.span_id(), "kafka.process parents on kafka.send");
    });
}