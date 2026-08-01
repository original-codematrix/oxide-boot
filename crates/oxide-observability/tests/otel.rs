//! OTLP trace export end-to-end proof, in its OWN test binary.
//!
//! `oxide_observability::otel` sets a process-global `TraceContextPropagator`
//! and (via `otel::testing::install_in_memory`) a process-global tracer
//! provider — both are one-shot `OnceLock`s, so this binary carries exactly
//! ONE test. Sharing a binary with `tests/observability.rs` would mean the
//! first boot there could win the global propagator/provider race before
//! this test gets to install its in-memory exporter, or vice versa; a
//! separate binary gives each its own process.
//!
//! The scenario proves the whole chain end to end: a request carrying a W3C
//! `traceparent` header is extracted by the `http.rs` middleware (Task 8),
//! attached as the `http.request` span's parent, and the finished span is
//! exported through the `tracing-opentelemetry` layer into the in-memory
//! exporter with that same trace ID — which only happens if `logging.rs`
//! actually stacked the OTEL layer onto the subscriber.

use oxide_boot::prelude::*;
use std::collections::HashMap;

#[oxide_boot::component]
struct PingController {}

#[oxide_web::controller("/ping")]
impl PingController {
    #[get("/{name}")]
    async fn ping(&self, name: axum::extract::Path<String>) -> String {
        format!("pong {}", *name)
    }
}

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: otel-test\n  server:\n    port: 0\n  management:\n    port: 0\n",
    )
    .unwrap();
    dir
}

#[tokio::test]
async fn traceparent_header_becomes_parent_of_exported_request_span() {
    // 1. Install a tracer provider backed by opentelemetry_sdk's in-memory
    //    span exporter, globally, BEFORE boot — `logging.rs` picks up the
    //    already-installed provider instead of trying to build one from
    //    `oxide.otel.endpoint` (which this test's config doesn't set).
    let exporter = oxide_observability::otel::testing::install_in_memory();

    // 2. Boot the app (ports 0 so this test doesn't collide with others).
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let main = app.context().get::<oxide_web::WebServer>().unwrap().local_addr().unwrap();

    // 3. GET with a fixed W3C traceparent header — this is the trace ID the
    //    exported span must carry.
    let client = reqwest::Client::new();
    let res = client
        .get(format!("http://{main}/ping/world"))
        .header("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    // 4. Stop the app — `OtelLifecycle::pre_destroy` shuts down (and thereby
    //    batch-flushes) the tracer provider, which is what actually moves
    //    the finished span into the in-memory exporter's buffer.
    app.stop().await.unwrap();

    let spans = exporter.get_finished_spans().unwrap();
    let request_spans: Vec<_> = spans.iter().filter(|s| s.name == "http.request").collect();
    assert_eq!(request_spans.len(), 1, "expected exactly one http.request span, got: {spans:?}");

    let trace_id = request_spans[0].span_context.trace_id().to_string();
    assert_eq!(trace_id, "0af7651916cd43dd8448eb211c80319c", "exported span did not carry the extracted trace id");
}
