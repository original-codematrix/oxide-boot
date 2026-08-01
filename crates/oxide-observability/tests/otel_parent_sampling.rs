//! Discriminates `Sampler::ParentBased(TraceIdRatioBased(ratio))` from a
//! bare `Sampler::TraceIdRatioBased(ratio)`, in its OWN test binary (same
//! "one global tracer provider per process" reasoning as `tests/otel.rs` —
//! see that file's module doc for why this can't share a binary with any
//! other `#[tokio::test]` that also boots an app).
//!
//! `tests/otel.rs` proves the extraction→parent→export chain works, but it
//! does so with `sample_ratio: 1.0` and a *sampled* traceparent (`-01`).
//! That combination can't tell `ParentBased(TraceIdRatioBased(1.0))` apart
//! from a bare always-on sampler (or even a bare `TraceIdRatioBased(1.0)`,
//! which samples everything regardless of any parent) — every candidate
//! sampler would export the span, so that test alone doesn't prove the
//! `ParentBased` wrapping is actually doing anything.
//!
//! This test closes that gap: it installs the provider with `sample_ratio:
//! 0.0` — normally "never sample" — but sends a request whose traceparent
//! marks the PARENT as sampled (`-01`). The only way the exported span
//! shows up is if the sampler *respects the parent's sampled decision*
//! instead of applying the ratio to the trace ID directly:
//! - `Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(0.0)))` — the
//!   correct, implemented sampler — delegates to the ratio-based root
//!   sampler ONLY for spans with no parent (or an unsampled parent); a
//!   sampled remote parent short-circuits straight to "sampled". The span
//!   IS exported.
//! - A bare `Sampler::TraceIdRatioBased(0.0)` — what a regression to
//!   "forgot to wrap in ParentBased" would look like — ignores the parent
//!   entirely and applies the 0.0 ratio directly to the trace ID, which
//!   deterministically means "never sample". The span would NOT be
//!   exported, and this test would fail.
//!
//! (Confirmed by temporarily swapping `build_provider`'s sampler to the bare
//! `Sampler::TraceIdRatioBased(sample_ratio)` and re-running this test: it
//! failed with "expected exactly one http.request span, got: []" — see
//! task-9-report.md for the pasted RAW RED output.)

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
        "oxide:\n  application:\n    name: otel-parent-sampling-test\n  server:\n    port: 0\n  management:\n    port: 0\n",
    )
    .unwrap();
    dir
}

#[tokio::test]
async fn sampled_parent_is_exported_even_with_ratio_zero() {
    // 1. Install with sample_ratio 0.0 — a bare ratio sampler would drop
    //    every span here, sampled parent or not.
    let exporter = oxide_observability::otel::testing::install_in_memory_with_ratio(0.0);

    // 2. Boot the app (ports 0 so this test doesn't collide with others).
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let main = app.context().get::<oxide_web::WebServer>().unwrap().local_addr().unwrap();

    // 3. GET with a traceparent whose flags byte (`01`) marks the PARENT as
    //    sampled — this is the fact `ParentBased` must respect for the span
    //    to survive a 0.0 ratio.
    let client = reqwest::Client::new();
    let res = client
        .get(format!("http://{main}/ping/world"))
        .header("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    // 4. Stop the app — flushes the batch processor (see tests/otel.rs for
    //    why this has to go through the real shutdown path).
    app.stop().await.unwrap();

    let spans = exporter.get_finished_spans().unwrap();
    let request_spans: Vec<_> = spans.iter().filter(|s| s.name == "http.request").collect();
    assert_eq!(
        request_spans.len(),
        1,
        "expected the sampled-parent span to be exported despite sample_ratio 0.0 (this fails if the sampler \
         isn't actually ParentBased — see this file's module doc), got: {spans:?}"
    );

    let trace_id = request_spans[0].span_context.trace_id().to_string();
    assert_eq!(trace_id, "0af7651916cd43dd8448eb211c80319c", "exported span did not carry the extracted trace id");
}
