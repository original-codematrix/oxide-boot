//! Proves `OtelLifecycle::flush` bounds the OTel tracer provider shutdown
//! with `oxide.otel.shutdown-timeout-seconds`, in its OWN test binary (same
//! rationale as `tests/otel.rs`: the process-global tracer provider is a
//! one-shot `OnceLock`, so this binary carries exactly ONE test).
//!
//! What this simulates: `otel::testing::install_in_memory_with_hung_shutdown`
//! installs a tracer provider whose exporter's `shutdown()` performs a real,
//! synchronous `std::thread::sleep` for far longer than the configured
//! timeout — standing in for a hung or unreachable OTLP collector, which is
//! exactly the scenario `TracerProvider::shutdown()`'s blocking
//! `futures_executor::block_on` wait can't otherwise escape.
//!
//! What it proves: with `oxide.otel.shutdown-timeout-seconds: 1` configured
//! and a shutdown that would otherwise block for 5 seconds, `app.stop()`
//! still returns `Ok(())` well within a generous bound — i.e. the timeout in
//! `OtelLifecycle::flush` actually cuts the wait short instead of only
//! existing on paper.

use oxide_boot::prelude::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: otel-shutdown-timeout-test\n  server:\n    port: 0\n  management:\n    port: 0\n  otel:\n    shutdown-timeout-seconds: 1\n",
    )
    .unwrap();
    dir
}

// Multi-threaded: `BatchSpanProcessor`'s background task (which calls the
// exporter's synchronous `shutdown()`, i.e. our simulated 5s
// `std::thread::sleep`) runs *on the tokio runtime itself* (`runtime::Tokio`
// in `build_provider`), not on a `spawn_blocking` thread. On a
// single-threaded runtime that sleep would monopolize the only worker
// thread and starve the `tokio::time::timeout` timer driving
// `OtelLifecycle::flush`'s bound, defeating the very thing this test
// exists to prove. A second worker thread keeps the timeout's timer — and
// `app.stop()`'s own task — schedulable while the background task blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hung_exporter_shutdown_does_not_block_stop_beyond_the_configured_timeout() {
    // Exporter shutdown would block for 5s; configured timeout above is 1s.
    let _exporter = oxide_observability::otel::testing::install_in_memory_with_hung_shutdown(Duration::from_secs(5));

    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    let started = Instant::now();
    app.stop().await.unwrap();
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(3),
        "stop() took {elapsed:?}, expected it to return shortly after the 1s shutdown-timeout-seconds bound, \
         well before the exporter's simulated 5s hang"
    );
}
