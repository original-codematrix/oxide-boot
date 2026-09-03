//! Proves `OtelLifecycle::flush` bounds the OTel tracer provider shutdown
//! with `oxide.otel.shutdown-timeout-seconds` — and that the bound actually
//! protects *process exit*, not just `stop()`'s return — in its OWN test
//! binary (same rationale as `tests/otel.rs`: the process-global tracer
//! provider is a one-shot `OnceLock`, so this binary carries exactly ONE
//! test).
//!
//! What this simulates: `otel::testing::install_in_memory_with_hung_export`
//! installs a tracer provider whose exporter's `export()` hangs (via a real
//! `tokio::time::sleep`, an `.await`, not a synchronous block) for far
//! longer than the configured timeout — standing in for a hung/unreachable
//! OTLP collector never responding to a span export. That hang lives inside
//! `BatchSpanProcessor`'s own background task (spawned by
//! `opentelemetry_sdk`'s `runtime::Tokio` integration when the provider is
//! built, not by this crate), which is exactly why
//! `TracerProvider::shutdown()`'s blocking wait for that task's reply — the
//! call `OtelLifecycle::flush` bounds — can otherwise hang indefinitely.
//!
//! The test hits a real HTTP endpoint (same `PingController` pattern as
//! `tests/otel.rs`) before calling `stop()`, so a genuine `http.request`
//! span is sitting in `BatchSpanProcessor`'s buffer when shutdown runs.
//! This matters: `BatchSpanProcessorInternal::export` short-circuits with
//! an already-`Ok` future when its buffer is empty (see
//! `opentelemetry_sdk-0.27.1/src/trace/span_processor.rs`), so a shutdown
//! with nothing buffered never calls the exporter's `export()` at all and
//! the simulated hang would never actually be exercised.
//!
//! Why this test builds and drops its own `tokio::runtime::Runtime` by hand
//! instead of using `#[tokio::test]`: an earlier version of this test only
//! timed `app.stop().await`, which is necessary but not sufficient. A fix
//! that runs the blocking shutdown wait on `tokio::task::spawn_blocking`,
//! wrapped in `tokio::time::timeout`, *also* makes `stop()` return within
//! the bound — the difference only shows up afterwards, because tokio's
//! `Runtime::drop` waits for every outstanding `spawn_blocking` task to
//! finish before returning, however long that takes. `#[oxide_boot::main]`
//! (`crates/oxide-macros/src/main_macro.rs`) builds exactly this kind of
//! `Runtime` as a temporary around `block_on` and drops it immediately
//! after — so the real risk this task exists to prevent is the *process*
//! hanging at exit, not `stop()` hanging. Building the runtime explicitly
//! here and timing the whole `block_on` + `drop(runtime)` sequence — the
//! same shape `#[oxide_boot::main]`'s generated `fn main` has — is the most
//! direct way to pin that: `OtelLifecycle::flush` runs the blocking
//! shutdown wait on a detached `std::thread::spawn` (invisible to the
//! runtime, so `Runtime::drop` never waits for it) rather than
//! `spawn_blocking`, precisely so this sequence does NOT block on the hung
//! exporter.
//!
//! Honesty note: reverting `OtelLifecycle::flush` to the `spawn_blocking`
//! version while keeping this test unchanged does NOT make it fail — this
//! test alone does not discriminate between the two implementations.
//! `opentelemetry_sdk`'s own `Runtime::drop`-triggered task cancellation
//! drops the still-pending `BatchSpanProcessor` background task promptly
//! (it's suspended at a proper `.await`, not stuck mid-poll), which closes
//! the response channel `TracerProvider::shutdown()` is waiting on — so
//! *both* implementations happen to unblock quickly here, regardless of
//! whether the wait ran on `spawn_blocking` or a detached thread. That's
//! why this test exists (it proves the realistic "hung network export"
//! case stays bounded end to end, which is what actually happens in
//! production), but proving the `spawn_blocking`-vs-detached-thread choice
//! itself matters needs a scenario the runtime's own cancellation can't
//! rescue — see
//! `otel::tests::run_detached_with_timeout_does_not_delay_runtime_drop_when_the_closure_hangs`
//! in `src/otel.rs`, which reproduces that directly (a plain
//! `std::thread::sleep` closure, uncancellable by construction) and does
//! fail under `spawn_blocking` (confirmed by hand: ~3s instead of the
//! configured bound).

use oxide_boot::prelude::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[oxide_boot::component]
struct PingController {}

#[oxide_web::controller("/ping")]
impl PingController {
    #[get("/")]
    async fn ping(&self) -> &'static str {
        "pong"
    }
}

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: otel-shutdown-timeout-test\n  server:\n    port: 0\n  management:\n    port: 0\n  otel:\n    shutdown-timeout-seconds: 1\n",
    )
    .unwrap();
    dir
}

#[test]
fn hung_exporter_does_not_delay_process_exit_beyond_the_configured_timeout() {
    let dir = test_config_dir();

    // Same shape as `#[oxide_boot::main]`'s generated `fn main`: a
    // multi-threaded runtime built as a local value, `block_on` to drive the
    // boot/request/stop sequence, then dropped.
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("build tokio runtime");

    let started = Instant::now();
    runtime.block_on(async {
        // Installed from inside `block_on`, not before: `BatchSpanProcessor`
        // (`runtime::Tokio`) needs an active Tokio reactor at construction
        // time, which only exists once we're running inside this runtime.
        // The exporter's `export()` hangs for 5s; configured timeout is 1s.
        let _exporter = oxide_observability::otel::testing::install_in_memory_with_hung_export(Duration::from_secs(5));

        let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
        let addr = app.context().get::<oxide_web::WebServer>().unwrap().local_addr().unwrap();

        // Produces a real, buffered `http.request` span — see the module
        // doc comment on why an empty buffer would make this test vacuous.
        let res = reqwest::Client::new().get(format!("http://{addr}/ping")).send().await.unwrap();
        assert_eq!(res.status(), 200);

        app.stop().await.unwrap();
    });
    // The critical assertion: dropping the runtime is where a
    // `spawn_blocking`-based shutdown would have hung for the full 5s
    // (`Runtime::drop` awaits outstanding `spawn_blocking` tasks — and the
    // thread that calls `TracerProvider::shutdown()` blocks inside it for
    // the full 5s too, since that call itself waits on the hung background
    // task's export). A detached `std::thread::spawn` is untracked by the
    // runtime, so this drop must return promptly regardless of that
    // still-running thread and the still-hung background export task.
    drop(runtime);
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(3),
        "boot+request+stop+runtime-drop took {elapsed:?}, expected it to complete shortly after the 1s \
         shutdown-timeout-seconds bound, well before the exporter's simulated 5s hang — a duration at or near 5s \
         would mean the hung export is (again) blocking runtime teardown"
    );
}
