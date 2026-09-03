//! Integration test binary: shutdown must wait for an in-flight handler to
//! finish (and its offset to be committed) rather than cutting it off.
//!
//! Task 12: today `pre_destroy` only drops the per-partition worker senders
//! and returns — it never awaits the worker tasks. A handler still running
//! when the app stops is cut off mid-work and its offset is never committed,
//! so the message is redelivered after restart. This test pins the fixed
//! behaviour: the handler must run to completion during `app.stop()`, and a
//! fresh consumer in the same group must not see the message again.

#[path = "harness/mod.rs"]
mod harness;

use oxide_boot::OxideApplication;
use oxide_kafka::kafka_listeners;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const TOPIC: &str = "shutdown-inflight";
/// How long the handler blocks before signalling completion. Long enough
/// that `app.stop()` reliably observes it still running, short enough to
/// keep the test fast.
const HANDLER_SLEEP: Duration = Duration::from_secs(2);

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Payload {
    seq: u32,
}

static DELIVERIES: AtomicUsize = AtomicUsize::new(0);
static STARTED: AtomicBool = AtomicBool::new(false);
static FINISHED: AtomicBool = AtomicBool::new(false);

#[oxide_boot::component]
struct SlowListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl SlowListener {
    #[kafka_listener(topic = "shutdown-inflight")]
    async fn on_event(&self, _payload: Payload) -> Result<(), String> {
        DELIVERIES.fetch_add(1, Ordering::SeqCst);
        STARTED.store(true, Ordering::SeqCst);
        tokio::time::sleep(HANDLER_SLEEP).await;
        FINISHED.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// Boots an application with a kafka config and a fresh application name
/// (i.e. a fresh consumer group) pointing at the shared Redpanda container.
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
        .expect("boot oxide app with listener");
    (dir, app)
}

async fn wait_until(cond: impl Fn() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_waits_for_in_flight_handler_and_commits_its_offset() {
    DELIVERIES.store(0, Ordering::SeqCst);
    STARTED.store(false, Ordering::SeqCst);
    FINISHED.store(false, Ordering::SeqCst);

    let group = format!("shutdown-inflight-group-{}", harness::unique_topic("g"));
    // The topic is fixed; on the shared, reused broker (see harness module
    // docs) it can carry records left over from an earlier invocation, so
    // reset it before producing the single message this test expects.
    harness::reset_topic(TOPIC, 1).await;

    let (_dir1, app1) = boot(&group).await;
    let producer = app1.context().get::<oxide_kafka::KafkaProducer>().expect("KafkaProducer registered");
    producer.send(TOPIC, "k1", &Payload { seq: 1 }).await.expect("produce message");

    // Wait for the handler to actually start (and be mid-sleep) before
    // stopping — this is the scenario the fix must handle: shutdown
    // requested while a handler is in flight.
    assert!(wait_until(|| STARTED.load(Ordering::SeqCst), Duration::from_secs(10)).await, "handler never started");
    assert!(!FINISHED.load(Ordering::SeqCst), "test is racy: handler already finished before stop() was called");

    let stop_started = Instant::now();
    app1.stop().await.expect("app stops cleanly");
    let stop_elapsed = stop_started.elapsed();

    assert!(FINISHED.load(Ordering::SeqCst), "handler was cut off: shutdown did not wait for the in-flight worker");
    assert!(
        stop_elapsed >= HANDLER_SLEEP,
        "stop() returned ({stop_elapsed:?}) before the handler's {HANDLER_SLEEP:?} sleep could have completed; \
         shutdown did not actually wait for the worker"
    );

    // A fresh consumer in the SAME group must not see the message again: the
    // offset committed by the completed handler must have been flushed
    // before the consumer was torn down.
    let (_dir2, app2) = boot(&group).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    app2.stop().await.expect("second boot stops cleanly");

    assert_eq!(DELIVERIES.load(Ordering::SeqCst), 1, "message must not be redelivered after a clean shutdown");
}
