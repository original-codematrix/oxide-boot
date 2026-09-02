//! Integration test binary 2: partition-ordered, at-least-once listener E2E.
//!
//! Proves the two core guarantees:
//! 1. ORDERING: 5 messages to the same key (one partition) are delivered in
//!    send order.
//! 2. COMMIT: after a first boot commits, a second boot on the same consumer
//!    group does NOT re-deliver (offsets are committed after handler success).

#[path = "harness/mod.rs"]
mod harness;

use oxide_boot::OxideApplication;
use oxide_kafka::kafka_listeners;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const LISTENER_TOPIC: &str = "listener-e2e";

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Payload {
    seq: u32,
}

static RECEIVED: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
fn received() -> &'static Mutex<Vec<u32>> {
    RECEIVED.get_or_init(|| Mutex::new(Vec::new()))
}

#[oxide_boot::component]
struct TestListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl TestListener {
    #[kafka_listener(topic = "listener-e2e")]
    async fn on_event(&self, payload: Payload) -> Result<(), String> {
        received().lock().expect("received lock").push(payload.seq);
        Ok(())
    }
}

/// Boots an application with a kafka config and a fresh application name
/// (i.e. a fresh consumer group) pointing at the shared Redpanda container.
async fn boot(dir_group: &str) -> (tempfile::TempDir, oxide_boot::Application) {
    let dir = tempfile::tempdir().expect("tempdir");
    let yml = format!(
        "oxide:\n  application:\n    name: \"{dir_group}\"\n  kafka:\n    bootstrap-servers: \"{}\"\n",
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

/// Waits (up to `timeout`) until the listener has received exactly `n` records.
async fn wait_until_len(n: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if received().lock().expect("lock").len() >= n {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    received().lock().expect("lock").len() >= n
}

#[tokio::test(flavor = "multi_thread")]
async fn listener_delivers_partition_ordered_then_no_redelivery() {
    received().lock().expect("lock").clear();
    // Each binary run gets a distinct group so stale committed offsets from a
    // previous execution don't interfere.
    let group = format!("listener-e2e-group-{}", harness::unique_topic("g"));

    // Boot 1: register the listener, produce 5 messages on one key.
    let (_dir1, app1) = boot(&group).await;
    let producer = app1.context().get::<oxide_kafka::KafkaProducer>().expect("KafkaProducer registered");

    for seq in 0..5u32 {
        producer.send(LISTENER_TOPIC, "k1", &Payload { seq }).await.expect("produce message");
    }

    // Ordering proof: all 5 delivered in send order on the same partition.
    assert!(
        wait_until_len(5, Duration::from_secs(30)).await,
        "listener should receive all 5 messages, got {:?}",
        received().lock().unwrap().clone()
    );
    let got: Vec<u32> = received().lock().expect("lock").clone();
    assert_eq!(got, vec![0, 1, 2, 3, 4], "messages delivered in partition (send) order");

    // Clean shutdown commits the offsets (stop runs pre_destroy → drain).
    app1.stop().await.expect("first boot stops cleanly");

    // Boot 2 on the SAME consumer group: offsets were committed in boot 1, so
    // no message may be re-delivered.
    harness::wait(500);
    let len_before_redelivery = received().lock().expect("lock").len();
    let (_dir2, app2) = boot(&group).await;
    // Give the second consumer a moment to (incorrectly) re-deliver if it were
    // going to, then stop.
    tokio::time::sleep(Duration::from_secs(5)).await;
    app2.stop().await.expect("second boot stops cleanly");

    let after = received().lock().expect("lock").len();
    assert_eq!(after, len_before_redelivery, "no redelivery after a clean shutdown: commits must have been persisted");
    assert_eq!(after, 5, "exactly the 5 original messages were seen in total");
}
