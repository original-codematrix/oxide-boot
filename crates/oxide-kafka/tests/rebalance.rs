//! Integration test binary: consumer-group rebalance handling.
//!
//! Task 13: today a partition revocation (another consumer joining the
//! group) is ignored entirely — the losing consumer's per-partition worker
//! keeps running, keeps draining whatever it already buffered, and keeps
//! committing offsets for a partition it no longer owns. That produces
//! duplicate processing: the new owner independently processes the same
//! records once it picks up the partition.
//!
//! A fully deterministic rebalance test is hard (exact rebalance timing is
//! not controllable from the test), so this pins the *observable effect*
//! instead, per the task brief: with two consumers in the same group on a
//! two-partition topic, every record produced across the whole run (before
//! and after the second consumer joins, including a burst produced right as
//! it joins so some of it is still buffered on the first consumer when the
//! revoke happens) must be processed EXACTLY ONCE — no duplicates — no
//! matter which of the two consumers ends up owning which partition. Each
//! record carries a globally unique id (assigned at produce time) so
//! duplicate processing is detected without needing partition/offset
//! visibility inside the handler (the `#[kafka_listener]` macro only
//! exposes the payload plus `MessageKey`/`MessageHeaders`).

#[path = "harness/mod.rs"]
mod harness;

use oxide_boot::OxideApplication;
use oxide_kafka::kafka_listeners;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

const TOPIC: &str = "rebalance-e2e";
const TOPIC_PARTITIONS: i32 = 2;
/// The handler sleeps briefly so a burst of same-key (same-partition)
/// records outruns processing and piles up in the partition worker's
/// channel — the exact window in which the pre-fix code kept processing
/// (and committing) buffered records for a partition it had just lost.
const HANDLER_SLEEP: Duration = Duration::from_millis(300);

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Payload {
    /// Globally unique across the whole test run (assigned at produce
    /// time), so double-processing is detectable without needing
    /// partition/offset visibility inside the handler.
    id: u32,
}

static NEXT_ID: AtomicU32 = AtomicU32::new(1);
static PROCESSED: Mutex<Vec<u32>> = Mutex::new(Vec::new());

#[oxide_boot::component]
struct RebalanceListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl RebalanceListener {
    #[kafka_listener(topic = "rebalance-e2e")]
    async fn on_event(&self, payload: Payload) -> Result<(), String> {
        tokio::time::sleep(HANDLER_SLEEP).await;
        PROCESSED.lock().unwrap().push(payload.id);
        Ok(())
    }
}

static NEXT_PORT: AtomicU32 = AtomicU32::new(28080);

/// Boots an application in the given consumer `group`, pointed at the shared
/// Redpanda container. Two apps in this test run concurrently (unlike the
/// other kafka integration binaries, which boot sequentially), so each gets
/// its own HTTP/management port to avoid a bind conflict.
async fn boot(group: &str) -> (tempfile::TempDir, oxide_boot::Application) {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = NEXT_PORT.fetch_add(2, Ordering::SeqCst);
    let yml = format!(
        "oxide:\n  application:\n    name: \"{group}\"\n  server:\n    port: {port}\n  management:\n    port: {}\n  kafka:\n    bootstrap-servers: \"{}\"\n",
        port + 1,
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

/// Resets `TOPIC` to `TOPIC_PARTITIONS` empty partitions via the admin API —
/// a plain produce would auto-create it with a single partition (which
/// cannot exercise a rebalance that splits ownership across two consumers),
/// and on the shared, reused broker (see harness module docs) an earlier
/// invocation may have left records or a different partition count behind,
/// so this deletes and recreates it rather than tolerating an existing one.
async fn create_topic() {
    harness::reset_topic(TOPIC, TOPIC_PARTITIONS).await;
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

fn next_id() -> u32 {
    NEXT_ID.fetch_add(1, Ordering::SeqCst)
}

fn processed_count() -> usize {
    PROCESSED.lock().unwrap().len()
}

/// Sends one record with a fresh, globally-unique id and records that id in
/// `produced` — the caller-local ledger the final assertion checks the
/// processed set against exactly (catching both duplicates AND loss, unlike
/// a bare "no duplicates" check).
async fn produce(producer: &oxide_kafka::KafkaProducer, key: &str, produced: &mut Vec<u32>) {
    let id = next_id();
    producer.send(TOPIC, key, &Payload { id }).await.expect("produce record");
    produced.push(id);
}

#[tokio::test(flavor = "multi_thread")]
async fn revoked_partition_is_not_processed_twice_across_a_rebalance() {
    PROCESSED.lock().unwrap().clear();
    create_topic().await;

    let group = format!("rebalance-group-{}", harness::unique_topic("g"));

    let (_dir1, app1) = boot(&group).await;
    let producer = app1.context().get::<oxide_kafka::KafkaProducer>().expect("KafkaProducer registered");

    let mut produced: Vec<u32> = Vec::new();

    // Find a key that the default partitioner routes to each of the two
    // partitions, producing a probe record for each along the way. This can
    // take more than `TOPIC_PARTITIONS` sends (a later key can land on a
    // partition already covered), so the exact count only becomes known
    // once the loop below finishes — never assume it's 2.
    let mut key_for_partition: HashMap<i32, String> = HashMap::new();
    for i in 0..64 {
        if key_for_partition.len() == TOPIC_PARTITIONS as usize {
            break;
        }
        let key = format!("k{i}");
        let id = next_id();
        let delivery = producer.send(TOPIC, &key, &Payload { id }).await.expect("produce probe");
        produced.push(id);
        key_for_partition.entry(delivery.partition).or_insert(key);
    }
    assert_eq!(key_for_partition.len(), TOPIC_PARTITIONS as usize, "need a key per partition to run this test");
    // We don't know in advance which partition the rebalance below will move
    // to the second consumer (it depends on the assignor), so burst BOTH
    // partitions — whichever one moves, some of its burst is still buffered
    // and unprocessed on app1 when the revoke happens.
    let burst_keys: Vec<String> = key_for_partition.values().cloned().collect();

    // Give the single consumer (it owns both partitions so far) time to
    // drain the probe records above (however many it took to find a key per
    // partition).
    assert!(
        wait_until(|| processed_count() >= produced.len(), Duration::from_secs(15)).await,
        "app1 never processed the probes"
    );

    // Produce a burst of same-key (same-partition) records per partition.
    // With a `HANDLER_SLEEP` of 300ms and 4-way concurrency (both partition
    // workers run in parallel), each partition's burst takes >1.5s to
    // drain — plenty of time to boot the second consumer while several
    // records are still buffered and unprocessed on app1.
    const BURST_PER_PARTITION: u32 = 6;
    for _ in 0..BURST_PER_PARTITION {
        for key in &burst_keys {
            produce(&producer, key, &mut produced).await;
        }
    }

    // Boot the second consumer in the SAME group while the burst is still
    // draining: this forces the rebalance during the exact window the
    // pre-fix code got wrong.
    let (_dir2, app2) = boot(&group).await;

    // Let everything settle: the burst must drain (by whichever consumer
    // ends up owning its partition after the rebalance) and the rebalance
    // must complete.
    assert!(
        wait_until(|| processed_count() >= produced.len(), Duration::from_secs(30)).await,
        "burst was not fully processed after the rebalance settled"
    );
    // A little extra grace: nothing async should still be in flight, but
    // give any trailing duplicate a chance to show up before we assert.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // A final record on each partition proves both consumers remain healthy
    // (own a partition and keep processing) after the rebalance.
    let post_rebalance_keys: Vec<String> = key_for_partition.values().cloned().collect();
    for key in &post_rebalance_keys {
        produce(&producer, key, &mut produced).await;
    }
    assert!(
        wait_until(|| processed_count() >= produced.len(), Duration::from_secs(15)).await,
        "post-rebalance confirmation records were not processed"
    );

    app1.stop().await.expect("app1 stops cleanly");
    app2.stop().await.expect("app2 stops cleanly");

    // The core assertion: the processed multiset is EXACTLY the produced
    // set — every record processed exactly once. A plain "no duplicates"
    // check (the previous version of this test) cannot catch a LOST record
    // (e.g. a revoked partition's tail silently dropped rather than handed
    // off), only a doubly-processed one; comparing the full sorted set
    // catches both.
    let mut got = PROCESSED.lock().unwrap().clone();
    got.sort_unstable();
    let mut want = produced.clone();
    want.sort_unstable();
    assert_eq!(
        got, want,
        "processed ids must exactly match produced ids (no duplicates, no loss) across the rebalance"
    );
}
