//! Integration test binary 3: retry + backoff + dead-letter-topic E2E.
//!
//! Case 1 (flaky): a handler fails the first two attempts and succeeds on the
//!   third ⇒ exactly `max_attempts` (3) invocations and NO DLT record.
//! Case 2 (permanent): a handler always fails with `max_attempts = 2` ⇒ the
//!   record lands in `<topic>.DLT` with correct payload and the four DLT
//!   headers, and the original offset is committed (no redelivery on reboot).

#[path = "harness/mod.rs"]
mod harness;

use oxide_boot::OxideApplication;
use oxide_kafka::kafka_listeners;
use rdkafka::Message;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Headers;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Payload {
    seq: u32,
}

static FLAKY_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static FAILING_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static POISON_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

/// Serializes the two retry tests: both register the same listener components,
/// and both boot their own app, so overlapping runs would let one test's
/// listener consume the other test's topic. A global lock keeps them isolated.
static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[oxide_boot::component]
struct FlakyListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl FlakyListener {
    #[kafka_listener(topic = "retry-flaky")]
    async fn on_event(&self, _payload: Payload) -> Result<(), String> {
        let n = FLAKY_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        if n < 2 { Err("flaky failure".to_string()) } else { Ok(()) }
    }
}

#[oxide_boot::component]
struct FailingListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl FailingListener {
    #[kafka_listener(topic = "retry-fail")]
    async fn on_event(&self, _payload: Payload) -> Result<(), String> {
        FAILING_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        Err("always fails".to_string())
    }
}

#[oxide_boot::component]
struct PoisonPillListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl PoisonPillListener {
    #[kafka_listener(topic = "retry-poison")]
    async fn on_event(&self, _payload: Payload) -> Result<(), String> {
        POISON_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        // The handler is never actually invoked for a malformed payload
        // (deserialization fails first), so a successful return here would
        // indicate a bug in the routing, not a real handler outcome.
        Ok(())
    }
}

/// Boots an app with the given kafka retry config and a fresh consumer group.
async fn boot(group: &str, max_attempts: u32, backoff_ms: u64) -> (tempfile::TempDir, oxide_boot::Application) {
    let dir = tempfile::tempdir().expect("tempdir");
    let yml = format!(
        "oxide:\n  application:\n    name: \"{group}\"\n  kafka:\n    bootstrap-servers: \"{}\"\n    consumer:\n      retry:\n        max-attempts: {max_attempts}\n        backoff-initial-ms: {backoff_ms}\n",
        harness::bootstrap_servers().await
    );
    std::fs::write(dir.path().join("application.yml"), yml).expect("write application.yml");
    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
        .start()
        .await
        .expect("boot with retry config");
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

/// True when the counter hasn't changed across `polls` consecutive 150ms samples.
fn is_stable(counter: &AtomicUsize, polls: usize) -> bool {
    let mut prev = counter.load(Ordering::SeqCst);
    for _ in 0..polls {
        std::thread::sleep(Duration::from_millis(150));
        let cur = counter.load(Ordering::SeqCst);
        if cur != prev {
            prev = cur;
        }
    }
    let end = counter.load(Ordering::SeqCst);
    end == prev
}

/// Polls `topic` for its first record, returning `(payload, headers,
/// elapsed)` where `elapsed` is measured from `since` — used to prove a
/// poison pill reaches the DLT immediately rather than after a retry
/// backoff sleep. `None` if nothing arrives within `timeout`.
async fn wait_for_first_record(
    topic: &str,
    since: Instant,
    timeout: Duration,
) -> Option<(Vec<u8>, Vec<(String, Vec<u8>)>, Duration)> {
    let bootstrap = harness::bootstrap_servers().await.to_string();
    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", &bootstrap);
    config.set("group.id", "read-topic-first");
    config.set("auto.offset.reset", "earliest");
    let consumer: BaseConsumer = config.create().expect("create reading consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(Ok(m)) = consumer.poll(Duration::from_millis(200)) {
            let headers = m
                .headers()
                .map(|h| {
                    h.iter().map(|h| (h.key.to_string(), h.value.map(|v| v.to_vec()).unwrap_or_default())).collect()
                })
                .unwrap_or_default();
            return Some((m.payload().unwrap_or_default().to_vec(), headers, since.elapsed()));
        }
    }
    None
}

/// Reads all records from `topic` with a raw consumer, returning
/// (payload, headers) tuples. Async because it needs the shared bootstrap
/// address (resolved in the caller's tokio runtime).
async fn read_topic(topic: &str) -> Vec<(Vec<u8>, Vec<(String, Vec<u8>)>)> {
    let bootstrap = harness::bootstrap_servers().await.to_string();
    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", &bootstrap);
    config.set("group.id", "read-topic");
    config.set("auto.offset.reset", "earliest");
    let consumer: BaseConsumer = config.create().expect("create reading consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut out = Vec::new();
    while Instant::now() < deadline {
        if let Some(Ok(m)) = consumer.poll(Duration::from_secs(1)) {
            let headers = m
                .headers()
                .map(|h| {
                    h.iter().map(|h| (h.key.to_string(), h.value.map(|v| v.to_vec()).unwrap_or_default())).collect()
                })
                .unwrap_or_default();
            out.push((m.payload().unwrap_or_default().to_vec(), headers));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn retry_stops_after_success_and_skips_dlt() {
    let _boot = BOOT_LOCK.lock().await;
    FLAKY_ATTEMPTS.store(0, Ordering::SeqCst);
    let group = harness::unique_topic("retry-flaky-g");
    let (_dir, app) = boot(&group, 3, 50).await;
    let producer = app.context().get::<oxide_kafka::KafkaProducer>().expect("producer");

    producer.send("retry-flaky", "k", &Payload { seq: 1 }).await.expect("produce");

    // max_attempts = 3 → exactly three invocations (1 initial + 2 retries).
    // Wait until the count stabilizes (a successful commit should stop all
    // further processing) before asserting the exact value.
    assert!(
        wait_until(
            || FLAKY_ATTEMPTS.load(Ordering::SeqCst) >= 3 && is_stable(&FLAKY_ATTEMPTS, 1),
            Duration::from_secs(30)
        )
        .await,
        "flaky listener should have been attempted 3 times, got {}",
        FLAKY_ATTEMPTS.load(Ordering::SeqCst)
    );
    assert_eq!(
        FLAKY_ATTEMPTS.load(Ordering::SeqCst),
        3,
        "no retries beyond success; saw {}",
        FLAKY_ATTEMPTS.load(Ordering::SeqCst)
    );

    app.stop().await.expect("stop");

    // No DLT record should have been produced for a message that succeeded.
    harness::wait(300);
    let dlt = read_topic("retry-flaky.DLT").await;
    assert!(dlt.is_empty(), "successful message must not be published to the DLT, got {dlt:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn permanent_failure_publishes_to_dlt_and_commits() {
    let _boot = BOOT_LOCK.lock().await;
    FAILING_ATTEMPTS.store(0, Ordering::SeqCst);
    let group = harness::unique_topic("retry-fail-g");
    let (_dir, app) = boot(&group, 2, 50).await;
    let producer = app.context().get::<oxide_kafka::KafkaProducer>().expect("producer");

    producer.send("retry-fail", "orig-key", &Payload { seq: 42 }).await.expect("produce");

    // max_attempts = 2 → two invocations, then DLT.
    assert!(
        wait_until(|| FAILING_ATTEMPTS.load(Ordering::SeqCst) >= 2, Duration::from_secs(30)).await,
        "failing listener should be attempted exactly 2 times, got {}",
        FAILING_ATTEMPTS.load(Ordering::SeqCst)
    );

    // Read the DLT record and check payload + all four diagnostic headers.
    let dlt = read_topic("retry-fail.DLT").await;
    assert_eq!(dlt.len(), 1, "exactly one DLT record expected, got {:?}", dlt.len());
    let (payload_bytes, headers) = &dlt[0];
    let body: Payload = serde_json::from_slice(payload_bytes).expect("payload preserved as JSON");
    assert_eq!(body, Payload { seq: 42 }, "payload identical in DLT");
    let hmap: HashMap<String, String> =
        headers.iter().map(|(k, v)| (k.to_ascii_lowercase(), String::from_utf8_lossy(v).to_string())).collect();
    assert_eq!(hmap.get("x-exception-message").map(String::as_str), Some("always fails"));
    assert_eq!(hmap.get("x-original-topic").map(String::as_str), Some("retry-fail"));
    assert_eq!(hmap.get("x-original-partition").map(String::as_str), Some("0"));
    assert!(hmap.contains_key("x-original-offset"), "x-original-offset present");

    // The original offset must be committed: on a reboot with the same group,
    // the message is NOT redelivered.
    let seen_before_reboot = FAILING_ATTEMPTS.load(Ordering::SeqCst);
    app.stop().await.expect("stop after DLT");

    harness::wait(500);
    let (_dir2, app2) = boot(&group, 2, 50).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    app2.stop().await.expect("second stop");

    assert_eq!(
        FAILING_ATTEMPTS.load(Ordering::SeqCst),
        seen_before_reboot,
        "no redelivery of the DLT-published record after a clean shutdown"
    );
}

/// A poison pill (malformed JSON the handler can never deserialize) must go
/// straight to the DLT after exactly one attempt — not be retried like a
/// transient failure. Proven two ways: (1) the handler is never invoked at
/// all (deserialization fails before it's called — `POISON_ATTEMPTS` stays
/// 0), and (2) the record reaches the DLT almost immediately even though
/// `backoff_initial_ms` is configured large enough (5s) that a genuine retry
/// would take far longer.
#[tokio::test(flavor = "multi_thread")]
async fn poison_pill_skips_retry_and_goes_straight_to_dlt() {
    let _boot = BOOT_LOCK.lock().await;
    POISON_ATTEMPTS.store(0, Ordering::SeqCst);
    let group = harness::unique_topic("retry-poison-g");
    let (_dir, app) = boot(&group, 3, 5000).await;
    let producer = app.context().get::<oxide_kafka::KafkaProducer>().expect("producer");

    let started = Instant::now();
    producer
        .send_raw("retry-poison", Some(b"poison-key"), b"{not valid json", None)
        .await
        .expect("produce malformed payload");

    let record = wait_for_first_record("retry-poison.DLT", started, Duration::from_secs(20)).await;
    let (payload_bytes, headers, elapsed) =
        record.expect("malformed payload must reach the DLT (no retry loop to wait out)");

    assert!(
        elapsed < Duration::from_secs(3),
        "poison pill reached the DLT after {elapsed:?}; a genuine retry \
         (backoff_initial_ms=5000, max_attempts=3) would take at least ~15s, \
         so this proves the retry loop was skipped entirely"
    );
    assert_eq!(
        POISON_ATTEMPTS.load(Ordering::SeqCst),
        0,
        "the handler must never run for a payload that fails to deserialize"
    );

    assert_eq!(payload_bytes, b"{not valid json", "original (malformed) payload preserved in the DLT");
    let hmap: HashMap<String, String> =
        headers.iter().map(|(k, v)| (k.to_ascii_lowercase(), String::from_utf8_lossy(v).to_string())).collect();
    assert!(
        hmap.get("x-exception-message").is_some_and(|m| m.contains("deserialize")),
        "exception header should mention the deserialize failure, got {hmap:?}"
    );
    assert_eq!(hmap.get("x-original-topic").map(String::as_str), Some("retry-poison"));
    assert!(hmap.contains_key("x-original-offset"), "x-original-offset present");

    // No additional DLT records: exactly one attempt, one publish.
    harness::wait(300);
    let dlt = read_topic("retry-poison.DLT").await;
    assert_eq!(dlt.len(), 1, "exactly one DLT record expected for the poison pill, got {:?}", dlt.len());

    app.stop().await.expect("stop after poison pill DLT");
}
