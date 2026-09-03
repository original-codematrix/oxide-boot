//! Integration test binary 1: Redpanda harness smoke + producer E2E.

#[path = "harness/mod.rs"]
mod harness;

use oxide_boot::OxideApplication;
use oxide_kafka::{KafkaProducer, KafkaProps};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tempfile::tempdir;

use rdkafka::Message;
use rdkafka::consumer::{BaseConsumer, Consumer};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Note {
    id: u32,
    text: String,
}

/// Serializes every test in this file that boots a full app: each boot binds
/// the management server, and running them concurrently (the default for a
/// `cargo test` binary) races them for the same port — see
/// `crates/oxide-kafka/tests/retry_dlt.rs` for the established pattern.
static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Env map requesting ephemeral (OS-assigned) ports for both the app server
/// and the management server, so parallel/repeated test runs never collide
/// on a fixed port (e.g. the default management port 8081).
fn ephemeral_ports_env() -> HashMap<String, String> {
    [("OXIDE_SERVER_PORT".to_string(), "0".to_string()), ("OXIDE_MANAGEMENT_PORT".to_string(), "0".to_string())]
        .into_iter()
        .collect()
}

/// Boots a minimal application with a kafka config pointing at the shared
/// Redpanda container, returning the running application.
async fn boot_with_kafka_config() -> (tempfile::TempDir, oxide_boot::Application) {
    let dir = tempdir().expect("tempdir");
    let yml = format!("oxide:\n  kafka:\n    bootstrap-servers: \"{}\"\n", harness::bootstrap_servers().await);
    std::fs::write(dir.path().join("application.yml"), yml).expect("write application.yml");
    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(ephemeral_ports_env())
        .start()
        .await
        .expect("boot oxide app");
    (dir, app)
}

/// Reads records from `topic` with a raw consumer (default offset earliest),
/// polling for up to ~5s. Returns owned `(payload, key)` pairs.
fn read_all(topic: &str, props: &KafkaProps) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let consumer: BaseConsumer = props
        .client_config(&[("auto.offset.reset", "earliest".to_string()), ("group.id", "readall".to_string())])
        .create()
        .expect("create reading consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(6);
    let mut out = Vec::new();
    while std::time::Instant::now() < deadline {
        match consumer.poll(std::time::Duration::from_secs(1)) {
            Some(Ok(m)) => out.push((m.payload().unwrap_or_default().to_vec(), m.key().map(|k| k.to_vec()))),
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break,
            Some(Err(_)) => continue,
            None => continue,
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn harness_smoke_metadata_fetch_reaches_broker() {
    let props = harness::kafka_props().await;
    let consumer: BaseConsumer = props.client_config(&[]).create().expect("create base consumer");
    let meta =
        consumer.fetch_metadata(None, std::time::Duration::from_secs(15)).expect("metadata fetch against redpanda");
    // Broker must be present so we know the container actually serves Kafka.
    let brokers = meta.brokers();
    assert!(!brokers.is_empty(), "expected at least one broker, got none");
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_delivers_json_and_key() {
    let _boot = BOOT_LOCK.lock().await;
    let (_dir, app) = boot_with_kafka_config().await;
    let producer = app.context().get::<KafkaProducer>().expect("KafkaProducer registered");
    let props = harness::kafka_props().await;
    let topic = harness::unique_topic("producer-e2e");

    let delivery = producer.send(&topic, "k1", &Note { id: 7, text: "hello".into() }).await;
    let delivery = delivery.expect("send succeeds");
    assert!(delivery.offset >= 0, "delivery has a valid offset");

    let records = read_all(&topic, &props);
    assert!(!records.is_empty(), "expected the produced record to be readable");

    let (body_bytes, key) = &records[0];
    let body: Note = serde_json::from_slice(body_bytes).expect("payload is JSON Note");
    assert_eq!(body, Note { id: 7, text: "hello".into() });
    assert_eq!(key.as_deref(), Some(&b"k1"[..]), "key is preserved");

    app.stop().await.expect("clean shutdown flushes producer");
}

/// `oxide.kafka.producer.send-timeout-seconds` / `.flush-timeout-seconds`
/// bind from YAML and a non-idempotent producer must NOT get an unbounded
/// `message.send.max.retries` — proven end-to-end (not just at the
/// `ProducerProps`/`client_config` unit-test level) by booting a real app
/// against the shared Redpanda container and confirming the producer still
/// sends successfully with these overrides in place.
#[tokio::test(flavor = "multi_thread")]
async fn producer_config_binds_timeouts_and_bounds_retries_when_not_idempotent() {
    let _boot = BOOT_LOCK.lock().await;
    let dir = tempdir().expect("tempdir");
    let yml = format!(
        "oxide:\n  kafka:\n    bootstrap-servers: \"{}\"\n    producer:\n      idempotence: false\n      send-timeout-seconds: 3\n      flush-timeout-seconds: 4\n",
        harness::bootstrap_servers().await
    );
    std::fs::write(dir.path().join("application.yml"), yml).expect("write application.yml");
    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(ephemeral_ports_env())
        .start()
        .await
        .expect("boot oxide app");

    let props = app.context().get::<KafkaProps>().expect("KafkaProps registered");
    assert_eq!(props.producer.send_timeout_seconds, 3);
    assert_eq!(props.producer.flush_timeout_seconds, 4);
    assert!(!props.producer.idempotence);

    let cfg = props.producer_client_config();
    assert!(
        cfg.get("message.send.max.retries").is_none(),
        "a non-idempotent producer must keep librdkafka's own bounded retry default"
    );

    let producer = app.context().get::<KafkaProducer>().expect("KafkaProducer registered");
    let topic = harness::unique_topic("producer-timeouts");
    producer
        .send(&topic, "k", &Note { id: 1, text: "ok".into() })
        .await
        .expect("send succeeds with the configured (non-default) timeouts");

    app.stop().await.expect("clean shutdown flushes producer within the configured flush timeout");
}

/// `oxide.kafka.producer.idempotence: true` (the default) DOES raise
/// `message.send.max.retries` to librdkafka's maximum.
#[tokio::test(flavor = "multi_thread")]
async fn idempotent_producer_config_raises_max_retries() {
    let props = harness::kafka_props().await;
    let cfg = props.producer_client_config();
    assert_eq!(cfg.get("enable.idempotence"), Some("true"));
    assert_eq!(cfg.get("message.send.max.retries"), Some(i32::MAX.to_string()).as_deref());
}

/// The `kafka` health indicator builds its consumer once (at registration,
/// not per-check) and reuses it — proven by asserting it keeps reporting
/// `UP` across several checks against the reachable Redpanda broker, rather
/// than only the very first one succeeding.
#[tokio::test(flavor = "multi_thread")]
async fn health_indicator_reuses_cached_consumer_across_checks() {
    let _boot = BOOT_LOCK.lock().await;
    let (_dir, app) = boot_with_kafka_config().await;
    let mgmt_addr = app.context().get::<oxide_web::ManagementServer>().unwrap().local_addr().unwrap();

    for attempt in 0..3 {
        let body: serde_json::Value =
            reqwest::get(format!("http://{mgmt_addr}/actuator/health")).await.unwrap().json().await.unwrap();
        assert_eq!(
            body["components"]["kafka"]["status"],
            serde_json::json!("UP"),
            "kafka health check #{attempt} should be UP against the reachable Redpanda broker: {body}"
        );
    }

    app.stop().await.expect("clean shutdown");
}
