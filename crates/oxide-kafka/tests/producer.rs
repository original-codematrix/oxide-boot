//! Integration test binary 1: Redpanda harness smoke + producer E2E.

#[path = "harness/mod.rs"]
mod harness;

use oxide_boot::OxideApplication;
use oxide_kafka::{KafkaProducer, KafkaProps};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tempfile::tempdir;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::Message;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Note {
    id: u32,
    text: String,
}

/// Boots a minimal application with a kafka config pointing at the shared
/// Redpanda container, returning the running application.
async fn boot_with_kafka_config() -> (tempfile::TempDir, oxide_boot::Application) {
    let dir = tempdir().expect("tempdir");
    let yml = format!(
        "oxide:\n  kafka:\n    bootstrap-servers: \"{}\"\n",
        harness::bootstrap_servers().await
    );
    std::fs::write(dir.path().join("application.yml"), yml).expect("write application.yml");
    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
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
    let consumer: BaseConsumer = props
        .client_config(&[])
        .create()
        .expect("create base consumer");
    let meta = consumer
        .fetch_metadata(None, std::time::Duration::from_secs(15))
        .expect("metadata fetch against redpanda");
    // Broker must be present so we know the container actually serves Kafka.
    let brokers = meta.brokers();
    assert!(!brokers.is_empty(), "expected at least one broker, got none");
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_delivers_json_and_key() {
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