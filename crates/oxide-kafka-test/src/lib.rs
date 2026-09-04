//! Test support for the Kafka starter — the `@EmbeddedKafka` analogue.
//!
//! # Isolation
//!
//! One broker is shared across the run, so isolation happens at the **topic
//! and consumer group** level: [`KafkaTest`] hands out names unique to the
//! test that asked for them.
//!
//! This matters more here than for the other starters. Reusing a topic
//! between tests means one test consumes another's messages; reusing a group
//! id means the second test starts at the first one's committed offset and
//! sees nothing. Both fail only under parallelism, and both look like
//! flakiness rather than a naming problem.
//!
//! Deleting and recreating a shared topic is not a way out: doing that while
//! a consumer group still holds committed offsets for the old incarnation
//! crashes Redpanda v24.2.1 outright (an assertion in `__consumer_offsets`).
//! Unique names avoid the situation entirely.
//!
//! # Waiting
//!
//! Kafka is asynchronous, so an assertion made immediately after producing
//! is a race. [`KafkaTest::await_until`] polls to a deadline and reports
//! what it last saw, which is the difference between a test that fails with
//! a useful message and one that fails with `assert_eq!(0, 1)`.

use oxide_boot::{Application, OxideApplication};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub use oxide_test::containers::ContainerHandle;

/// Starts (or attaches to) the shared Redpanda broker and returns its
/// `bootstrap.servers` value.
pub async fn redpanda() -> ContainerHandle {
    oxide_test::containers::redpanda().await
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A name unique to this test, suffixed onto `stem`, e.g.
/// `orders-91423-7`.
pub fn unique_name(stem: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{stem}-{}-{n}", std::process::id())
}

/// A booted application wired to the shared broker, with a consumer group id
/// unique to this test.
pub struct KafkaTest {
    app: Application,
    brokers: String,
    group_id: String,
    _dir: tempfile::TempDir,
}

impl KafkaTest {
    /// Boots an application whose only configured starter is Kafka, on
    /// ephemeral HTTP ports, with a unique consumer group.
    pub async fn start() -> Self {
        Self::builder().start().await
    }

    pub fn builder() -> KafkaTestBuilder {
        KafkaTestBuilder { extra_yaml: String::new(), env: HashMap::new() }
    }

    /// The broker address, for building a raw producer or consumer.
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// The consumer group id this test's listeners use.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    pub fn app(&self) -> &Application {
        &self.app
    }

    /// Produces one JSON message and waits for the broker to acknowledge it.
    ///
    /// Awaits the delivery report rather than firing and forgetting: a test
    /// that asserts on a message the broker never accepted would otherwise
    /// fail somewhere far from the cause.
    pub async fn produce<T: serde::Serialize>(&self, topic: &str, key: &str, value: &T) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("message.timeout.ms", "10000")
            .create()
            .expect("build a test producer");
        let payload = serde_json::to_string(value).expect("serialize the test payload");
        let record: FutureRecord<str, String> = FutureRecord::to(topic).key(key).payload(&payload);
        producer
            .send(record, Duration::from_secs(10))
            .await
            .unwrap_or_else(|(e, _)| panic!("producing to '{topic}' failed: {e}"));
    }

    /// Polls `condition` until it returns true, or fails after `within`.
    ///
    /// `what` names the thing being waited for and appears in the failure,
    /// so a timeout says which expectation was not met rather than only that
    /// one was not.
    pub async fn await_until<F>(&self, what: &str, within: Duration, mut condition: F)
    where
        F: FnMut() -> bool,
    {
        let deadline = std::time::Instant::now() + within;
        loop {
            if condition() {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "timed out after {within:?} waiting for {what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn stop(self) -> Result<(), oxide_boot::BootError> {
        self.app.stop().await
    }
}

/// Configures a [`KafkaTest`] before it boots.
pub struct KafkaTestBuilder {
    extra_yaml: String,
    env: HashMap<String, String>,
}

impl KafkaTestBuilder {
    /// Appends YAML under the `oxide:` key. Indentation is the caller's.
    pub fn with_yaml(mut self, yaml: &str) -> Self {
        self.extra_yaml.push_str(yaml);
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub async fn start(self) -> KafkaTest {
        let brokers = redpanda().await.endpoint;
        let group_id = unique_name("test-group");
        let dir = tempfile::tempdir().expect("tempdir for application.yml");

        let yaml = format!(
            "oxide:\n  application:\n    name: kafka-test\n  server:\n    port: 0\n  management:\n    port: 0\n  kafka:\n    bootstrap-servers: \"{brokers}\"\n{}",
            self.extra_yaml
        );
        std::fs::write(dir.path().join("application.yml"), yaml).expect("write application.yml");

        // The group id reaches listeners through the environment rather than
        // the YAML, because `#[kafka_listener(group_id = ...)]` takes a
        // literal and cannot be made unique at compile time.
        let mut env = self.env;
        env.entry("OXIDE_KAFKA_CONSUMER_GROUP_ID".to_string()).or_insert_with(|| group_id.clone());

        let app = OxideApplication::builder()
            .base_dir(dir.path())
            .env(env)
            .start()
            .await
            .expect("boot an application with the kafka starter");

        KafkaTest { app, brokers, group_id, _dir: dir }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_unique_and_keep_their_stem() {
        let a = unique_name("orders");
        let b = unique_name("orders");
        assert_ne!(a, b);
        assert!(a.starts_with("orders-"), "stem lost: {a}");
    }

    #[test]
    fn names_are_valid_kafka_identifiers() {
        let name = unique_name("orders");
        // Kafka allows [a-zA-Z0-9._-] and caps names at 249 characters.
        assert!(name.len() < 250, "too long: {name}");
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')),
            "invalid characters: {name}"
        );
    }
}
