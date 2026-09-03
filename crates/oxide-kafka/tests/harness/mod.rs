//! Redpanda test harness shared by the oxide-kafka integration-test binaries.
//!
//! Each test binary attaches to the one shared, *reused* Redpanda container
//! via `oxide_test::containers::redpanda()` (testcontainers is expensive;
//! that fixture caches the handle in a process-wide `OnceCell` and owns the
//! wait-for-broker-readiness logic this harness used to own directly — see
//! its module docs for why no `libc::atexit` cleanup hook is needed, and for
//! why the container is reused across the whole `cargo test` invocation and
//! beyond rather than being one-per-binary). Call the async
//! `bootstrap_servers()` (or `kafka_props()`) from within a tokio runtime to
//! resolve it. Tests in a binary serialize on the established BOOT_LOCK
//! discipline; consumer GROUPS are unique per test (`{test}-{nanos}`, via
//! `unique_topic`) so binaries are repeatable — but because the broker
//! itself now persists across separate `cargo test` invocations, a topic
//! with a fixed (non-unique) name can carry records left over from an
//! earlier invocation into a fresh consumer group's `earliest` read. Tests
//! that assume an empty topic must call `reset_topic` on it before
//! producing, rather than relying on the topic being fresh just because the
//! group is.

#![allow(dead_code)]

use oxide_kafka::{ConsumerProps, KafkaProps, ProducerProps, RetryProps};
use std::time::Duration;
use tokio::sync::OnceCell;

/// The Redpanda `bootstrap.servers` string, cached per process.
static BOOTSTRAP_SERVERS: OnceCell<String> = OnceCell::const_new();

/// The Redpanda `bootstrap.servers` string for the running shared container.
/// Must be awaited from within a tokio runtime.
pub async fn bootstrap_servers() -> &'static str {
    BOOTSTRAP_SERVERS.get_or_init(|| async { oxide_test::containers::redpanda().await.endpoint }).await
}

/// A topic name unique to the given test/prefix.
pub fn unique_topic(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// Builds a `KafkaProps` pointing at the shared Redpanda container, with a
/// small retry backoff so integration tests stay fast. Must be awaited from
/// within a tokio runtime (it triggers the container start).
pub async fn kafka_props() -> KafkaProps {
    let bootstrap = bootstrap_servers().await.to_string();
    let retry = RetryProps { backoff_initial_ms: 50, ..Default::default() };
    KafkaProps {
        bootstrap_servers: bootstrap,
        consumer: ConsumerProps { retry, ..Default::default() },
        producer: ProducerProps::default(),
        properties: [("message.timeout.ms".to_string(), "10000".to_string())].into_iter().collect(),
    }
}

/// Sleeps `ms` before continuing (keeps tests from busy-looping polls).
pub fn wait(ms: u64) {
    std::thread::sleep(Duration::from_millis(ms));
}

/// Deletes (if present) and recreates `topic` with `partitions` partitions,
/// giving a test a clean slate regardless of records a previous `cargo
/// test` invocation left behind on the shared, reused broker (see module
/// docs). Call this before producing to any topic whose test assumes it
/// starts empty. Must be awaited from within a tokio runtime.
pub async fn reset_topic(topic: &str, partitions: i32) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", bootstrap_servers().await);
    let admin: AdminClient<DefaultClientContext> = config.create().expect("create admin client for topic reset");

    // Best-effort: ignore "does not exist" (nothing to clean up on a topic
    // that has never been created, e.g. the very first invocation ever).
    let _ = admin.delete_topics(&[topic], &AdminOptions::new()).await;

    // Recreate fresh, retrying briefly: a just-issued delete can still be
    // in flight, so an immediate create can transiently fail with "topic
    // already exists".
    let mut last_err = None;
    for attempt in 0..20 {
        let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
        match admin.create_topics(&[new_topic], &AdminOptions::new()).await {
            Ok(results) if results.iter().all(|r| r.is_ok()) => return,
            Ok(results) => last_err = Some(format!("{results:?}")),
            Err(e) => last_err = Some(format!("{e}")),
        }
        if attempt < 19 {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }
    panic!("reset_topic({topic}): create_topics did not succeed after retries: {last_err:?}");
}
