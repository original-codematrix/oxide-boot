//! Kafka health indicator: reports `/actuator/health` component `kafka`.
//!
//! Registered only when `KafkaProps` is present in the context (the register
//! seam returns `None` otherwise), so a kafka-less app never sees a hypothetical
//! `kafka` health component. The check runs a blocking rdkafka metadata fetch
//! on a blocking thread with a short timeout and reports `Up`/`Down{detail}`.

use crate::KafkaProps;
use oxide_core::Context;
use oxide_web::{HealthIndicator, HealthStatus, RegisteredCheck};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::sync::Arc;
use std::time::Duration;

/// The per-request budget for the metadata fetch (matches the health check
/// timeout in `oxide-web`).
const FETCH_TIMEOUT: Duration = Duration::from_secs(2);

fn check_metadata(consumer: &BaseConsumer) -> HealthStatus {
    match consumer.fetch_metadata(None, FETCH_TIMEOUT) {
        Ok(meta) if !meta.brokers().is_empty() => HealthStatus::Up,
        Ok(_) => HealthStatus::Down { detail: "no brokers reachable".to_string() },
        Err(e) => {
            tracing::warn!(error = %e, "kafka health check failed to fetch metadata");
            HealthStatus::Down { detail: "metadata fetch failed".to_string() }
        }
    }
}

/// Resolves the kafka health check for the current context (None if kafka is
/// not configured). The metadata fetch is blocking, so it runs on
/// `spawn_blocking`.
///
/// The `BaseConsumer` is built exactly once, here, and reused across every
/// subsequent check (rather than creating a fresh client per request) — this
/// only runs once at boot (see `oxide_web::health::resolve_indicators`), and
/// `ClientConfig::create()` for a consumer only validates the config; like
/// `KafkaProducer::new`, it does NOT connect to the broker eagerly. So a
/// still-unreachable broker at boot time does not fail this registration —
/// it just means the first (and every) check reports `Down` until the
/// broker becomes reachable, preserving the no-boot-time-broker-probe
/// contract (`tests/unreachable_broker.rs`).
fn register(ctx: &Context) -> Option<RegisteredCheck> {
    let props = (*ctx.get::<KafkaProps>()?).clone();
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &props.bootstrap_servers);
    config.set("socket.timeout.ms", "1500");
    let consumer: Arc<BaseConsumer> = match config.create() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            // Keep driver internals out of `detail` (surfaced by the health
            // endpoint); the real error still goes to the logs. A genuinely
            // invalid client config (not a connectivity problem) fails every
            // check from here on rather than boot itself.
            tracing::warn!(error = %e, "kafka health check failed to create consumer");
            return Some(Box::new(|| {
                Box::pin(async { HealthStatus::Down { detail: "failed to create consumer".to_string() } })
            }));
        }
    };
    Some(Box::new(move || {
        let consumer = Arc::clone(&consumer);
        Box::pin(async move {
            match tokio::task::spawn_blocking(move || check_metadata(&consumer)).await {
                Ok(status) => status,
                Err(_) => HealthStatus::Down { detail: "health check task failed".to_string() },
            }
        })
    }))
}

inventory::submit! {
    HealthIndicator {
        name: "kafka",
        register,
    }
}
