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
use std::time::Duration;

/// The per-request budget for the metadata fetch (matches the health check
/// timeout in `oxide-web`).
const FETCH_TIMEOUT: Duration = Duration::from_secs(2);

fn check_metadata(servers: &str) -> HealthStatus {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", servers);
    config.set("socket.timeout.ms", "1500");
    let consumer: BaseConsumer = match config.create() {
        Ok(c) => c,
        Err(e) => return HealthStatus::Down { detail: e.to_string() },
    };
    match consumer.fetch_metadata(None, FETCH_TIMEOUT) {
        Ok(meta) if !meta.brokers().is_empty() => HealthStatus::Up,
        Ok(_) => HealthStatus::Down { detail: "no brokers reachable".to_string() },
        Err(e) => HealthStatus::Down { detail: e.to_string() },
    }
}

/// Resolves the kafka health check for the current context (None if kafka is
/// not configured). The metadata fetch is blocking, so it runs on
/// `spawn_blocking`.
fn register(ctx: &Context) -> Option<RegisteredCheck> {
    let props = (*ctx.get::<KafkaProps>()?).clone();
    Some(Box::new(move || {
        let servers = props.bootstrap_servers.clone();
        Box::pin(async move {
            match tokio::task::spawn_blocking(move || check_metadata(&servers)).await {
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
