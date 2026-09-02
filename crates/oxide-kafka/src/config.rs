//! Kafka connection configuration, bound to the `oxide.kafka` prefix.
//!
//! `KafkaProps` is a required config (no `default` flag): if the crate is
//! linked and the `oxide.kafka` section is absent from `application.yml`,
//! boot fails fast — mirroring the `Spec §3` fail-fast contract for
//! integration starters.

use rdkafka::ClientConfig;

fn default_auto_offset_reset() -> String {
    "earliest".to_string()
}
fn default_concurrency() -> usize {
    4
}
fn default_max_attempts() -> u32 {
    3
}
fn default_backoff_initial_ms() -> u64 {
    100
}
fn default_backoff_multiplier() -> f64 {
    2.0
}
fn default_idempotence() -> bool {
    true
}

/// Retry / dead-letter configuration for the listener machinery.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RetryProps {
    /// Total attempts including the first. `3` = one initial try plus two
    /// retries. Consumers block on a failing record until it succeeds or the
    /// attempts are exhausted (ordering before throughput).
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_backoff_initial_ms")]
    pub backoff_initial_ms: u64,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
    /// Whether to publish exhausted records to `<topic>.DLT` before committing.
    #[serde(default = "default_dlt_enabled")]
    pub dlt_enabled: bool,
}

fn default_dlt_enabled() -> bool {
    true
}

impl Default for RetryProps {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            backoff_initial_ms: default_backoff_initial_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            dlt_enabled: default_dlt_enabled(),
        }
    }
}

/// Consumer-side listener tuning.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConsumerProps {
    #[serde(default = "default_auto_offset_reset")]
    pub auto_offset_reset: String,
    /// Max records processed concurrently across all partitions; a partition
    /// is always processed strictly sequentially.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default)]
    pub retry: RetryProps,
}

impl Default for ConsumerProps {
    fn default() -> Self {
        Self {
            auto_offset_reset: default_auto_offset_reset(),
            concurrency: default_concurrency(),
            retry: RetryProps::default(),
        }
    }
}

/// Producer-side tuning.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ProducerProps {
    #[serde(default = "default_idempotence")]
    pub idempotence: bool,
}

impl Default for ProducerProps {
    fn default() -> Self {
        Self { idempotence: default_idempotence() }
    }
}

/// Parsed `oxide.kafka` configuration. `bootstrap_servers` is required.
#[derive(Debug, Clone, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.kafka")]
pub struct KafkaProps {
    #[serde(rename = "bootstrap-servers")]
    pub bootstrap_servers: String,
    #[serde(default)]
    pub consumer: ConsumerProps,
    #[serde(default)]
    pub producer: ProducerProps,
    /// Raw librdkafka passthrough options (`String` -> `String`), applied to
    /// both producer and consumer client configs.
    #[serde(default)]
    pub properties: std::collections::HashMap<String, String>,
}

impl KafkaProps {
    /// Builds a base `rdkafka::ClientConfig` for either a producer or a
    /// consumer, applying `bootstrap.servers`, the `properties` passthrough,
    /// and any `extra` overrides (not otherwise configurable through the YAML
    /// shape, e.g. `enable.auto.commit`).
    pub fn client_config(&self, extra: &[(&str, String)]) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap_servers);
        for (key, value) in &self.properties {
            config.set(key.as_str(), value.as_str());
        }
        for (key, value) in extra {
            config.set(*key, value.as_str());
        }
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_props_defaults() {
        let c = ConsumerProps::default();
        assert_eq!(c.auto_offset_reset, "earliest");
        assert_eq!(c.concurrency, 4);
        assert_eq!(c.retry.max_attempts, 3);
        assert_eq!(c.retry.backoff_initial_ms, 100);
        assert_eq!(c.retry.backoff_multiplier, 2.0);
        assert!(c.retry.dlt_enabled);
    }

    #[test]
    fn producer_props_default_idempotent() {
        assert!(ProducerProps::default().idempotence);
    }

    #[test]
    fn client_config_applies_passthrough_and_extra() {
        let props = KafkaProps {
            bootstrap_servers: "localhost:9092".to_string(),
            consumer: ConsumerProps::default(),
            producer: ProducerProps::default(),
            properties: [("message.timeout.ms".to_string(), "5000".to_string())].into_iter().collect(),
        };
        let config = props.client_config(&[("enable.auto.commit", "false".to_string())]);
        assert_eq!(config.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(config.get("message.timeout.ms"), Some("5000"));
        assert_eq!(config.get("enable.auto.commit"), Some("false"));
    }
}