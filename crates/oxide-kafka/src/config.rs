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
fn default_max_backoff_millis() -> u64 {
    30_000
}
fn default_idempotence() -> bool {
    true
}
fn default_shutdown_timeout_seconds() -> u64 {
    30
}

/// Retry / dead-letter configuration for the listener machinery.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
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
    /// Upper bound on any single backoff sleep, regardless of how large the
    /// geometric growth would otherwise make it — binds automatically from
    /// `max-backoff-millis` via `rename_all = "kebab-case"`.
    #[serde(default = "default_max_backoff_millis")]
    pub max_backoff_millis: u64,
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
            max_backoff_millis: default_max_backoff_millis(),
            dlt_enabled: default_dlt_enabled(),
        }
    }
}

/// Consumer-side listener tuning.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ConsumerProps {
    #[serde(default = "default_auto_offset_reset")]
    pub auto_offset_reset: String,
    /// Max records processed concurrently across all partitions; a partition
    /// is always processed strictly sequentially.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default)]
    pub retry: RetryProps,
    /// How long `pre_destroy` waits for in-flight partition workers to
    /// finish (and commit their offsets) before giving up and continuing
    /// shutdown anyway.
    #[serde(default = "default_shutdown_timeout_seconds")]
    pub shutdown_timeout_seconds: u64,
}

impl ConsumerProps {
    pub fn shutdown_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.shutdown_timeout_seconds)
    }
}

impl Default for ConsumerProps {
    fn default() -> Self {
        Self {
            auto_offset_reset: default_auto_offset_reset(),
            concurrency: default_concurrency(),
            retry: RetryProps::default(),
            shutdown_timeout_seconds: default_shutdown_timeout_seconds(),
        }
    }
}

/// Producer-side tuning.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
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
#[serde(rename_all = "kebab-case")]
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
        assert_eq!(c.retry.max_backoff_millis, 30_000);
        assert!(c.retry.dlt_enabled);
        assert_eq!(c.shutdown_timeout_seconds, 30);
        assert_eq!(c.shutdown_timeout(), std::time::Duration::from_secs(30));
    }

    #[test]
    fn producer_props_default_idempotent() {
        assert!(ProducerProps::default().idempotence);
    }

    #[test]
    fn retry_props_bind_documented_kebab_keys() {
        let props: RetryProps = serde_json::from_str(
            r#"{"max-attempts": 7, "backoff-initial-ms": 250, "backoff-multiplier": 3.5, "max-backoff-millis": 12000, "dlt-enabled": false}"#,
        )
        .unwrap();
        assert_eq!(props.max_attempts, 7);
        assert_eq!(props.backoff_initial_ms, 250);
        assert_eq!(props.backoff_multiplier, 3.5);
        assert_eq!(props.max_backoff_millis, 12000);
        assert!(!props.dlt_enabled);
    }

    /// `max_backoff_millis` binds from `max-backoff-millis` purely via
    /// `rename_all = "kebab-case"` — no per-field `#[serde(rename = ...)]`
    /// is needed (Task 13b's contract for new fields).
    #[test]
    fn retry_props_max_backoff_millis_binds_via_rename_all() {
        let props: RetryProps = serde_json::from_str(r#"{"max-backoff-millis": 5000}"#).unwrap();
        assert_eq!(props.max_backoff_millis, 5000);
    }

    #[test]
    fn consumer_props_bind_documented_kebab_keys() {
        let props: ConsumerProps = serde_json::from_str(r#"{"auto-offset-reset": "latest"}"#).unwrap();
        assert_eq!(props.auto_offset_reset, "latest");
    }

    /// `shutdown_timeout_seconds`'s explicit `#[serde(rename = "...")]` was
    /// removed as redundant now that `ConsumerProps` carries
    /// `rename_all = "kebab-case"`; this proves the documented key still
    /// binds via `rename_all` alone.
    #[test]
    fn consumer_props_shutdown_timeout_still_binds_via_rename_all() {
        let props: ConsumerProps = serde_json::from_str(r#"{"shutdown-timeout-seconds": 45}"#).unwrap();
        assert_eq!(props.shutdown_timeout_seconds, 45);
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
