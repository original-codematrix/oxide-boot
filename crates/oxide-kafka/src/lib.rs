//! Oxide Boot Kafka starter: annotated listeners, an injectable producer,
//! retry + dead-letter topics, at-least-once offset handling and full
//! observability on top of `rdkafka`.

pub mod config;
mod health;
mod listener;
mod observe;
pub mod producer;
mod retry;

pub use config::{ConsumerProps, KafkaProps, ProducerProps, RetryProps};
pub use listener::{ListenerDescriptor, ListenerInfra, MessageHeaders, MessageKey, ReceivedMessage};
pub use oxide_kafka_macros::kafka_listeners;
pub use producer::{Delivery, KafkaProducer};

/// Errors produced by the Kafka starter.
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("failed to serialize message for topic '{topic}': {source}")]
    Serialize {
        topic: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to send message to topic '{topic}': {source}")]
    Send {
        topic: String,
        #[source]
        source: rdkafka::error::KafkaError,
    },
    #[error("failed to flush producer: {source}")]
    Flush {
        #[source]
        source: rdkafka::error::KafkaError,
    },
    #[error("kafka error: {0}")]
    Other(String),
}

/// Internal paths used by macro-generated code. Not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use inventory;
    pub use oxide_core::Context;
    pub use serde::Deserialize;
    pub use serde_json::Deserializer;
    pub use std::boxed::Box;
}
