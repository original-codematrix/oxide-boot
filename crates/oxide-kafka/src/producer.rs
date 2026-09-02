//! `KafkaProducer` component: an injectable, idempotent wrapper around
//! rdkafka's `FutureProducer`. Each send records a `kafka_producer_send_seconds`
//! histogram and a `kafka.send` span, and injects the W3C `traceparent` into
//! the record headers so the trace flows end-to-end through Kafka.

use crate::observe;
use crate::{KafkaError, KafkaProps};
use oxide_core::{ComponentDescriptor, Context};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde::Serialize;
use std::time::{Duration, Instant};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Result of a successfully delivered record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    pub partition: i32,
    pub offset: i64,
}

/// Injectable producer component. Constructed automatically from `KafkaProps`
/// and flushed on shutdown before the OpenTelemetry flush.
pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    fn new(props: &KafkaProps) -> Self {
        let config = props.client_config(&[
            ("enable.idempotence", props.producer.idempotence.to_string()),
            // Idempotent producers internally retry on transient errors; cap
            // the retry count at librdkafka's maximum.
            ("message.send.max.retries", "2147483647".to_string()),
        ]);
        let producer: FutureProducer = config.create().expect("build kafka producer");
        Self { producer }
    }

    /// Serializes `value` as JSON and sends it with a string key.
    pub async fn send<T: Serialize>(&self, topic: &str, key: &str, value: &T) -> Result<Delivery, KafkaError> {
        let payload = serde_json::to_vec(value)
            .map_err(|e| KafkaError::Serialize { topic: topic.to_string(), source: e })?;
        self.send_raw(topic, Some(key.as_bytes()), &payload, None).await
    }

    /// Sends a raw payload with an optional key and headers, injecting the W3C
    /// `traceparent` header. `headers` are preserved (a `traceparent` already
    /// present is overwritten).
    pub async fn send_raw(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        payload: &[u8],
        headers: Option<OwnedHeaders>,
    ) -> Result<Delivery, KafkaError> {
        // Normalize incoming headers into a mutable (name, value) list.
        let mut header_list: Vec<(String, Vec<u8>)> = headers
            .map(|h| {
                h.iter()
                    .map(|h| (h.key.to_string(), h.value.map(|v| v.to_vec()).unwrap_or_default()))
                    .collect()
            })
            .unwrap_or_default();

        // The `kafka.send` span (created inside the caller's active scope, so a child
        // of the caller's trace) injects its own context's traceparent for the
        // downstream consumer — producing the end-to-end W3C chain.
        let span = tracing::info_span!("kafka.send", topic = %topic);
        let cx = span.context();
        observe::inject_traceparent(&cx, &mut header_list);

        let mut owned = OwnedHeaders::new();
        for (k, v) in &header_list {
            owned = owned.insert(Header { key: k.as_str(), value: Some(v.as_slice()) });
        }

        let mut record = FutureRecord::to(topic).payload(payload);
        if let Some(k) = key {
            record = record.key(k);
        }
        let record = record.headers(owned);

        let started = Instant::now();
        let result = self
            .producer
            .send(record, Timeout::After(Duration::from_secs(10)))
            .instrument(span)
            .await;

        let outcome = match result {
            Ok((partition, offset)) => Ok(Delivery { partition, offset }),
            Err((e, _msg)) => Err(KafkaError::Send { topic: topic.to_string(), source: e }),
        };

        let status = if outcome.is_ok() { "success" } else { "error" };
        metrics::histogram!(
            "kafka_producer_send_seconds",
            "topic" => topic.to_string(),
            "result" => status.to_string(),
        )
        .record(started.elapsed().as_secs_f64());

        outcome
    }

    async fn flush(&self) -> KafkaResult<()> {
        self.producer.flush(Timeout::After(Duration::from_secs(10)))
    }
}

inventory::submit! {
    ComponentDescriptor::builder(
        "KafkaProducer",
        std::any::TypeId::of::<KafkaProducer>,
        |b| {
            let props = b.resolve::<KafkaProps>()?;
            let me = KafkaProducer::new(&props);
            b.insert(me);
            Ok(())
        },
    )
    .deps(|| vec![(std::any::TypeId::of::<KafkaProps>(), "KafkaProps")])
    .pre_destroy(|ctx: &Context| {
        let me = ctx.get::<KafkaProducer>().expect("KafkaProducer exists during shutdown");
        // Flush remaining messages out before the process tears down. Runs at
        // destroy_order 0, so it completes before the OpenTelemetry flush (100).
        Box::pin(async move {
            match me.flush().await {
                Ok(()) => Ok(()),
                Err(source) => {
                    let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(KafkaError::Flush { source });
                    Err(boxed)
                }
            }
        })
    })
    .build()
}