//! Retry + dead-letter-topic machinery for listener handlers.
//!
//! On a handler error the partition is **blocked** (retries are sequential per
//! record — ordering before throughput, mirroring Spring's blocking retry).
//! After the attempts are exhausted the record is either published to the
//! dead-letter topic (`<topic>.DLT`, if `dlt-enabled`) or logged and committed.

use crate::ReceivedMessage;
use crate::config::RetryProps;
use crate::producer::KafkaProducer;
use rdkafka::message::{Header, OwnedHeaders};
use std::time::Duration;

/// Total attempts are `max_attempts` (including the first). Returns the backoff
/// sleeps for retries 2..=`max_attempts` (i.e. `max_attempts - 1` entries):
/// each is `backoff_initial_ms * backoff_multiplier^(attempt-1)`.
pub fn backoff_sequence(props: &RetryProps) -> impl Iterator<Item = Duration> + '_ {
    (1..props.max_attempts).map(move |attempt| {
        let factor = props.backoff_multiplier.powf((attempt as f64) - 1.0);
        Duration::from_millis((props.backoff_initial_ms as f64 * factor).round() as u64)
    })
}

/// DLT header names (match the Spec §9 contract exactly).
pub const HEADER_EXCEPTION_MESSAGE: &str = "x-exception-message";
pub const HEADER_ORIGINAL_TOPIC: &str = "x-original-topic";
pub const HEADER_ORIGINAL_PARTITION: &str = "x-original-partition";
pub const HEADER_ORIGINAL_OFFSET: &str = "x-original-offset";

/// Publishes `msg` (with its original payload/key) to `<topic>.DLT`, attaching
/// the retry/DLT diagnostic headers. Returns `Err` if the produce itself fails
/// (the caller must then NOT commit, so the record is redelivered).
pub async fn publish_to_dlt(
    producer: &KafkaProducer,
    msg: &ReceivedMessage,
    last_error: &str,
) -> Result<(), crate::KafkaError> {
    let dlt_topic = format!("{}.DLT", msg.topic);
    let mut headers = OwnedHeaders::new()
        .insert(Header { key: HEADER_EXCEPTION_MESSAGE, value: Some(last_error.as_bytes()) })
        .insert(Header { key: HEADER_ORIGINAL_TOPIC, value: Some(msg.topic.as_bytes()) })
        .insert(Header { key: HEADER_ORIGINAL_PARTITION, value: Some(msg.partition.to_string().as_bytes()) })
        .insert(Header { key: HEADER_ORIGINAL_OFFSET, value: Some(msg.offset.to_string().as_bytes()) });
    // Preserve any other headers the original record carried (e.g. traceparent).
    for (k, v) in &msg.headers {
        headers = headers.insert(Header { key: k.as_str(), value: Some(v.as_slice()) });
    }
    producer.send_raw(&dlt_topic, msg.key.as_deref(), &msg.payload, Some(headers)).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_sequence_sizes_and_values() {
        let props =
            RetryProps { max_attempts: 3, backoff_initial_ms: 100, backoff_multiplier: 2.0, ..Default::default() };
        let backoffs: Vec<Duration> = backoff_sequence(&props).collect();
        // 2 entries (attempts 2 and 3), 100ms and 200ms.
        assert_eq!(backoffs.len(), 2);
        assert_eq!(backoffs[0], Duration::from_millis(100));
        assert_eq!(backoffs[1], Duration::from_millis(200));
    }

    #[test]
    fn single_attempt_yields_no_backoffs() {
        let props = RetryProps { max_attempts: 1, ..Default::default() };
        assert_eq!(backoff_sequence(&props).count(), 0);
    }
}
