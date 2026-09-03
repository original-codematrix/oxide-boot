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
/// each is `backoff_initial_ms * backoff_multiplier^(attempt-1)`, capped at
/// `max_backoff_millis` and jittered by ±20 % (a long outage would otherwise
/// produce unboundedly long sleeps, and un-jittered backoff synchronises
/// retries across partitions into a thundering herd).
pub fn backoff_sequence(props: &RetryProps) -> impl Iterator<Item = Duration> + '_ {
    jittered_backoff_sequence(props, pseudo_random_unit)
}

/// The pure geometric sequence, capped at `max_backoff_millis`, WITHOUT
/// jitter. Exposed so tests have a deterministic baseline to compare the
/// jittered sequence against (see `jittered_backoff_sequence`).
pub fn capped_backoff_sequence(props: &RetryProps) -> impl Iterator<Item = Duration> + '_ {
    let cap = Duration::from_millis(props.max_backoff_millis);
    (1..props.max_attempts).map(move |attempt| {
        let factor = props.backoff_multiplier.powf((attempt as f64) - 1.0);
        let raw = Duration::from_millis((props.backoff_initial_ms as f64 * factor).round() as u64);
        raw.min(cap)
    })
}

/// `capped_backoff_sequence` with a ±20 % jitter applied to each entry. The
/// jitter draw is injected via `rand_unit` (expected to yield values in
/// `[0, 1)`) rather than hard-coding a random source, so a test can assert
/// the jitter bound deterministically instead of flaking on real randomness.
fn jittered_backoff_sequence<'a>(
    props: &'a RetryProps,
    mut rand_unit: impl FnMut() -> f64 + 'a,
) -> impl Iterator<Item = Duration> + 'a {
    capped_backoff_sequence(props).map(move |d| {
        let jitter_fraction = (rand_unit() * 2.0 - 1.0) * 0.2; // draw in [-0.2, 0.2]
        let factor = (1.0 + jitter_fraction).max(0.0);
        Duration::from_millis((d.as_millis() as f64 * factor).round() as u64)
    })
}

/// A cheap, non-cryptographic "random" `[0, 1)` draw, mixing the current
/// time with a call counter so consecutive calls (e.g. successive backoff
/// entries within the same retry loop) don't collide. Good enough for retry
/// jitter (whose only job is to avoid synchronised retries across
/// partitions/instances); never used where a test needs determinism — tests
/// call `jittered_backoff_sequence` directly with an injected source instead.
fn pseudo_random_unit() -> f64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    let mut x = nanos ^ counter.wrapping_mul(0x9E3779B97F4A7C15);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    (x % 1_000_000) as f64 / 1_000_000.0
}

/// Outcome of a single listener attempt: either the user's handler ran and
/// returned an error, or (before the handler ran at all) the payload failed
/// to deserialize — a **poison pill**. A poison pill will never succeed on
/// retry (the payload is malformed, not the handler transiently failing), so
/// `run_with_retry` in `listener.rs` matches on this variant to skip the
/// retry loop entirely and go straight to the DLT after exactly one attempt.
///
/// This is a distinct enum variant rather than a magic-string prefix on the
/// error message: a user handler's own error can never be misclassified as a
/// poison pill, because the two cases are different type-level branches, not
/// a string pattern the handler's message might (however implausibly)
/// happen to match.
#[derive(Debug, Clone)]
pub enum HandlerError {
    /// The user's handler returned this error.
    Handler(String),
    /// Deserializing the payload into the handler's parameter type failed,
    /// before the handler ran.
    PoisonPill(String),
}

impl HandlerError {
    /// The error message, regardless of which variant this is — used once
    /// the retry loop has decided on a terminal outcome and only needs the
    /// message for logging / the DLT `x-exception-message` header.
    pub fn into_message(self) -> String {
        match self {
            HandlerError::Handler(m) | HandlerError::PoisonPill(m) => m,
        }
    }
}

/// Wraps `message` as a poison-pill error (see [`HandlerError::PoisonPill`]).
/// Used by macro-generated listener code around JSON deserialization.
pub fn mark_poison_pill(message: impl std::fmt::Display) -> HandlerError {
    HandlerError::PoisonPill(message.to_string())
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
        // `backoff_sequence` now applies +/-20% jitter (production-facing,
        // see `backoff_is_capped_and_jittered` below), so the exact-value
        // assertions belong on the deterministic, un-jittered
        // `capped_backoff_sequence` instead.
        let backoffs: Vec<Duration> = capped_backoff_sequence(&props).collect();
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

    #[test]
    fn backoff_is_capped_and_jittered() {
        let props = RetryProps {
            max_attempts: 10,
            backoff_initial_ms: 1000,
            backoff_multiplier: 3.0,
            max_backoff_millis: 5000,
            ..Default::default()
        };
        let capped: Vec<Duration> = capped_backoff_sequence(&props).collect();
        // Uncapped this sequence would reach 1000 * 3^8 ≈ 6.5M ms; every
        // entry must instead be pinned at the 5000ms cap.
        for d in &capped {
            assert!(*d <= Duration::from_millis(5000), "cap exceeded: {d:?}");
        }
        assert!(capped.iter().any(|d| *d == Duration::from_millis(5000)), "cap should actually be hit");

        // Injected jitter source (not real randomness) walks a fixed set of
        // draws spanning the full [0, 1) range, so the assertion below is
        // deterministic instead of flaking.
        let draws = [0.0_f64, 0.25, 0.5, 0.75, 0.999];
        let mut i = 0usize;
        let jittered: Vec<Duration> = jittered_backoff_sequence(&props, move || {
            let v = draws[i % draws.len()];
            i += 1;
            v
        })
        .collect();

        assert_eq!(jittered.len(), capped.len());
        for (raw, jit) in capped.iter().zip(jittered.iter()) {
            let lower = (raw.as_millis() as f64 * 0.8).floor() as u128;
            let upper = (raw.as_millis() as f64 * 1.2).ceil() as u128;
            assert!(
                jit.as_millis() >= lower && jit.as_millis() <= upper,
                "{jit:?} not within +/-20% of un-jittered {raw:?} (bounds [{lower}, {upper}]ms)"
            );
            // The cap plus the largest allowed jitter draw is the hard
            // ceiling this test brief calls out explicitly.
            assert!(*jit <= Duration::from_millis(6000), "cap+jitter exceeded: {jit:?}");
        }
    }

    #[test]
    fn poison_pill_marker_is_a_distinct_variant() {
        let marked = mark_poison_pill("deserialize: expected value at line 1 column 1");
        assert!(
            matches!(marked, HandlerError::PoisonPill(ref m) if m == "deserialize: expected value at line 1 column 1")
        );

        let handler_err = HandlerError::Handler("plain handler error".to_string());
        assert!(!matches!(handler_err, HandlerError::PoisonPill(_)));

        // A handler error whose message happens to look like a poison-pill
        // message is still classified correctly — the discriminant is the
        // enum variant, not the string content.
        let lookalike = HandlerError::Handler("deserialize: expected value at line 1 column 1".to_string());
        assert!(!matches!(lookalike, HandlerError::PoisonPill(_)));
        assert_eq!(lookalike.into_message(), "deserialize: expected value at line 1 column 1");
    }
}
