//! Retrying a failed call — the `spring-retry` equivalent.
//!
//! ```ignore
//! #[retryable(max_attempts = 4, backoff_ms = 100, multiplier = 2.0)]
//! async fn fetch(&self, id: &str) -> Result<Note, ClientError> {
//!     self.client.get_note(id).await
//! }
//! ```
//!
//! # What retrying is and is not for
//!
//! A retry helps only against a *transient* failure: a dropped connection, a
//! 503, a lock timeout. Retrying a 400 or a deserialization error just does
//! the same wrong thing more slowly, and retrying a non-idempotent write can
//! duplicate it. `retry_if` exists so the decision is explicit rather than
//! "everything that returned Err".
//!
//! # Why the backoff has jitter
//!
//! When an upstream service fails, every caller fails at once — and with a
//! fixed backoff they all retry at once too, arriving as a synchronised
//! burst exactly when the upstream is least able to take it. Jitter spreads
//! them out. It is on by default at 25% because the failure mode it prevents
//! is one nobody notices until an incident.

use std::time::Duration;

/// How a `#[retryable]` method retries.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Total attempts, including the first. `1` disables retrying.
    pub max_attempts: u32,
    /// Wait before the second attempt.
    pub initial_backoff: Duration,
    /// Each subsequent wait is multiplied by this. `1.0` is a flat backoff.
    pub multiplier: f64,
    /// Ceiling for the wait, so an exponential backoff cannot grow without
    /// bound on a long-lived retry loop.
    pub max_backoff: Duration,
    /// Fraction of the computed wait to randomise, `0.0..=1.0`. See the
    /// module docs for why this defaults to a quarter.
    pub jitter: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            multiplier: 2.0,
            max_backoff: Duration::from_secs(30),
            jitter: 0.25,
        }
    }
}

impl RetryPolicy {
    /// The wait before attempt `attempt`, where the first retry is
    /// `attempt = 2`.
    ///
    /// Exponential, capped, then jittered. Returns zero for `attempt <= 1`,
    /// which has no wait before it.
    pub fn delay_for(&self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return Duration::ZERO;
        }
        let steps = (attempt - 2) as i32;
        let base = self.initial_backoff.as_secs_f64() * self.multiplier.max(1.0).powi(steps);
        let capped = base.min(self.max_backoff.as_secs_f64());
        Duration::from_secs_f64(apply_jitter(capped, self.jitter))
    }
}

/// Spreads a delay by up to `fraction` in either direction.
///
/// Seeded from the clock rather than by pulling in a random-number
/// generator: the requirement here is de-correlating callers, not
/// unpredictability, and a dependency for that would not pay for itself.
fn apply_jitter(seconds: f64, fraction: f64) -> f64 {
    let fraction = fraction.clamp(0.0, 1.0);
    if fraction == 0.0 || seconds == 0.0 {
        return seconds;
    }
    let nanos =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0);
    // Map the sub-second clock to -1.0..1.0.
    let unit = (nanos as f64 / 1_000_000_000.0) * 2.0 - 1.0;
    (seconds * (1.0 + unit * fraction)).max(0.0)
}

/// Records one retry, for the log and the metrics.
///
/// Called by the code `#[retryable]` generates; not usually called directly.
pub fn record_retry(operation: &'static str, attempt: u32, delay: Duration, error: &dyn std::fmt::Display) {
    tracing::warn!(
        operation,
        attempt,
        delay_ms = delay.as_millis() as u64,
        error = %error,
        "call failed; retrying"
    );
}

/// Records that every attempt failed.
pub fn record_exhausted(operation: &'static str, attempts: u32, error: &dyn std::fmt::Display) {
    tracing::error!(operation, attempts, error = %error, "retries exhausted");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_jitter() -> RetryPolicy {
        RetryPolicy { jitter: 0.0, ..RetryPolicy::default() }
    }

    #[test]
    fn the_first_attempt_has_no_wait() {
        assert_eq!(no_jitter().delay_for(1), Duration::ZERO);
        assert_eq!(no_jitter().delay_for(0), Duration::ZERO);
    }

    #[test]
    fn backoff_grows_by_the_multiplier() {
        let p = no_jitter();
        assert_eq!(p.delay_for(2), Duration::from_millis(100));
        assert_eq!(p.delay_for(3), Duration::from_millis(200));
        assert_eq!(p.delay_for(4), Duration::from_millis(400));
    }

    #[test]
    fn backoff_stops_at_the_ceiling() {
        let p = RetryPolicy { max_backoff: Duration::from_millis(250), ..no_jitter() };
        assert_eq!(p.delay_for(4), Duration::from_millis(250));
        // Far enough out that an uncapped exponential would overflow into
        // an implausible wait.
        assert_eq!(p.delay_for(40), Duration::from_millis(250));
    }

    #[test]
    fn a_multiplier_below_one_does_not_shrink_the_backoff() {
        // A shrinking backoff retries faster the longer an upstream stays
        // down, which is the opposite of the point; treated as flat.
        let p = RetryPolicy { multiplier: 0.5, ..no_jitter() };
        assert_eq!(p.delay_for(3), Duration::from_millis(100));
    }

    #[test]
    fn jitter_stays_within_its_fraction() {
        let p = RetryPolicy { jitter: 0.25, ..RetryPolicy::default() };
        for _ in 0..200 {
            let d = p.delay_for(2).as_secs_f64();
            assert!((0.075..=0.125).contains(&d), "delay {d} outside 100ms ±25%");
        }
    }

    #[test]
    fn jitter_never_produces_a_negative_wait() {
        let p = RetryPolicy { jitter: 1.0, ..RetryPolicy::default() };
        for _ in 0..200 {
            assert!(p.delay_for(3) >= Duration::ZERO);
        }
    }
}
