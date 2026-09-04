//! `#[retryable]` — retrying a failed call.
//!
//! The behaviour worth pinning is where retrying is *wrong*: a permanent
//! error must not be retried, a non-retryable one must fail immediately, and
//! the attempt count must be exact. A retry that quietly does the same wrong
//! thing four times is worse than one that fails once.

use oxide_boot::retryable;
use std::sync::Mutex;
use std::time::Instant;

/// Per-key call counter, so tests that run concurrently in this binary do
/// not read each other's totals.
static CALLS: Mutex<Option<std::collections::HashMap<&'static str, u32>>> = Mutex::new(None);

fn record(key: &'static str) -> u32 {
    let mut guard = CALLS.lock().unwrap_or_else(|p| p.into_inner());
    let map = guard.get_or_insert_with(std::collections::HashMap::new);
    let entry = map.entry(key).or_insert(0);
    *entry += 1;
    *entry
}

fn calls(key: &'static str) -> u32 {
    CALLS.lock().unwrap_or_else(|p| p.into_inner()).as_ref().and_then(|m| m.get(key)).copied().unwrap_or(0)
}

#[derive(Debug, PartialEq)]
enum Failure {
    Transient,
    Permanent,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient => write!(f, "transient"),
            Self::Permanent => write!(f, "permanent"),
        }
    }
}

struct Service;

impl Service {
    /// Fails twice, then succeeds: the ordinary case.
    #[retryable(max_attempts = 4, backoff_ms = 1)]
    async fn flaky(&self) -> Result<&'static str, Failure> {
        if record("flaky") < 3 { Err(Failure::Transient) } else { Ok("ok") }
    }

    /// Always fails: the caller must see the last error, after exactly
    /// `max_attempts` calls.
    #[retryable(max_attempts = 3, backoff_ms = 1)]
    async fn always_fails(&self) -> Result<&'static str, Failure> {
        record("always_fails");
        Err(Failure::Transient)
    }

    /// Retries only what the predicate allows. A permanent failure retried
    /// four times is four times the latency for the same wrong answer.
    #[retryable(max_attempts = 4, backoff_ms = 1, retry_if = "matches!(error, Failure::Transient)")]
    async fn selective(&self) -> Result<&'static str, Failure> {
        record("selective");
        Err(Failure::Permanent)
    }

    /// Succeeds first time: no retry, no delay.
    #[retryable(max_attempts = 5, backoff_ms = 1000)]
    async fn immediate(&self) -> Result<&'static str, Failure> {
        record("immediate");
        Ok("first try")
    }

    /// Falls back rather than propagating, once the attempts are spent.
    #[retryable(max_attempts = 2, backoff_ms = 1, recover = "fallback")]
    async fn with_recovery(&self) -> Result<&'static str, Failure> {
        record("with_recovery");
        Err(Failure::Transient)
    }

    async fn fallback(&self, _error: Failure) -> Result<&'static str, Failure> {
        Ok("recovered")
    }

    /// Uses `?` inside the body, which must be caught by the retry loop
    /// rather than escaping it.
    #[retryable(max_attempts = 3, backoff_ms = 1)]
    async fn uses_question_mark(&self) -> Result<&'static str, Failure> {
        let inner: Result<&'static str, Failure> =
            if record("uses_question_mark") < 2 { Err(Failure::Transient) } else { Ok("ok") };
        let value = inner?;
        Ok(value)
    }

    /// Retrying is off: exactly one call.
    #[retryable(max_attempts = 1)]
    async fn no_retry(&self) -> Result<&'static str, Failure> {
        record("no_retry");
        Err(Failure::Transient)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transient_failure_is_retried_until_it_succeeds() {
    assert_eq!(Service.flaky().await, Ok("ok"));
    assert_eq!(calls("flaky"), 3, "expected two failures and one success");
}

#[tokio::test(flavor = "multi_thread")]
async fn attempts_are_bounded_and_the_last_error_propagates() {
    assert_eq!(Service.always_fails().await, Err(Failure::Transient));
    // Exactly `max_attempts`, not one more: an off-by-one here means every
    // outage costs an extra round trip per call.
    assert_eq!(calls("always_fails"), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_retryable_error_fails_on_the_first_attempt() {
    let started = Instant::now();
    assert_eq!(Service.selective().await, Err(Failure::Permanent));
    assert_eq!(calls("selective"), 1, "a permanent failure was retried");
    // And it did not sit through a backoff on the way out.
    assert!(started.elapsed() < std::time::Duration::from_millis(50));
}

#[tokio::test(flavor = "multi_thread")]
async fn success_costs_nothing() {
    let started = Instant::now();
    assert_eq!(Service.immediate().await, Ok("first try"));
    assert_eq!(calls("immediate"), 1);
    // The 1000ms backoff must never be paid when nothing failed.
    assert!(started.elapsed() < std::time::Duration::from_millis(100), "took {:?}", started.elapsed());
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_runs_when_the_attempts_are_spent() {
    assert_eq!(Service.with_recovery().await, Ok("recovered"));
    assert_eq!(calls("with_recovery"), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_question_mark_inside_the_body_is_retried_not_returned() {
    // `?` produces the async block's value, so the loop sees it as an
    // ordinary error. Getting this wrong would make `?` skip the retry
    // entirely -- silently, and only for the code paths that use it.
    assert_eq!(Service.uses_question_mark().await, Ok("ok"));
    assert_eq!(calls("uses_question_mark"), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn max_attempts_of_one_disables_retrying() {
    assert_eq!(Service.no_retry().await, Err(Failure::Transient));
    assert_eq!(calls("no_retry"), 1);
}
