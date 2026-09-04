//! A circuit breaker, for the failure retrying cannot help with.
//!
//! A retry answers a *single* transient fault. Against a service that is
//! comprehensively down or overloaded it makes things worse: every call now
//! costs several times the latency and several times the connections, and
//! the struggling upstream receives more load rather than less. A breaker
//! ends that — after enough failures it rejects immediately, without opening
//! a connection at all, and gives the upstream room to recover.
//!
//! ```text
//!               failure rate exceeded
//!    Closed ──────────────────────────────► Open
//!      ▲                                      │
//!      │ probe succeeded          open duration elapsed
//!      │                                      ▼
//!      └──────────────── HalfOpen ◄───────────┘
//!                           │
//!                           │ probe failed
//!                           └──────────────► Open
//! ```

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How a breaker decides to open.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Off unless switched on. A breaker that opens unexpectedly looks to
    /// the caller like an outage that is not happening, so this is a
    /// deliberate choice rather than a default.
    pub enabled: bool,
    /// How many recent calls the failure rate is measured over.
    ///
    /// A sliding window rather than an all-time ratio: after a day of
    /// healthy traffic, an all-time ratio cannot reach any threshold, and
    /// the breaker would never open again.
    pub window_size: usize,
    /// Calls required before the rate is evaluated at all.
    ///
    /// Without this the first failed call after boot is a 100% failure rate
    /// and opens the circuit.
    pub minimum_calls: usize,
    /// Fraction of the window that must have failed, `0.0..=1.0`.
    pub failure_rate_threshold: f64,
    /// How long to stay open before letting a probe through.
    pub open_duration: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            window_size: 20,
            minimum_calls: 10,
            failure_rate_threshold: 0.5,
            open_duration: Duration::from_secs(30),
        }
    }
}

/// What the breaker currently allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Make the call, then report the outcome.
    Allow,
    /// Fail immediately; do not touch the network.
    Reject,
}

/// The breaker's observable state, for health endpoints and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug)]
enum Phase {
    Closed,
    Open {
        since: Instant,
    },
    /// Exactly one call is let through. More would hit a service that is
    /// just recovering with a burst, which is what put it under in the first
    /// place.
    HalfOpen {
        probe_in_flight: bool,
    },
}

#[derive(Debug)]
struct State {
    phase: Phase,
    /// `true` marks a failure. Bounded by `window_size`.
    outcomes: VecDeque<bool>,
}

/// Tracks failures for one upstream and decides whether to keep calling it.
///
/// One breaker per client rather than per method: what it models is "this
/// service is down", which is a property of the service.
#[derive(Debug)]
pub struct CircuitBreaker {
    name: &'static str,
    config: CircuitBreakerConfig,
    state: Mutex<State>,
}

impl CircuitBreaker {
    pub fn new(name: &'static str, config: CircuitBreakerConfig) -> Self {
        Self { name, config, state: Mutex::new(State { phase: Phase::Closed, outcomes: VecDeque::new() }) }
    }

    /// Whether a call may proceed. Every `Allow` must be followed by
    /// [`record_success`](Self::record_success) or
    /// [`record_failure`](Self::record_failure), or a half-open probe stays
    /// in flight forever and the breaker never closes.
    pub fn check(&self) -> Decision {
        if !self.config.enabled {
            return Decision::Allow;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match &mut state.phase {
            Phase::Closed => Decision::Allow,
            Phase::Open { since } => {
                if since.elapsed() < self.config.open_duration {
                    return Decision::Reject;
                }
                tracing::info!(circuit = self.name, "open duration elapsed; letting one probe through");
                state.phase = Phase::HalfOpen { probe_in_flight: true };
                Decision::Allow
            }
            Phase::HalfOpen { probe_in_flight } => {
                if *probe_in_flight {
                    Decision::Reject
                } else {
                    *probe_in_flight = true;
                    Decision::Allow
                }
            }
        }
    }

    pub fn record_success(&self) {
        if !self.config.enabled {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if matches!(state.phase, Phase::HalfOpen { .. }) {
            tracing::info!(circuit = self.name, "probe succeeded; closing");
            state.phase = Phase::Closed;
            // Cleared, not carried over: the window that opened the circuit
            // describes an outage that has ended, and keeping it would
            // reopen on the next single failure.
            state.outcomes.clear();
            return;
        }
        self.push(&mut state, false);
    }

    pub fn record_failure(&self) {
        if !self.config.enabled {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if matches!(state.phase, Phase::HalfOpen { .. }) {
            tracing::warn!(circuit = self.name, "probe failed; reopening");
            state.phase = Phase::Open { since: Instant::now() };
            return;
        }
        self.push(&mut state, true);
        self.trip_if_over_threshold(&mut state);
    }

    fn push(&self, state: &mut State, failed: bool) {
        state.outcomes.push_back(failed);
        while state.outcomes.len() > self.config.window_size.max(1) {
            state.outcomes.pop_front();
        }
    }

    fn trip_if_over_threshold(&self, state: &mut State) {
        if state.outcomes.len() < self.config.minimum_calls {
            return;
        }
        let failures = state.outcomes.iter().filter(|failed| **failed).count();
        let rate = failures as f64 / state.outcomes.len() as f64;
        if rate >= self.config.failure_rate_threshold {
            tracing::warn!(
                circuit = self.name,
                failure_rate = rate,
                calls = state.outcomes.len(),
                "failure rate exceeded; opening"
            );
            state.phase = Phase::Open { since: Instant::now() };
            state.outcomes.clear();
        }
    }

    /// The current state, for a health endpoint or a test.
    pub fn state(&self) -> CircuitState {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match state.phase {
            Phase::Closed => CircuitState::Closed,
            Phase::Open { .. } => CircuitState::Open,
            Phase::HalfOpen { .. } => CircuitState::HalfOpen,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn breaker(config: CircuitBreakerConfig) -> CircuitBreaker {
        CircuitBreaker::new("test", config)
    }

    fn enabled() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            enabled: true,
            window_size: 10,
            minimum_calls: 4,
            failure_rate_threshold: 0.5,
            open_duration: Duration::from_millis(50),
        }
    }

    #[test]
    fn a_disabled_breaker_allows_everything() {
        let b = breaker(CircuitBreakerConfig::default());
        for _ in 0..100 {
            assert_eq!(b.check(), Decision::Allow);
            b.record_failure();
        }
        assert_eq!(b.state(), CircuitState::Closed);
    }

    #[test]
    fn the_minimum_call_count_holds_it_closed() {
        let b = breaker(enabled());
        // Three failures out of three is a 100% failure rate, and must not
        // open anything: the first failed call after boot is not evidence.
        for _ in 0..3 {
            b.record_failure();
        }
        assert_eq!(b.state(), CircuitState::Closed);
        assert_eq!(b.check(), Decision::Allow);
    }

    #[test]
    fn crossing_the_threshold_opens_the_circuit() {
        let b = breaker(enabled());
        for _ in 0..4 {
            b.record_failure();
        }
        assert_eq!(b.state(), CircuitState::Open);
        assert_eq!(b.check(), Decision::Reject, "an open circuit must not touch the network");
    }

    #[test]
    fn a_healthy_mix_stays_closed() {
        let b = breaker(enabled());
        // 20% failures, spread out, against a 50% threshold.
        for i in 0..10 {
            if i % 5 == 2 { b.record_failure() } else { b.record_success() }
        }
        assert_eq!(b.state(), CircuitState::Closed);
    }

    #[test]
    fn an_early_burst_can_open_even_when_the_long_run_rate_is_lower() {
        // The rate is evaluated over the *current* window, so failures
        // clustered at the start cross the threshold before the later
        // successes can dilute them. Pinned because it looks like a bug from
        // the outside -- "40% failures opened a 50% breaker" -- and is not:
        // at the moment of the fourth call the window really was at 50%.
        let b = breaker(enabled());
        b.record_failure();
        b.record_failure();
        b.record_success();
        b.record_success();
        assert_eq!(b.state(), CircuitState::Closed, "still under the minimum-plus-threshold");
        b.record_failure();
        assert_eq!(b.state(), CircuitState::Open, "3 of 5 in the window is over 50%");
    }

    #[test]
    fn it_stays_open_until_the_duration_elapses() {
        let b = breaker(enabled());
        for _ in 0..4 {
            b.record_failure();
        }
        assert_eq!(b.check(), Decision::Reject);
        std::thread::sleep(Duration::from_millis(80));
        // Now, and only now, one call gets through.
        assert_eq!(b.check(), Decision::Allow);
        assert_eq!(b.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn half_open_lets_exactly_one_probe_through() {
        let b = breaker(enabled());
        for _ in 0..4 {
            b.record_failure();
        }
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(b.check(), Decision::Allow, "the first probe");
        // A recovering service must not be met with a burst.
        for _ in 0..10 {
            assert_eq!(b.check(), Decision::Reject, "a second probe was let through");
        }
    }

    #[test]
    fn a_successful_probe_closes_the_circuit() {
        let b = breaker(enabled());
        for _ in 0..4 {
            b.record_failure();
        }
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(b.check(), Decision::Allow);
        b.record_success();

        assert_eq!(b.state(), CircuitState::Closed);
        // And the window that opened it is gone: carrying it over would
        // reopen on the very next failure.
        b.record_failure();
        assert_eq!(b.state(), CircuitState::Closed);
    }

    #[test]
    fn a_failed_probe_reopens_and_restarts_the_clock() {
        let b = breaker(enabled());
        for _ in 0..4 {
            b.record_failure();
        }
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(b.check(), Decision::Allow);
        b.record_failure();

        assert_eq!(b.state(), CircuitState::Open);
        assert_eq!(b.check(), Decision::Reject, "the open duration did not restart");
    }

    #[test]
    fn the_window_slides() {
        let config = CircuitBreakerConfig { window_size: 4, minimum_calls: 4, ..enabled() };
        let b = breaker(config);
        // Two failures, then four successes: the failures fall out of the
        // window and the rate goes to zero.
        b.record_failure();
        b.record_failure();
        for _ in 0..4 {
            b.record_success();
        }
        assert_eq!(b.state(), CircuitState::Closed);
        // One more failure is 1/4, under the 50% threshold.
        b.record_failure();
        assert_eq!(b.state(), CircuitState::Closed);
    }
}
