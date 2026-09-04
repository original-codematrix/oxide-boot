//! The circuit breaker on the client path.
//!
//! `oxide-boot` already tests the breaker's state machine. What is tested
//! here is the wiring the design calls out as easy to get wrong: an
//! exhausted retry must count as *one* failure, a 4xx must not count at all,
//! and an open circuit must not open a connection.

use axum::Router;
use axum::routing::get;
use oxide_boot::circuit::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
use oxide_client::{ClientError, ClientRetryProps, send_guarded};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Per-server counters. A shared static here would let one test's traffic
/// trip another test's assertions.
#[derive(Clone, Default)]
struct Hits {
    down: Arc<AtomicUsize>,
    missing: Arc<AtomicUsize>,
}

async fn serve() -> (SocketAddr, Hits) {
    let hits = Hits::default();
    let down = Arc::clone(&hits.down);
    let missing = Arc::clone(&hits.missing);

    let app = Router::new()
        .route(
            "/down",
            get(move || {
                let hits = Arc::clone(&down);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .route(
            "/missing",
            get(move || {
                let hits = Arc::clone(&missing);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::NOT_FOUND
                }
            }),
        )
        .route("/ok", get(|| async { axum::http::StatusCode::OK }));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, hits)
}

fn breaker(minimum_calls: usize) -> CircuitBreaker {
    CircuitBreaker::new(
        "test-client",
        CircuitBreakerConfig {
            enabled: true,
            window_size: 10,
            minimum_calls,
            failure_rate_threshold: 0.5,
            open_duration: Duration::from_millis(80),
        },
    )
}

fn retrying(max_attempts: u32) -> ClientRetryProps {
    ClientRetryProps { max_attempts, backoff_ms: 1, jitter: 0.0, ..ClientRetryProps::default() }
}

async fn call(
    addr: SocketAddr,
    path: &str,
    retry: &ClientRetryProps,
    breaker: Option<&CircuitBreaker>,
) -> Result<reqwest::Response, ClientError> {
    let client = reqwest::Client::new();
    let req = client.get(format!("http://{addr}{path}"));
    send_guarded(&client, req, "test-client", "GET", path, retry, breaker).await
}

#[tokio::test(flavor = "multi_thread")]
async fn an_open_circuit_rejects_without_touching_the_network() {
    let (addr, hits) = serve().await;
    let b = breaker(2);

    for _ in 0..2 {
        assert!(call(addr, "/down", &ClientRetryProps::default(), Some(&b)).await.is_err());
    }
    assert_eq!(b.state(), CircuitState::Open);
    let before = hits.down.load(Ordering::SeqCst);

    let err = call(addr, "/down", &ClientRetryProps::default(), Some(&b)).await.unwrap_err();
    assert!(matches!(err, ClientError::CircuitOpen { .. }), "got {err:?}");
    // The point of the breaker: the upstream sees nothing at all.
    assert_eq!(hits.down.load(Ordering::SeqCst), before, "an open circuit still made a request");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exhausted_retry_counts_as_one_failure_not_three() {
    let (addr, hits) = serve().await;
    // Three failures needed to open. With three attempts per call, counting
    // per attempt would open after the very first call.
    let b = breaker(3);

    assert!(call(addr, "/down", &retrying(3), Some(&b)).await.is_err());
    assert_eq!(hits.down.load(Ordering::SeqCst), 3, "the retry loop did not run");
    assert_eq!(b.state(), CircuitState::Closed, "three attempts were counted as three failures");

    // Two more calls, two more failures: now it opens.
    assert!(call(addr, "/down", &retrying(3), Some(&b)).await.is_err());
    assert!(call(addr, "/down", &retrying(3), Some(&b)).await.is_err());
    assert_eq!(b.state(), CircuitState::Open);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_4xx_does_not_open_the_circuit() {
    let (addr, hits) = serve().await;
    let b = breaker(2);

    // A client fetching many missing resources must not break itself
    // against an upstream that is answering correctly.
    for _ in 0..20 {
        let err = call(addr, "/missing", &ClientRetryProps::default(), Some(&b)).await.unwrap_err();
        assert!(matches!(err, ClientError::Http { .. }), "got {err:?}");
    }
    assert_eq!(b.state(), CircuitState::Closed, "404s opened the circuit");
    assert_eq!(hits.missing.load(Ordering::SeqCst), 20, "some calls were rejected");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_4xx_probe_does_not_wedge_a_half_open_circuit() {
    let (addr, _hits) = serve().await;
    let b = breaker(2);

    for _ in 0..2 {
        assert!(call(addr, "/down", &ClientRetryProps::default(), Some(&b)).await.is_err());
    }
    assert_eq!(b.state(), CircuitState::Open);
    tokio::time::sleep(Duration::from_millis(120)).await;

    // The probe gets a 404. That is not a failure -- but it must still be
    // reported, or the probe stays in flight and the circuit never closes.
    let err = call(addr, "/missing", &ClientRetryProps::default(), Some(&b)).await.unwrap_err();
    assert!(matches!(err, ClientError::Http { .. }));
    assert_eq!(b.state(), CircuitState::Closed, "the half-open probe was never resolved");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recovered_upstream_closes_the_circuit() {
    let (addr, _hits) = serve().await;
    let b = breaker(2);

    for _ in 0..2 {
        assert!(call(addr, "/down", &ClientRetryProps::default(), Some(&b)).await.is_err());
    }
    assert_eq!(b.state(), CircuitState::Open);

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(call(addr, "/ok", &ClientRetryProps::default(), Some(&b)).await.is_ok());
    assert_eq!(b.state(), CircuitState::Closed);
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_breaker_nothing_changes() {
    let (addr, hits) = serve().await;
    for _ in 0..10 {
        assert!(call(addr, "/down", &ClientRetryProps::default(), None).await.is_err());
    }
    // Every call reaches the upstream: the breaker is opt-in.
    assert_eq!(hits.down.load(Ordering::SeqCst), 10);
}
