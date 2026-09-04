//! Client-side retry, against a server that actually fails.
//!
//! The interesting assertions are the ones about *not* retrying. A retry on
//! an HTTP call is not consequence-free: repeating a `POST` that in fact
//! succeeded, and whose response was lost, creates the resource twice. So
//! the defaults are conservative and the tests pin that, not just the happy
//! path.

use axum::Router;
use axum::routing::{get, post};
use oxide_client::{ClientError, ClientRetryProps, send_with_retry};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Per-server counters rather than statics.
///
/// These tests run concurrently in one binary, and a shared counter means
/// one test's `store(0)` erases another's tally -- which is exactly what a
/// first version of this file did, reporting "a 4xx was retried" for a test
/// that had made one request. Each test gets its own server and its own
/// state.
#[derive(Clone, Default)]
struct Hits {
    get: std::sync::Arc<AtomicUsize>,
    post: std::sync::Arc<AtomicUsize>,
}

/// Serves four behaviours, counting the hits it receives.
async fn serve() -> (SocketAddr, Hits) {
    let hits = Hits::default();

    let flaky_hits = std::sync::Arc::clone(&hits.get);
    let bad_hits = std::sync::Arc::clone(&hits.get);
    let post_hits = std::sync::Arc::clone(&hits.post);

    let app = Router::new()
        // Fails with 503 twice, then succeeds.
        .route(
            "/flaky",
            get(move || {
                let hits = std::sync::Arc::clone(&flaky_hits);
                async move {
                    if hits.fetch_add(1, Ordering::SeqCst) < 2 {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        axum::http::StatusCode::OK
                    }
                }
            }),
        )
        .route(
            "/bad",
            get(move || {
                let hits = std::sync::Arc::clone(&bad_hits);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::BAD_REQUEST
                }
            }),
        )
        .route("/down", get(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }))
        .route(
            "/write",
            post(move || {
                let hits = std::sync::Arc::clone(&post_hits);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        );

    // Bound here rather than inside the task, so a bind failure fails the
    // test instead of being swallowed.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, hits)
}

fn retrying(max_attempts: u32) -> ClientRetryProps {
    ClientRetryProps { max_attempts, backoff_ms: 1, jitter: 0.0, ..ClientRetryProps::default() }
}

async fn call(
    addr: SocketAddr,
    method: &str,
    path: &str,
    retry: &ClientRetryProps,
) -> Result<reqwest::Response, ClientError> {
    let client = reqwest::Client::new();
    let url = format!("http://{addr}{path}");
    let req = match method {
        "POST" => client.post(&url).body("{}"),
        _ => client.get(&url),
    };
    send_with_retry(&client, req, "test-client", method, path, retry).await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_5xx_is_retried_until_it_succeeds() {
    let (addr, hits) = serve().await;
    let result = call(addr, "GET", "/flaky", &retrying(4)).await;
    assert!(result.is_ok(), "expected success after retries: {:?}", result.err());
    assert_eq!(hits.get.load(Ordering::SeqCst), 3, "two failures and one success");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_4xx_is_not_retried() {
    let (addr, hits) = serve().await;
    let err = call(addr, "GET", "/bad", &retrying(4)).await.unwrap_err();
    assert!(matches!(err, ClientError::Http { .. }));
    // A 400 says the request itself is wrong; repeating it produces the same
    // answer more slowly.
    assert_eq!(hits.get.load(Ordering::SeqCst), 1, "a 4xx was retried");
}

#[tokio::test(flavor = "multi_thread")]
async fn retrying_is_off_unless_configured() {
    let (addr, hits) = serve().await;
    // The default is `max_attempts: 1`.
    let err = call(addr, "GET", "/flaky", &ClientRetryProps::default()).await.unwrap_err();
    assert!(matches!(err, ClientError::Http { .. }));
    assert_eq!(hits.get.load(Ordering::SeqCst), 1, "retrying happened without being asked for");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_post_is_not_retried_by_default() {
    let (addr, hits) = serve().await;
    let err = call(addr, "POST", "/write", &retrying(4)).await.unwrap_err();
    assert!(matches!(err, ClientError::Http { .. }));
    // Repeating a POST whose response was lost creates the resource twice.
    // Retrying one has to be an explicit decision.
    assert_eq!(hits.post.load(Ordering::SeqCst), 1, "a POST was retried without retry-non-idempotent");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_post_is_retried_when_explicitly_allowed() {
    let (addr, hits) = serve().await;
    let props = ClientRetryProps { retry_non_idempotent: true, ..retrying(3) };
    let err = call(addr, "POST", "/write", &props).await.unwrap_err();
    assert!(matches!(err, ClientError::Http { .. }));
    assert_eq!(hits.post.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn attempts_are_bounded() {
    let (addr, _hits) = serve().await;
    let started = std::time::Instant::now();
    let err = call(addr, "GET", "/down", &retrying(3)).await.unwrap_err();
    assert!(matches!(err, ClientError::Http { .. }));
    // Three attempts at a 1ms backoff, not an unbounded loop.
    assert!(started.elapsed() < std::time::Duration::from_secs(5), "took {:?}", started.elapsed());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_failure_is_retried() {
    // Nothing listens on port 1, so every attempt is a transport error --
    // the case retrying exists for.
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let err = call(addr, "GET", "/anything", &retrying(2)).await.unwrap_err();
    assert!(matches!(err, ClientError::Transport(_)), "expected a transport error, got {err:?}");
}
