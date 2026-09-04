//! Oxide Boot HTTP client starter: declarative `#[http_client]` trait clients
//! on reqwest with automatic W3C trace propagation, client metrics, and
//! typed `ClientError` (with RFC 7807 problem+json detection).

pub mod error;

pub use error::{ClientError, ProblemDetail};

/// A path/query string argument for an `#[http_client]` method (unnamed).
#[derive(Debug, Clone)]
pub struct Path<T>(pub T);

/// A JSON request body argument for an `#[http_client]` method (serialized).
#[derive(Debug, Clone)]
pub struct Json<T>(pub T);

pub use oxide_client_macros::http_client;

/// Default `oxide.clients.<name>.connect-timeout-seconds`.
pub const DEFAULT_CONNECT_TIMEOUT_SECONDS: u64 = 5;
/// Default `oxide.clients.<name>.request-timeout-seconds`.
pub const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;

#[doc(hidden)]
pub fn default_connect_timeout_seconds() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_SECONDS
}

#[doc(hidden)]
pub fn default_request_timeout_seconds() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_SECONDS
}

/// The set of characters percent-encoded in a single `Path<T>` value before
/// it is interpolated into a route template. Only RFC 3986 "unreserved"
/// characters (letters, digits, `-`, `.`, `_`, `~`) pass through literally;
/// everything else — including `/`, `?`, `=`, `&`, `#`, `%` and whitespace —
/// is percent-encoded. This is deliberately strict: a `Path<T>` value must
/// never be able to introduce a route separator or a query string, so no
/// reserved/delimiter character is ever allowed through unescaped.
const PATH_SEGMENT: &percent_encoding::AsciiSet =
    &percent_encoding::NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');

/// Percent-encodes a single path segment value (a `Path<T>` argument,
/// formatted via `Display`) so it cannot change the route or smuggle a query
/// string into the request URL.
pub fn encode_path_segment(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, PATH_SEGMENT).to_string()
}

/// Internal paths referenced by generated macro code.
#[doc(hidden)]
pub mod __private {
    pub use crate::send;
    pub use crate::{
        ClientError, default_connect_timeout_seconds, default_request_timeout_seconds, encode_path_segment,
        resolve_base_url, send_guarded, send_with_retry,
    };
    pub use inventory;
    pub use oxide_boot::__private::{AppConfig, ComponentDescriptor, ContextError};
    pub use oxide_boot::ConfigProperties;
    pub use oxide_boot::circuit::CircuitBreaker;
    pub use serde;
}

/// Resolves a client's base URL: a literal, or a `${oxide.clients.…}` key.
/// Fails (rather than panicking) when a `${...}` key is present but missing
/// from config, so boot fails with a clear message naming the key.
pub fn resolve_base_url(value: &str, config: &oxide_core::AppConfig) -> Result<String, ClientError> {
    if let Some(key) = value.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        config
            .get_opt::<String>(key)
            .map_err(|e| ClientError::Config(format!("failed to resolve base_url config key '{key}': {e}")))?
            .ok_or_else(|| ClientError::Config(format!("missing config key '{key}' for http client base_url")))
    } else {
        Ok(value.to_string())
    }
}

/// Builds, sends, observes and parses a request. Used by `#[http_client]`.
///
/// `route_template` is the route as written on the method (e.g.
/// `/invoices/{id}`), known at macro-expansion time — used as the `uri`
/// metric label instead of the concrete request path so cardinality stays
/// bounded regardless of how many distinct path-parameter values are called.
/// Retry settings for one client, bound to `oxide.clients.<name>.retry`.
///
/// **Off by default.** A retry on an HTTP call is not free of consequence:
/// retrying a `POST` that in fact succeeded, and whose response was lost,
/// creates the resource twice. Spring Cloud OpenFeign takes the same
/// position (`Retryer.NEVER_RETRY`), and so does this.
///
/// When it is switched on, only *idempotent* methods are retried unless
/// `retry-non-idempotent` says otherwise. `GET`, `HEAD`, `PUT`, `DELETE` and
/// `OPTIONS` are safe to repeat by definition; `POST` and `PATCH` are not,
/// and making that an explicit choice is the difference between a duplicate
/// order and a caught outage.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ClientRetryProps {
    /// Total attempts, including the first. `1` -- the default -- is off.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
    #[serde(default = "default_jitter")]
    pub jitter: f64,
    /// Allows `POST` and `PATCH` to be retried. See the type docs.
    #[serde(default)]
    pub retry_non_idempotent: bool,
}

fn default_max_attempts() -> u32 {
    1
}
fn default_backoff_ms() -> u64 {
    100
}
fn default_max_backoff_ms() -> u64 {
    5_000
}
fn default_multiplier() -> f64 {
    2.0
}
fn default_jitter() -> f64 {
    0.25
}

impl Default for ClientRetryProps {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            backoff_ms: default_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            multiplier: default_multiplier(),
            jitter: default_jitter(),
            retry_non_idempotent: false,
        }
    }
}

impl ClientRetryProps {
    fn policy(&self) -> oxide_boot::retry::RetryPolicy {
        oxide_boot::retry::RetryPolicy {
            max_attempts: self.max_attempts,
            initial_backoff: std::time::Duration::from_millis(self.backoff_ms),
            multiplier: self.multiplier,
            max_backoff: std::time::Duration::from_millis(self.max_backoff_ms),
            jitter: self.jitter,
        }
    }
}

/// A circuit breaker for one client, off unless configured.
///
/// ```yaml
/// oxide:
///   clients:
///     notes:
///       circuit-breaker:
///         enabled: true
///         window-size: 20
///         minimum-calls: 10
///         failure-rate-threshold: 0.5
///         open-duration-seconds: 30
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ClientCircuitBreakerProps {
    /// Off by default, for the same reason retrying is: a breaker that opens
    /// unexpectedly looks like an outage that is not happening.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_window_size")]
    pub window_size: usize,
    #[serde(default = "default_minimum_calls")]
    pub minimum_calls: usize,
    #[serde(default = "default_failure_rate_threshold")]
    pub failure_rate_threshold: f64,
    #[serde(default = "default_open_duration_seconds")]
    pub open_duration_seconds: u64,
}

fn default_window_size() -> usize {
    20
}
fn default_minimum_calls() -> usize {
    10
}
fn default_failure_rate_threshold() -> f64 {
    0.5
}
fn default_open_duration_seconds() -> u64 {
    30
}

impl Default for ClientCircuitBreakerProps {
    fn default() -> Self {
        Self {
            enabled: false,
            window_size: default_window_size(),
            minimum_calls: default_minimum_calls(),
            failure_rate_threshold: default_failure_rate_threshold(),
            open_duration_seconds: default_open_duration_seconds(),
        }
    }
}

impl ClientCircuitBreakerProps {
    /// Builds the breaker for a client, or `None` when it is switched off --
    /// so the request path has no breaker to consult at all rather than a
    /// disabled one to check on every call.
    pub fn breaker(&self, client_name: &'static str) -> Option<oxide_boot::circuit::CircuitBreaker> {
        if !self.enabled {
            return None;
        }
        Some(oxide_boot::circuit::CircuitBreaker::new(
            client_name,
            oxide_boot::circuit::CircuitBreakerConfig {
                enabled: true,
                window_size: self.window_size,
                minimum_calls: self.minimum_calls,
                failure_rate_threshold: self.failure_rate_threshold,
                open_duration: std::time::Duration::from_secs(self.open_duration_seconds),
            },
        ))
    }
}

/// Whether a failure means *the upstream is unhealthy*, which is a narrower
/// question than whether it is worth retrying.
///
/// A 4xx does not count. The service answered, correctly, that the request
/// was wrong -- a client that fetches many missing resources would otherwise
/// open the circuit against a perfectly healthy dependency, and then the
/// calls that *would* have succeeded fail too. What counts is a transport
/// failure, a 5xx, or a 429: the upstream is down, broken, or shedding load,
/// and backing off is the useful response to all three.
///
/// This deliberately differs from [`is_retryable`], which excludes 501 and
/// 505: those are not worth *repeating*, but they are still the upstream
/// failing to serve us.
fn counts_against_the_circuit(error: &ClientError) -> bool {
    match error {
        ClientError::Http { status, .. } => status.is_server_error() || status.as_u16() == 429,
        ClientError::Transport(_) => true,
        // Build, Json and Config never reached the network, and CircuitOpen
        // is the breaker's own answer -- counting it would keep the circuit
        // open forever by feeding it its own output.
        _ => false,
    }
}

/// Whether repeating this method is safe without the server's cooperation.
fn is_idempotent(method: &reqwest::Method) -> bool {
    matches!(
        *method,
        reqwest::Method::GET
            | reqwest::Method::HEAD
            | reqwest::Method::PUT
            | reqwest::Method::DELETE
            | reqwest::Method::OPTIONS
            | reqwest::Method::TRACE
    )
}

/// Whether a failure is worth repeating.
///
/// A transport error (connection refused, timeout, reset) is transient by
/// nature. Of the status codes, only 5xx and 429 are: a 4xx says the request
/// itself is wrong, and sending it again produces the same answer more
/// slowly. 501 and 505 are excluded for the same reason -- the server is
/// telling us it will never support this.
fn is_retryable(error: &ClientError) -> bool {
    match error {
        ClientError::Http { status, .. } => {
            status.as_u16() == 429
                || (status.is_server_error()
                    && *status != reqwest::StatusCode::NOT_IMPLEMENTED
                    && *status != reqwest::StatusCode::HTTP_VERSION_NOT_SUPPORTED)
        }
        // Connection refused, reset, timeout: transient by nature.
        ClientError::Transport(_) => true,
        // Build, JSON, config and the rest are deterministic -- the same
        // input produces the same failure, so repeating it only adds
        // latency.
        _ => false,
    }
}

pub async fn send(
    client: &reqwest::Client,
    req: reqwest::RequestBuilder,
    client_name: &str,
    method: &str,
    route_template: &str,
) -> Result<reqwest::Response, ClientError> {
    send_with_retry(client, req, client_name, method, route_template, &ClientRetryProps::default()).await
}

/// [`send`] with a retry policy.
///
/// The request is cloned per attempt. A body that cannot be cloned -- a
/// stream -- makes retrying impossible, and that is reported rather than
/// silently degrading to a single attempt.
pub async fn send_with_retry(
    client: &reqwest::Client,
    req: reqwest::RequestBuilder,
    client_name: &str,
    method: &str,
    route_template: &str,
    retry: &ClientRetryProps,
) -> Result<reqwest::Response, ClientError> {
    let policy = retry.policy();
    let retrying = policy.max_attempts > 1;

    let mut attempt: u32 = 1;
    loop {
        // Cloned before sending, because `execute` consumes the request.
        // `try_clone` is `None` only for a streaming body.
        let attempt_req = if retrying {
            match req.try_clone() {
                Some(cloned) => cloned,
                None if attempt == 1 => {
                    // First attempt only: send it, but do not pretend a
                    // retry is available.
                    return send_once(client, req, client_name, method, route_template).await;
                }
                None => unreachable!("a non-cloneable body returns on the first attempt above"),
            }
        } else {
            return send_once(client, req, client_name, method, route_template).await;
        };

        let outcome = send_once(client, attempt_req, client_name, method, route_template).await;
        let error = match outcome {
            Ok(response) => return Ok(response),
            Err(e) => e,
        };

        let method_allows = retry.retry_non_idempotent
            || reqwest::Method::from_bytes(method.as_bytes()).map(|m| is_idempotent(&m)).unwrap_or(false);

        if attempt >= policy.max_attempts || !is_retryable(&error) || !method_allows {
            return Err(error);
        }

        attempt += 1;
        let delay = policy.delay_for(attempt);
        oxide_boot::retry::record_retry(client_name_static(client_name), attempt, delay, &error);
        metrics::counter!("http_client_retries_total", "client" => client_name.to_string(), "uri" => route_template.to_string())
            .increment(1);
        tokio::time::sleep(delay).await;
    }
}

/// [`send_with_retry`] behind a circuit breaker.
///
/// The order is deliberate: **breaker outside, retry inside**. With an open
/// circuit nothing is attempted at all -- a retry loop behind an open breaker
/// would be pure waiting. And because the outcome is recorded once, after the
/// loop, an exhausted retry counts as *one* failure rather than three;
/// otherwise a breaker configured for ten failures would open after four.
pub async fn send_guarded(
    client: &reqwest::Client,
    req: reqwest::RequestBuilder,
    client_name: &str,
    method: &str,
    route_template: &str,
    retry: &ClientRetryProps,
    breaker: Option<&oxide_boot::circuit::CircuitBreaker>,
) -> Result<reqwest::Response, ClientError> {
    let Some(breaker) = breaker else {
        return send_with_retry(client, req, client_name, method, route_template, retry).await;
    };

    if breaker.check() == oxide_boot::circuit::Decision::Reject {
        metrics::counter!("http_client_circuit_rejected_total", "client" => client_name.to_string()).increment(1);
        return Err(ClientError::CircuitOpen { client: client_name.to_string() });
    }

    let outcome = send_with_retry(client, req, client_name, method, route_template, retry).await;
    match &outcome {
        Ok(_) => breaker.record_success(),
        Err(e) if counts_against_the_circuit(e) => breaker.record_failure(),
        // A 4xx still has to be reported, or a half-open probe that returns
        // one leaves the probe in flight and the circuit never closes again.
        Err(_) => breaker.record_success(),
    }
    outcome
}

/// `record_retry` takes a `&'static str` for the same reason metric labels
/// do; client names come from the macro and are static in practice, but the
/// signature here carries a `&str`, so this narrows it without allocating on
/// the happy path.
fn client_name_static(name: &str) -> &'static str {
    // Leaking is bounded by the number of distinct clients in the binary,
    // which is fixed at compile time.
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    static NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let names = NAMES.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = names.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(found) = guard.get(name) {
        return found;
    }
    let leaked: &'static str = Box::leak(name.to_owned().into_boxed_str());
    guard.insert(leaked);
    leaked
}

async fn send_once(
    client: &reqwest::Client,
    req: reqwest::RequestBuilder,
    client_name: &str,
    method: &str,
    route_template: &str,
) -> Result<reqwest::Response, ClientError> {
    let mut request = req.build().map_err(|e| ClientError::Build(e.to_string()))?;

    // W3C traceparent propagation from the current span context.
    let cx = opentelemetry::Context::current();
    let mut carrier = HeaderCarrier(request.headers_mut());
    opentelemetry::global::get_text_map_propagator(|p| p.inject_context(&cx, &mut carrier));

    let start = std::time::Instant::now();
    let result = client.execute(request).await;

    match result {
        Ok(resp) => {
            let status = resp.status().as_u16().to_string();
            metrics::histogram!("http_client_requests_seconds", "client" => client_name.to_string(), "method" => method.to_string(), "status" => status.clone(), "uri" => route_template.to_string())
                .record(start.elapsed().as_secs_f64());
            let status_code = resp.status();
            if status_code.is_success() {
                Ok(resp)
            } else {
                let text = resp.text().await.unwrap_or_default();
                Err(ClientError::Http { status: status_code, body: text })
            }
        }
        Err(e) => {
            metrics::histogram!("http_client_requests_seconds", "client" => client_name.to_string(), "method" => method.to_string(), "status" => "error".to_string(), "uri" => route_template.to_string())
                .record(start.elapsed().as_secs_f64());
            Err(ClientError::Transport(e))
        }
    }
}

/// Injects/extracts W3C headers over a reqwest HeaderMap.
struct HeaderCarrier<'a>(&'a mut reqwest::header::HeaderMap);

impl opentelemetry::propagation::Injector for HeaderCarrier<'_> {
    fn set(&mut self, key: &str, value: String) {
        // `opentelemetry::propagation::Injector::set` has no error channel
        // (it returns `()`); mirror the existing key-parse handling below
        // and simply skip a value the propagator produced that somehow
        // isn't a valid header value, rather than panicking a request.
        if let Ok(k) = reqwest::header::HeaderName::from_bytes(key.as_bytes())
            && let Ok(v) = reqwest::header::HeaderValue::from_str(&value)
        {
            self.0.insert(k, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_segments_are_percent_encoded() {
        assert_eq!(encode_path_segment("a/../b"), "a%2F..%2Fb");
        assert_eq!(encode_path_segment("x?y=1"), "x%3Fy%3D1");
    }

    #[test]
    fn missing_base_url_key_is_an_error_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let config = oxide_core::AppConfig::load(dir.path(), &std::collections::HashMap::new()).unwrap();
        assert!(resolve_base_url("${oxide.clients.nope.url}", &config).is_err());
    }

    #[test]
    fn literal_base_url_resolves_without_config_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let config = oxide_core::AppConfig::load(dir.path(), &std::collections::HashMap::new()).unwrap();
        assert_eq!(resolve_base_url("http://localhost:8080", &config).unwrap(), "http://localhost:8080");
    }
}
