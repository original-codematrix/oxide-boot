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
        resolve_base_url,
    };
    pub use inventory;
    pub use oxide_boot::__private::{AppConfig, ComponentDescriptor, ContextError};
    pub use oxide_boot::ConfigProperties;
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
pub async fn send(
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
