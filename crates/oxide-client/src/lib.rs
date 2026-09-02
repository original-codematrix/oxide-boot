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

/// Internal paths referenced by generated macro code.
#[doc(hidden)]
pub mod __private {
    pub use crate::send;
    pub use crate::{ClientError, resolve_base_url};
    pub use inventory;
    pub use oxide_boot::__private::{AppConfig, ComponentDescriptor};
}

/// Resolves a client's base URL: a literal, or a `${oxide.clients.…}` key.
pub fn resolve_base_url(value: &str, config: &oxide_core::AppConfig) -> String {
    if let Some(key) = value.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        config
            .get_opt::<String>(key)
            .ok()
            .flatten()
            .unwrap_or_else(|| panic!("missing config key '{key}' for http client base_url"))
    } else {
        value.to_string()
    }
}

/// Builds, sends, observes and parses a request. Used by `#[http_client]`.
pub async fn send(
    client: &reqwest::Client,
    req: reqwest::RequestBuilder,
    client_name: &str,
    method: &str,
    _method_name: &str,
) -> Result<reqwest::Response, ClientError> {
    let mut request = req.build().map_err(|e| ClientError::Build(e.to_string()))?;

    // W3C traceparent propagation from the current span context.
    let cx = opentelemetry::Context::current();
    let mut carrier = HeaderCarrier(request.headers_mut());
    opentelemetry::global::get_text_map_propagator(|p| p.inject_context(&cx, &mut carrier));

    let uri = request.url().clone();
    let start = std::time::Instant::now();
    let result = client.execute(request).await;

    match result {
        Ok(resp) => {
            let status = resp.status().as_u16().to_string();
            metrics::histogram!("http_client_requests_seconds", "client" => client_name.to_string(), "method" => method.to_string(), "status" => status.clone(), "uri" => uri.path().to_string())
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
            metrics::histogram!("http_client_requests_seconds", "client" => client_name.to_string(), "method" => method.to_string(), "status" => "error".to_string(), "uri" => uri.path().to_string())
                .record(start.elapsed().as_secs_f64());
            Err(ClientError::Transport(e))
        }
    }
}

/// Injects/extracts W3C headers over a reqwest HeaderMap.
struct HeaderCarrier<'a>(&'a mut reqwest::header::HeaderMap);

impl opentelemetry::propagation::Injector for HeaderCarrier<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(k) = reqwest::header::HeaderName::from_bytes(key.as_bytes()) {
            self.0.insert(k, reqwest::header::HeaderValue::from_str(&value).expect("header value"));
        }
    }
}
