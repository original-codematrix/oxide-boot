//! Automatic HTTP instrumentation, mounted as a [`oxide_web::RouterLayer`] so
//! it wraps every route mounted on the main `WebServer` router.
//!
//! Per request this:
//! - records a Micrometer-compatible `http_server_requests_seconds` histogram
//!   (`method`/`uri`/`status` labels — `uri` is the route TEMPLATE from
//!   axum's `MatchedPath`, e.g. `/ping/{name}`, never the concrete path;
//!   `"UNMATCHED"` when no route matched),
//! - opens a `http.request` tracing span enclosing the handler run, with the
//!   status code recorded once the response comes back,
//! - extracts W3C `traceparent`/`tracestate` via the process-global
//!   OpenTelemetry propagator and attaches it as the span's parent context.
//!
//! The extraction step is harmless with the default noop propagator (nothing
//! is set globally by this crate) — a later boot step (Task 9) installs a
//! real `TraceContextPropagator` globally, at which point this same code
//! starts producing real parent contexts without any change here.

use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) async fn observe(req: axum::extract::Request, next: axum::middleware::Next) -> axum::response::Response {
    let method = req.method().as_str().to_owned();
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "UNMATCHED".to_owned());

    let span = tracing::info_span!(
        "http.request",
        http.method = %method,
        http.route = %route,
        http.status_code = tracing::field::Empty,
    );
    let parent = opentelemetry::global::get_text_map_propagator(|prop| prop.extract(&HeaderCarrier(req.headers())));
    span.set_parent(parent);

    let started = std::time::Instant::now();
    // `next.run` drives the rest of the middleware stack and the handler
    // across await points; entering a span guard and holding it there would
    // be incorrect for async code (the guard isn't `Send` and doesn't track
    // task suspension), so the span is attached via `Instrument` instead.
    let response = next.run(req).instrument(span.clone()).await;

    let status = response.status().as_u16().to_string();
    span.record("http.status_code", status.as_str());

    metrics::histogram!(
        "http_server_requests_seconds",
        "method" => method, "uri" => route, "status" => status
    )
    .record(started.elapsed().as_secs_f64());

    response
}

struct HeaderCarrier<'a>(&'a axum::http::HeaderMap);

impl opentelemetry::propagation::Extractor for HeaderCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

inventory::submit! {
    oxide_web::RouterLayer {
        name: "oxide-observability/http",
        apply: |router| router.layer(oxide_web::__private::axum::middleware::from_fn(crate::http::observe)),
    }
}
