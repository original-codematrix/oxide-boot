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
//! The extraction step works because [`crate::otel`] installs a real
//! `TraceContextPropagator` globally during logging init, always (regardless
//! of whether OTLP export itself is configured) — this code doesn't know or
//! care whether that propagator is real or the library default noop.

use std::borrow::Cow;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// The method name without allocating, for the nine standard methods --
/// which is every request in practice.
///
/// A lookup table interning these labels was tried and reverted. Behind a
/// `Mutex` it serialised every request on one lock; as a thread-local
/// `HashMap` it still hashed three short strings per request with SipHash,
/// which costs more than the three small allocations it was replacing.
/// Alternating floor runs measured ~79,600 req/s without it against ~76,300
/// with it. A `match` costs nothing at all, so that part stays.
fn method_label(method: &axum::http::Method) -> Cow<'static, str> {
    match *method {
        axum::http::Method::GET => Cow::Borrowed("GET"),
        axum::http::Method::POST => Cow::Borrowed("POST"),
        axum::http::Method::PUT => Cow::Borrowed("PUT"),
        axum::http::Method::DELETE => Cow::Borrowed("DELETE"),
        axum::http::Method::HEAD => Cow::Borrowed("HEAD"),
        axum::http::Method::OPTIONS => Cow::Borrowed("OPTIONS"),
        axum::http::Method::PATCH => Cow::Borrowed("PATCH"),
        axum::http::Method::TRACE => Cow::Borrowed("TRACE"),
        axum::http::Method::CONNECT => Cow::Borrowed("CONNECT"),
        // Extension methods are rare enough that one allocation on that
        // path is not worth a lookup table on the common one.
        _ => Cow::Owned(method.as_str().to_owned()),
    }
}

/// Whether the request carries trace context worth extracting.
///
/// Building an OpenTelemetry `Context` and calling `set_parent` costs an
/// allocation and a propagator dispatch on every request. When no upstream
/// sent trace context there is nothing to inherit, and the span becomes a
/// root either way -- so the work is skipped rather than done and discarded.
fn has_trace_context(headers: &axum::http::HeaderMap) -> bool {
    headers.contains_key("traceparent") || headers.contains_key("baggage")
}

pub(crate) async fn observe(req: axum::extract::Request, next: axum::middleware::Next) -> axum::response::Response {
    let method = method_label(req.method());
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        // Owned rather than interned: see the note on `method_label` for why
        // a lookup table lost to the allocation here.
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "UNMATCHED".to_owned());

    let span = tracing::info_span!(
        "http.request",
        http.method = %method,
        http.route = %route,
        http.status_code = tracing::field::Empty,
        // Reserved slots a handler can fill via
        // `registry::record_request_field`. tracing fixes a span's fields at
        // creation and needs literal names, so they have to be declared here
        // -- empty until something records into them, and then absent from
        // the output rather than rendered as null.
        oxide.tenant = tracing::field::Empty,
        oxide.principal = tracing::field::Empty,
        oxide.entity = tracing::field::Empty,
        oxide.detail = tracing::field::Empty,
    );
    if has_trace_context(req.headers()) {
        let parent = opentelemetry::global::get_text_map_propagator(|prop| prop.extract(&HeaderCarrier(req.headers())));
        // `set_parent` (tracing-opentelemetry 0.33+) returns a `Result`,
        // failing only if the otel layer isn't registered, the span was
        // already started, or the span is filtered out — none of which should
        // abort request handling, so the error is discarded.
        let _ = span.set_parent(parent);
    }

    let started = std::time::Instant::now();
    // `next.run` drives the rest of the middleware stack and the handler
    // across await points; entering a span guard and holding it there would
    // be incorrect for async code (the guard isn't `Send` and doesn't track
    // task suspension), so the span is attached via `Instrument` instead.
    let response = next.run(req).instrument(span.clone()).await;

    // `as_str()` reads a static table and copies three bytes; the previous
    // `as_u16().to_string()` formatted an integer to get the same result.
    //
    // Going further and returning `Cow::Borrowed` from a `match` over the
    // common codes -- the same trick `method_label` uses above -- was tried
    // and reverted. It removed two of the four per-request allocations and
    // changed throughput by nothing: six alternating floor rounds on a quiet
    // machine measured 151,094 req/s against 150,252, a paired ratio of
    // 0.995 with three rounds falling either way and the ranges overlapping.
    // At 1.5% run-to-run spread an effect of even 2% would have shown. Thirty
    // lines of status codes to keep in sync were not worth a measured zero.
    let status = response.status().as_str().to_owned();
    span.record("http.status_code", status.as_str());

    let props = crate::metrics::HttpMetricsProps::current();
    if props.enabled {
        // The counter is always emitted when metrics are on: it is one
        // series per method/route/status, and rate, throughput and error
        // ratio are all derived from it. The histogram costs fourteen series
        // for the same triple, so it is separately switchable.
        // The histogram is recorded first so the counter can move the labels
        // rather than clone them. The previous order cloned unconditionally,
        // which meant `histogram: false` still paid for a second label set it
        // then threw away -- so the switch that exists to make metrics
        // cheaper saved nothing on the request path.
        if props.histogram {
            metrics::histogram!(
                "http_server_requests_seconds",
                "method" => method.clone(), "uri" => route.clone(), "status" => status.clone()
            )
            .record(started.elapsed().as_secs_f64());
        }

        metrics::counter!(
            "http_server_requests_total",
            "method" => method, "uri" => route, "status" => status
        )
        .increment(1);
    }

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
        apply: |_ctx, router| router.layer(axum::middleware::from_fn(crate::http::observe)),
    }
}
