//! Observability helpers for the Kafka starter: W3C `traceparent` injection
//! and extraction over Kafka message headers, plus metric label helpers.
//!
//! Headers travel as `(name, value-bytes)` pairs (rdkafka `OwnedHeaders` and
//! the `ReceivedMessage::headers` shape both round-trip through this). The
//! W3C propagator is a genuine process-global (installed by
//! `oxide-observability`); without a global propagator set, injection and
//! extraction are no-ops.

use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector};

/// A mutable `(name, value)` header list usable as an `Injector`/`Extractor`
/// for the OpenTelemetry `TextMapPropagator` API.
#[derive(Debug)]
pub struct HeaderCarrier<'a> {
    pub headers: &'a mut Vec<(String, Vec<u8>)>,
}

impl Injector for HeaderCarrier<'_> {
    fn set(&mut self, key: &str, value: String) {
        // Replace any existing header of the same (case-insensitive) name.
        let upper = key.to_ascii_uppercase();
        if let Some(slot) = self.headers.iter_mut().find(|(k, _)| k.to_ascii_uppercase() == upper) {
            *slot = (key.to_string(), value.into_bytes());
        } else {
            self.headers.push((key.to_string(), value.into_bytes()));
        }
    }
}

impl Extractor for HeaderCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        let upper = key.to_ascii_uppercase();
        self.headers
            .iter()
            .find(|(k, _)| k.to_ascii_uppercase() == upper)
            .and_then(|(_, v)| std::str::from_utf8(v).ok())
    }
    fn keys(&self) -> Vec<&str> {
        self.headers.iter().map(|(k, _)| k.as_str()).collect()
    }
}

/// Injects the W3C `traceparent`(and `tracestate`) from `cx` into `headers`.
/// Uses the process-global propagator; no-op if none is installed.
pub fn inject_traceparent(cx: &Context, headers: &mut Vec<(String, Vec<u8>)>) {
    let mut carrier = HeaderCarrier { headers };
    opentelemetry::global::get_text_map_propagator(|prop| prop.inject_context(cx, &mut carrier));
}

/// Extracts a W3C trace context from `headers`, parenting the returned
/// context on any incoming `traceparent`.
pub fn extract_traceparent(headers: &[(String, Vec<u8>)]) -> Context {
    let mut owned = headers.to_vec();
    let carrier = HeaderCarrier { headers: &mut owned };
    opentelemetry::global::get_text_map_propagator(|prop| prop.extract_with_context(&Context::current(), &carrier))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{Tracer as _, TracerProvider as _};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::{Sampler, TracerProvider};

    /// Proves that `inject_traceparent`, given a real span context and the W3C
    /// propagator installed, writes a well-formed `traceparent` header — this
    /// is exactly the contract the producer's `kafka.send` path relies on.
    #[test]
    fn inject_writes_traceparent_from_span_context() {
        // Install a W3C propagator (as `oxide-observability` does on boot) and a
        // real tracer provider so a span carries a genuine trace id.
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let provider = TracerProvider::builder().with_sampler(Sampler::AlwaysOn).build();
        let tracer = provider.tracer("observe-test");

        let span = tracer.start("unit-span");
        let cx = opentelemetry::trace::TraceContextExt::current_with_span(span);

        let mut headers: Vec<(String, Vec<u8>)> = Vec::new();
        inject_traceparent(&cx, &mut headers);

        // The carrier must now contain a `traceparent` header.
        let traceparent =
            headers.iter().find(|(k, _)| k.eq_ignore_ascii_case("traceparent")).expect("traceparent header injected");
        let value = std::str::from_utf8(&traceparent.1).expect("utf8");
        // Format: 00-<32 hex trace-id>-<16 hex span-id>-<2 flags>
        let parts: Vec<&str> = value.split('-').collect();
        assert_eq!(parts.len(), 4, "traceparent has the 4 dash-separated parts: {value}");
        assert_eq!(parts[0], "00");
        assert_eq!(parts[1].len(), 32, "trace-id is 32 hex chars");
        assert_eq!(parts[2].len(), 16, "span-id is 16 hex chars");
        assert_eq!(parts[3].len(), 2, "trace-flags is 2 hex chars");
        assert!(parts[3].chars().all(|c| c.is_ascii_hexdigit()));
    }
}
