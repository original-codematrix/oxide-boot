//! Pre-registering metrics, and adding fields to the request span.
//!
//! Two gaps that only show up in production.
//!
//! **A counter that has never fired does not exist.** Prometheus renders
//! nothing for it, so `rate(orders_failed_total[5m])` returns no series
//! rather than zero, and an alert built on it never fires -- it stays silent
//! exactly while nothing has gone wrong yet, and keeps staying silent when
//! the first failure arrives into an unseen series. Spring's Micrometer
//! avoids this by registering meters up front. [`MetricRegistration`] is the
//! seam for the same thing here: declare the metric at boot, it renders as
//! `0`, and the alert has something to watch from the first scrape.
//!
//! **The request span's fields are fixed at creation.** `tracing` requires
//! every field to be declared when the span is opened, and the names must be
//! literals at compile time -- so a handler cannot attach an arbitrary
//! `tenant_id` to the span covering its own request. [`RequestField`] is the
//! set of slots declared empty on every request span, and
//! [`record_request_field`] fills one from inside a handler.

use oxide_core::{ComponentDescriptor, Context};

/// Kind of metric to pre-register. Histograms are deliberately absent: an
/// empty histogram renders its buckets with zero counts, which is already
/// the useful state, and pre-declaring one buys nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
}

/// Declares a metric that should exist from boot, with an optional
/// description and a fixed set of label values.
///
/// Registered via `inventory`, so a starter or an application declares its
/// metrics next to the code that emits them:
///
/// ```ignore
/// inventory::submit! {
///     MetricRegistration::counter("orders_failed_total")
///         .describe("Orders rejected during checkout")
/// }
/// ```
///
/// Labelled metrics need every label *value* named up front, because a
/// counter with labels is really one series per combination and only the
/// combinations you list can be rendered as zero:
///
/// ```ignore
/// inventory::submit! {
///     MetricRegistration::counter("orders_total").with_labels(&[&[("status", "ok")], &[("status", "failed")]])
/// }
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub struct MetricRegistration {
    pub name: &'static str,
    pub kind: MetricKind,
    pub description: Option<&'static str>,
    /// One entry per series to create. Empty means a single unlabelled
    /// series.
    pub label_sets: &'static [&'static [(&'static str, &'static str)]],
}

impl MetricRegistration {
    pub const fn counter(name: &'static str) -> Self {
        Self { name, kind: MetricKind::Counter, description: None, label_sets: &[] }
    }

    pub const fn gauge(name: &'static str) -> Self {
        Self { name, kind: MetricKind::Gauge, description: None, label_sets: &[] }
    }

    pub const fn describe(mut self, text: &'static str) -> Self {
        self.description = Some(text);
        self
    }

    pub const fn with_labels(mut self, label_sets: &'static [&'static [(&'static str, &'static str)]]) -> Self {
        self.label_sets = label_sets;
        self
    }
}

inventory::collect!(MetricRegistration);

/// Creates every declared metric at its zero value.
///
/// `increment(0)` and `set(0.0)` are what registers a series with the
/// recorder without changing it -- there is no separate "declare" call in the
/// `metrics` facade.
fn install_declared_metrics() {
    for reg in inventory::iter::<MetricRegistration> {
        if let Some(text) = reg.description {
            match reg.kind {
                MetricKind::Counter => metrics::describe_counter!(reg.name, text),
                MetricKind::Gauge => metrics::describe_gauge!(reg.name, text),
            }
        }
        if reg.label_sets.is_empty() {
            match reg.kind {
                MetricKind::Counter => metrics::counter!(reg.name).increment(0),
                MetricKind::Gauge => metrics::gauge!(reg.name).set(0.0),
            }
            continue;
        }
        for set in reg.label_sets {
            let labels: Vec<metrics::Label> = set.iter().map(|(k, v)| metrics::Label::new(*k, *v)).collect();
            match reg.kind {
                MetricKind::Counter => metrics::counter!(reg.name, labels).increment(0),
                MetricKind::Gauge => metrics::gauge!(reg.name, labels).set(0.0),
            }
        }
    }
}

/// A slot on the HTTP request span that a handler can fill in.
///
/// `tracing` fixes a span's field set when the span is opened, and field
/// names must be literals at compile time -- so a framework cannot offer
/// "declare any field you like at runtime" for a span it creates itself.
/// What it can offer is a set of reserved slots, declared empty on every
/// request span and filled from inside a handler once the value is known.
///
/// These four cover what request traces are normally enriched with. For
/// anything else, open a child span in the handler, which nests under the
/// request span and takes whatever fields you want:
///
/// ```ignore
/// let _s = tracing::info_span!("checkout", basket_size = items.len()).entered();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestField {
    /// Which tenant the request belongs to.
    Tenant,
    /// Who is making it -- a user id, a service account, an API key label.
    Principal,
    /// The primary entity it acts on, when the route parameter alone is not
    /// enough to identify it.
    Entity,
    /// One free-form value, for the case that does not fit the three above.
    Detail,
}

impl RequestField {
    /// The literal declared on the span in `crate::http`. Kept in one place
    /// so the two cannot drift apart.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Tenant => "oxide.tenant",
            Self::Principal => "oxide.principal",
            Self::Entity => "oxide.entity",
            Self::Detail => "oxide.detail",
        }
    }
}

/// Records a value into one of the request span's reserved slots.
///
/// Call this from inside a handler: it targets the current span, which is
/// the request span the observability layer opened. Recording the same slot
/// twice keeps the last value, which is `tracing`'s own behaviour.
pub fn record_request_field(field: RequestField, value: impl std::fmt::Display) {
    tracing::Span::current().record(field.as_str(), tracing::field::display(value));
}

/// Marker component; installs declared metrics once the recorder exists.
pub struct MetricRegistry {
    _private: (),
}

inventory::submit! {
    ComponentDescriptor::builder(
        "MetricRegistry",
        std::any::TypeId::of::<MetricRegistry>,
        |b| {
            b.insert(MetricRegistry { _private: () });
            Ok(())
        },
    )
    // The recorder is installed by `MetricsEndpoint`'s own construction, so
    // this has to run after it or every declared metric would be recorded
    // into a recorder that is later replaced.
    .deps(|| vec![(std::any::TypeId::of::<crate::MetricsEndpoint>(), "MetricsEndpoint")])
    .post_construct(|_ctx: &Context| {
        Box::pin(async move {
            install_declared_metrics();
            let declared = inventory::iter::<MetricRegistration>.into_iter().count();
            if declared > 0 {
                tracing::debug!(declared, "pre-registered metrics at zero");
            }
            Ok(())
        })
    })
    .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_counter_registration_defaults_to_one_unlabelled_series() {
        let reg = MetricRegistration::counter("x_total");
        assert_eq!(reg.kind, MetricKind::Counter);
        assert!(reg.label_sets.is_empty());
        assert!(reg.description.is_none());
    }

    #[test]
    fn labels_and_description_are_carried_through() {
        static SETS: &[&[(&str, &str)]] = &[&[("status", "ok")], &[("status", "failed")]];
        let reg = MetricRegistration::counter("orders_total").describe("Orders seen").with_labels(SETS);
        assert_eq!(reg.description, Some("Orders seen"));
        assert_eq!(reg.label_sets.len(), 2);
    }

    #[test]
    fn slot_names_match_what_the_span_declares() {
        // The span in `crate::http` declares these literals; if either side
        // is edited alone, `record_request_field` silently does nothing.
        assert_eq!(RequestField::Tenant.as_str(), "oxide.tenant");
        assert_eq!(RequestField::Principal.as_str(), "oxide.principal");
        assert_eq!(RequestField::Entity.as_str(), "oxide.entity");
        assert_eq!(RequestField::Detail.as_str(), "oxide.detail");
    }
}
