//! Logging setup, registered as an [`oxide_boot::LoggingInitializer`] at
//! priority 100 — this beats oxide-boot's own built-in fallback (priority 0)
//! whenever oxide-observability is on the dependency graph.
//!
//! Installs the Prometheus recorder first (so any code that runs during the
//! rest of boot can already record metrics), then layers a
//! `tracing_subscriber::registry()` subscriber: an `EnvFilter` (directives
//! from `oxide_boot::logging_directives`, the same builder the built-in
//! fallback uses, so both stay in sync), the optional [`crate::otel`] OTEL
//! layer (present only when `oxide.otel.endpoint` is configured), and a
//! `fmt` layer — JSON when the `prod` profile is active or
//! `oxide.logging.format: json` is configured, pretty otherwise.
//!
//! The `registry()` + layer-stack shape (rather than the `fmt()` shortcut
//! this crate used through Task 8) exists so the OTEL layer, which is only
//! sometimes present, can be composed in via `Option<Layer>` without a
//! second, incompatible subscriber type per branch.

use oxide_core::AppConfig;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

inventory::submit! {
    oxide_boot::LoggingInitializer { priority: 100, init: crate::logging::init }
}

pub(crate) fn init(config: &AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    crate::metrics::prometheus_handle(); // recorder first, so early code can record

    let filter = tracing_subscriber::EnvFilter::new(oxide_boot::logging_directives(config));
    let json = config.profiles().iter().any(|p| p == "prod")
        || matches!(config.get_opt::<String>("oxide.logging.format")?, Some(f) if f == "json");

    // Global propagator + (if `oxide.otel.endpoint` is set) tracer provider;
    // `None` when export is off, which is the common case and not an error.
    let otel_layer = crate::otel::build_layer(config)?;

    // The OTEL layer goes on `Registry` directly (its type is pinned to the
    // exact subscriber it's attached to — that's `tracing-opentelemetry`'s
    // design, not a choice made here), so it has to be the first `.with()`
    // call; `EnvFilter` and the `fmt` layer are generic over the subscriber
    // they attach to and work at any position. `Layered::enabled` ANDs
    // across every layer in the stack regardless of order, so the filter
    // still applies to everything below it.
    let registry = tracing_subscriber::registry().with(otel_layer).with(filter);

    // try_init: keep the first subscriber if tests boot multiple apps.
    // Two branches (not a shared `let fmt = if json {..} else {..}`)
    // because `.json()` and `.pretty()` produce different concrete Layer
    // types that don't unify without boxing.
    if json {
        let _ = registry.with(tracing_subscriber::fmt::layer().json()).try_init();
    } else {
        let _ = registry.with(tracing_subscriber::fmt::layer().pretty()).try_init();
    }
    Ok(())
}
