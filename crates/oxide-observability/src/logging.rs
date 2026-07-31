//! Logging setup, registered as an [`oxide_boot::LoggingInitializer`] at
//! priority 100 — this beats oxide-boot's own built-in fallback (priority 0)
//! whenever oxide-observability is on the dependency graph.
//!
//! Installs the Prometheus recorder first (so any code that runs during the
//! rest of boot can already record metrics), then a `tracing_subscriber::fmt`
//! subscriber: JSON when the `prod` profile is active or
//! `oxide.logging.format: json` is configured, pretty otherwise. The
//! `EnvFilter` directives come from `oxide_boot::logging_directives`, the
//! same builder the built-in fallback uses, so both stay in sync.
//!
//! The OpenTelemetry layer is added on top of this in Task 8.

use oxide_core::AppConfig;

inventory::submit! {
    oxide_boot::LoggingInitializer { priority: 100, init: crate::logging::init }
}

pub(crate) fn init(config: &AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    crate::metrics::prometheus_handle(); // recorder first, so early code can record

    let filter = oxide_boot::logging_directives(config);
    let json = config.profiles().iter().any(|p| p == "prod")
        || matches!(config.get_opt::<String>("oxide.logging.format")?, Some(f) if f == "json");

    // try_init: keep the first subscriber if tests boot multiple apps.
    if json {
        let _ = tracing_subscriber::fmt().with_env_filter(filter).json().try_init();
    } else {
        let _ = tracing_subscriber::fmt().with_env_filter(filter).pretty().try_init();
    }
    Ok(())
}
