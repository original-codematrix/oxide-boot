//! Oxide Boot — a Spring-Boot-like application framework for Rust.

mod app;
mod logging;

pub use app::{AppBuilder, Application, BootError, OxideApplication};
pub use logging::{LoggingInitializer, logging_directives};
pub use oxide_core::Inject;
pub use oxide_macros::{ConfigProperties, component, configuration, main};

/// Internal re-exports used by generated macro code. Not part of the
/// public API; do not use directly.
///
/// Kept narrow on purpose: each item here is consumed by
/// `oxide-macros`-generated code (`#[component]`, `#[main]`,
/// `#[derive(ConfigProperties)]`) or by another crate's own `__private`
/// module built on top of it (`oxide-client`), not re-exported wholesale
/// from `oxide-core`.
#[doc(hidden)]
pub mod __private {
    pub use inventory;
    pub use oxide_core::serde_yaml_ng;
    pub use oxide_core::{AppConfig, ComponentDescriptor, ConfigDefaults, Context, ContextError, Inject, TraitBinding};
    pub use serde;
    pub use tokio;
}

pub mod prelude {
    pub use crate::{Application, BootError, OxideApplication};
    pub use oxide_core::{AppConfig, ComponentResult, Inject};
    pub use oxide_macros::{ConfigProperties, component};
}
