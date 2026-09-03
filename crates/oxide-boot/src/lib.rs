//! Oxide Boot — a Spring-Boot-like application framework for Rust.

mod app;
mod logging;

pub use app::{AppBuilder, Application, BootError, OxideApplication};
pub use logging::{LoggingInitializer, logging_directives};
pub use oxide_macros::{ConfigProperties, component, main};

/// Internal re-exports used by generated macro code. Not part of the
/// public API; do not use directly.
#[doc(hidden)]
pub mod __private {
    pub use crate::logging::LoggingInitializer;
    pub use inventory;
    pub use oxide_core::*;
    pub use serde;
    pub use tokio;
}

pub mod prelude {
    pub use crate::{Application, BootError, OxideApplication};
    pub use oxide_core::{AppConfig, ComponentResult, Inject};
    pub use oxide_macros::{ConfigProperties, component};
}
