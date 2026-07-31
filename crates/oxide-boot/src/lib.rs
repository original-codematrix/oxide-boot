//! Oxide Boot — a Spring-Boot-like application framework for Rust.

mod app;

pub use app::{AppBuilder, Application, BootError, OxideApplication};

/// Internal re-exports used by generated macro code. Not part of the
/// public API; do not use directly.
#[doc(hidden)]
pub mod __private {
    pub use inventory;
    pub use oxide_core::*;
    pub use tokio;
}

pub mod prelude {
    pub use crate::{Application, BootError, OxideApplication};
    pub use oxide_core::{AppConfig, ComponentResult, Inject};
}
