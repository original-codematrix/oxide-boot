//! Oxide Boot web starter: annotated controllers on top of axum, RFC 7807
//! error responses, and an HTTP server wired into the boot lifecycle.

pub mod error;
mod health;
pub mod management;
pub mod routing;
pub mod server;
mod serve;
pub use error::{ApiError, IntoProblemDetail, ProblemDetail, WebError};
pub use management::{ManagementProps, ManagementRoute, ManagementServer};
pub use oxide_web_macros::controller;
pub use routing::RouteDescriptor;
pub use server::{ServerProps, WebServer};

/// Internal re-exports used by generated macro code. Not part of the
/// public API; do not use directly.
#[doc(hidden)]
pub mod __private {
    pub use axum;
    pub use inventory;
    pub use oxide_core::Context;
}
