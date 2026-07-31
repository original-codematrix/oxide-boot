//! Oxide Boot web starter: annotated controllers on top of axum, RFC 7807
//! error responses, and an HTTP server wired into the boot lifecycle.

pub mod error;
pub use error::{ApiError, IntoProblemDetail, ProblemDetail, WebError};

/// Internal re-exports used by generated macro code. Not part of the
/// public API; do not use directly.
#[doc(hidden)]
pub mod __private {
    pub use axum;
    pub use inventory;
}
