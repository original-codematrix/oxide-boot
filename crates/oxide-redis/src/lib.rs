//! Oxide Boot Redis starter: an injectable `RedisTemplate` over a
//! multiplexed, auto-reconnecting connection, wired into the boot lifecycle
//! with a health indicator.
//!
//! The Spring counterpart is `spring-boot-starter-data-redis`. Like it, the
//! handle is a *template* rather than a pool checkout: `ConnectionManager`
//! multiplexes commands over one connection and reconnects underneath, so
//! cloning the handle is cheap and there is nothing to return.
//!
//! Dragonfly speaks the Redis wire protocol, so it is a drop-in server here;
//! the integration tests run against it.

pub mod config;
mod health;

pub use config::{RedisProps, RedisTemplate};

/// Errors produced by the Redis starter.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error("failed to serialize a value to JSON: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("failed to deserialize the value stored at '{key}': {source}")]
    Deserialize {
        key: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("redis command timed out after {0:?}")]
    Timeout(std::time::Duration),
    #[error("redis client has not been started yet (post_construct has not run)")]
    NotStarted,
}

/// Result alias for the Redis starter.
pub type Result<T> = std::result::Result<T, Error>;
