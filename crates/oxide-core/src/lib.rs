//! Core building blocks of Oxide Boot: configuration loading and the
//! dependency-injection container. Users normally depend on `oxide-boot`,
//! not on this crate directly.

pub mod config;
pub mod container;
pub use config::{AppConfig, ConfigError};
pub use container::{
    ComponentDescriptor, ComponentResult, Context, ContextBuilder, ContextError, HookFn, Inject, assemble,
};
