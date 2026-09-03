//! Core building blocks of Oxide Boot: configuration loading and the
//! dependency-injection container. Users normally depend on `oxide-boot`,
//! not on this crate directly.

pub mod config;
pub mod container;
pub use config::{AppConfig, ConfigDefaults, ConfigError};
pub use container::{
    ComponentDescriptor, ComponentResult, Context, ContextBuilder, ContextError, HookFn, Inject, assemble,
};

// Re-exported so `oxide-macros`-generated code (which only depends on
// `oxide-boot`, not directly on `serde_yaml_ng`) can serialize a
// `ConfigDefaults::defaults_yaml` value via `oxide_boot::__private::serde_yaml_ng`.
pub use serde_yaml_ng;
