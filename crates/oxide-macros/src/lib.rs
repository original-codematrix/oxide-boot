//! Procedural macros for Oxide Boot. Generated code only references
//! paths under `::oxide_boot::__private`.

mod component;
mod config_properties;
mod main_macro;

mod configuration;

use proc_macro::TokenStream;

/// Marks a struct as an injectable singleton component.
#[proc_macro_attribute]
pub fn component(args: TokenStream, item: TokenStream) -> TokenStream {
    component::expand(args.into(), item.into()).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// Binds a config subtree (`#[config(prefix = "...")]`) to this struct and
/// registers it as an injectable component. Requires `serde::Deserialize`.
///
/// Adding `default` (`#[config(prefix = "...", default)]`) tolerates a
/// missing section, falling back to `Default`. Such structs must *also*
/// derive `serde::Serialize`: their `Default` value is serialized and
/// seeded into the config tree before `application.yml` overrides run, so
/// that multi-segment kebab-case keys (e.g. `request-timeout-seconds`)
/// exist as leaves and `OXIDE_*` environment overrides can bind to them
/// even when the section is absent from YAML entirely. A struct that omits
/// `serde::Serialize` here fails to compile with a pointed error.
#[proc_macro_derive(ConfigProperties, attributes(config))]
pub fn config_properties(input: TokenStream) -> TokenStream {
    config_properties::expand(input.into()).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// Entry point: builds a multi-threaded Tokio runtime and runs the async body.
#[proc_macro_attribute]
pub fn main(_args: TokenStream, item: TokenStream) -> TokenStream {
    main_macro::expand(item.into()).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// `@Configuration` — an impl block of `#[bean]` factory methods.
///
/// See [`configuration`](crate::configuration) for the full rationale; in
/// short, this covers what `#[component]` cannot: types the application does
/// not own, types built by a builder, and more than one instance of a type.
#[proc_macro_attribute]
pub fn configuration(args: TokenStream, item: TokenStream) -> TokenStream {
    match configuration::expand(args.into(), item.into()) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
