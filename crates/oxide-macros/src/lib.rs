//! Procedural macros for Oxide Boot. Generated code only references
//! paths under `::oxide_boot::__private`.

mod component;
mod config_properties;

use proc_macro::TokenStream;

/// Marks a struct as an injectable singleton component.
#[proc_macro_attribute]
pub fn component(args: TokenStream, item: TokenStream) -> TokenStream {
    component::expand(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Binds a config subtree (`#[config(prefix = "...")]`) to this struct and
/// registers it as an injectable component. Requires `serde::Deserialize`.
#[proc_macro_derive(ConfigProperties, attributes(config))]
pub fn config_properties(input: TokenStream) -> TokenStream {
    config_properties::expand(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
