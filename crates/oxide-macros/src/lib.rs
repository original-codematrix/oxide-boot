//! Procedural macros for Oxide Boot. Generated code only references
//! paths under `::oxide_boot::__private`.

mod component;

use proc_macro::TokenStream;

/// Marks a struct as an injectable singleton component.
#[proc_macro_attribute]
pub fn component(args: TokenStream, item: TokenStream) -> TokenStream {
    component::expand(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
