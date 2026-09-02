//! Procedural macros for the Oxide Boot HTTP client starter.

mod http_client;

use proc_macro::TokenStream;

/// Turns a trait whose methods are annotated with `#[get]`/`#[post]`/... into
/// a declarative FeignClient-style HTTP client, registered as a component.
#[proc_macro_attribute]
pub fn http_client(args: TokenStream, item: TokenStream) -> TokenStream {
    http_client::expand(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}