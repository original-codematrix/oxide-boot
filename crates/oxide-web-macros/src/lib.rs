//! Procedural macros for the Oxide Boot web starter. Generated code only
//! references paths under `::oxide_web::__private`.

mod controller;

use proc_macro::TokenStream;

/// Turns an impl block into HTTP routes: methods annotated with
/// #[get]/#[post]/#[put]/#[delete]/#[patch] are mounted on the web server.
#[proc_macro_attribute]
pub fn controller(args: TokenStream, item: TokenStream) -> TokenStream {
    controller::expand(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
