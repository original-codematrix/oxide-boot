//! Procedural macros for `oxide-test`: the `#[oxide_test]` attribute is the
//! `@SpringBootTest` equivalent — it boots the application under test on
//! ephemeral ports (activating the `test` profile) and hands the test body a
//! reference to a running `TestApplication`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{FnArg, ItemFn, Pat};

/// Turns `async fn test_name(app: &TestApplication)` into a Rust test that
/// boots a `TestApplication` from `CARGO_MANIFEST_DIR` on the `test` profile
/// (ephemeral ports), runs the body with `&TestApplication`, then shuts down.
#[proc_macro_attribute]
pub fn oxide_test(_args: TokenStream, item: TokenStream) -> TokenStream {
    match expand(item.into()) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(item: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
    let input: ItemFn = syn::parse2(item)?;
    let name = &input.sig.ident;
    let body = &input.block;
    let vis = &input.vis;
    let attrs = &input.attrs;

    // At most one param, an `&TestApplication` binding.
    let mut param_idents: Vec<syn::Ident> = Vec::new();
    for arg in input.sig.inputs.iter() {
        match arg {
            FnArg::Receiver(_) => {
                return Err(syn::Error::new_spanned(arg, "#[oxide_test] test functions must not take `self`"));
            }
            FnArg::Typed(pt) => {
                let Pat::Ident(pid) = &*pt.pat else {
                    return Err(syn::Error::new_spanned(&pt.pat, "#[oxide_test] params must be plain bindings"));
                };
                param_idents.push(pid.ident.clone());
            }
        }
    }
    if param_idents.len() > 1 {
        return Err(syn::Error::new_spanned(
            &input.sig,
            "#[oxide_test] accepts at most one param: the injected `&TestApplication`",
        ));
    }

    let bind = match param_idents.first() {
        Some(binding) => quote! { let #binding = &__app; },
        None => quote! {},
    };

    // The three `expect`s below run inside a generated `#[test]` fn body,
    // not application/runtime code — panicking IS the correct, intended
    // failure signal for a test (that's how `#[test]` reports failure), so
    // these are deliberately left as `expect` rather than converted.
    Ok(quote! {
        #(#attrs)*
        #[::core::prelude::v1::test]
        #vis fn #name() {
            ::tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("#[oxide_test] built a tokio runtime")
                .block_on(async {
                    let __app = ::oxide_test::TestApp::builder()
                        .base_dir(::std::path::PathBuf::from(::std::env!("CARGO_MANIFEST_DIR")))
                        .test_profile()
                        .start()
                        .await
                        .expect("#[oxide_test] booted the application under test");
                    #bind
                    { #body }
                    __app.stop().await.expect("#[oxide_test] shut the application down");
                });
        }
    })
}
