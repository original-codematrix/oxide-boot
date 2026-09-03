use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

pub fn expand(item: TokenStream) -> syn::Result<TokenStream> {
    let func: ItemFn = syn::parse2(item)?;
    if func.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(func.sig.fn_token, "#[oxide_boot::main] requires an async fn"));
    }
    let body = &func.block;
    let output = &func.sig.output;
    Ok(quote! {
        fn main() #output {
            // `main`'s return type is whatever the user's async fn declared
            // (`()`, `Result<_, _>`, ...), so there is no way to construct an
            // error value of an arbitrary caller-chosen type here — there is
            // no error channel to propagate a runtime-build failure into.
            // Matches `#[tokio::main]`'s own generated code, which does the
            // same. Failure here means the OS refused to create the async
            // runtime's worker threads, which is not a recoverable condition
            // for this process anyway.
            ::oxide_boot::__private::tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime")
                .block_on(async #body)
        }
    })
}
