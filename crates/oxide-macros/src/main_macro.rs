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
            ::oxide_boot::__private::tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime")
                .block_on(async #body)
        }
    })
}
