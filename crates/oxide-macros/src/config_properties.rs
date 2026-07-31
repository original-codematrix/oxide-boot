use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitStr};

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let ident = &input.ident;

    let mut prefix: Option<LitStr> = None;
    for attr in &input.attrs {
        if attr.path().is_ident("config") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("prefix") {
                    prefix = Some(meta.value()?.parse()?);
                    Ok(())
                } else {
                    Err(meta.error("expected `prefix = \"...\"`"))
                }
            })?;
        }
    }
    let Some(prefix) = prefix else {
        return Err(syn::Error::new_spanned(
            ident,
            "missing #[config(prefix = \"...\")] attribute on ConfigProperties struct",
        ));
    };

    Ok(quote! {
        ::oxide_boot::__private::inventory::submit! {
            ::oxide_boot::__private::ComponentDescriptor {
                type_name: stringify!(#ident),
                type_id: || ::core::any::TypeId::of::<#ident>(),
                deps: || <[_]>::into_vec(::std::boxed::Box::new([(
                    ::core::any::TypeId::of::<::oxide_boot::__private::AppConfig>(),
                    "AppConfig",
                )])),
                construct: |__builder| {
                    let __config = __builder.resolve::<::oxide_boot::__private::AppConfig>()?;
                    let __props: #ident = __config.get(#prefix).map_err(|e| {
                        ::oxide_boot::__private::ContextError::Construct {
                            type_name: stringify!(#ident),
                            message: ::std::format!("binding prefix '{}': {}", #prefix, e),
                        }
                    })?;
                    __builder.insert(__props);
                    ::core::result::Result::Ok(())
                },
                post_construct: ::core::option::Option::None,
                pre_destroy: ::core::option::Option::None,
            }
        }
    })
}
