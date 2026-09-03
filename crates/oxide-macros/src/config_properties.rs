use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, LitStr};

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let ident = &input.ident;

    let mut prefix: Option<LitStr> = None;
    let mut has_default = false;
    for attr in &input.attrs {
        if attr.path().is_ident("config") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("prefix") {
                    prefix = Some(meta.value()?.parse()?);
                    Ok(())
                } else if meta.path.is_ident("default") {
                    has_default = true;
                    Ok(())
                } else {
                    Err(meta.error("expected `prefix = \"...\"` or `default`"))
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

    let defaults_registration = if has_default {
        // `default` structs get their `Default` value serialized and seeded into
        // the config tree (see `oxide_core::config::seed_defaults`), so they must
        // additionally implement `serde::Serialize`. That's a semantic property,
        // not something this proc-macro can see at expansion time (it only sees
        // syntax) — so instead of letting a raw, macro-internal
        // `serde_yaml_ng::to_string` trait-bound error leak through, route
        // serialization through a helper generic over a purpose-built
        // supertrait-of-`Serialize` with a custom `#[diagnostic::on_unimplemented]`
        // message that names the real cause and how to fix it.
        let assert_trait = format_ident!("OxideConfigPropertiesRequiresSerializeFor{}", ident);
        let defaults_fn = format_ident!("__oxide_config_properties_defaults_yaml_for_{}", ident);
        quote! {
            #[doc(hidden)]
            #[diagnostic::on_unimplemented(
                message = "`{Self}` has `#[config(prefix = \"...\", default)]` but does not implement `serde::Serialize`",
                note = "its `Default` value is serialized and seeded into the config tree so environment-variable \
                        overrides can bind to kebab-case keys even when the section is absent from YAML — derive \
                        `serde::Serialize` on this struct in addition to `serde::Deserialize`"
            )]
            trait #assert_trait: ::oxide_boot::__private::serde::Serialize {}
            impl<T: ::oxide_boot::__private::serde::Serialize> #assert_trait for T {}

            #[doc(hidden)]
            #[allow(non_snake_case)]
            fn #defaults_fn<T: #assert_trait + ::core::default::Default>() -> ::std::string::String {
                ::oxide_boot::__private::serde_yaml_ng::to_string(&T::default()).unwrap_or_default()
            }

            ::oxide_boot::__private::inventory::submit! {
                ::oxide_boot::__private::ConfigDefaults {
                    prefix: #prefix,
                    defaults_yaml: #defaults_fn::<#ident>,
                }
            }
        }
    } else {
        quote! {}
    };

    let construct_body = if has_default {
        quote! {
            let __config = __builder.resolve::<::oxide_boot::__private::AppConfig>()?;
            let __props: #ident = __config
                .get_opt(#prefix)
                .map_err(|e| {
                    ::oxide_boot::__private::ContextError::Construct {
                        type_name: stringify!(#ident),
                        message: ::std::format!("binding prefix '{}': {}", #prefix, e),
                    }
                })?
                .unwrap_or_default();
            __builder.insert(__props);
            ::core::result::Result::Ok(())
        }
    } else {
        quote! {
            let __config = __builder.resolve::<::oxide_boot::__private::AppConfig>()?;
            let __props: #ident = __config.get(#prefix).map_err(|e| {
                ::oxide_boot::__private::ContextError::Construct {
                    type_name: stringify!(#ident),
                    message: ::std::format!("binding prefix '{}': {}", #prefix, e),
                }
            })?;
            __builder.insert(__props);
            ::core::result::Result::Ok(())
        }
    };

    Ok(quote! {
        ::oxide_boot::__private::inventory::submit! {
            ::oxide_boot::__private::ComponentDescriptor::builder(
                stringify!(#ident),
                ::core::any::TypeId::of::<#ident>,
                |__builder| {
                    #construct_body
                },
            )
            .deps(|| <[_]>::into_vec(::std::boxed::Box::new([(
                ::core::any::TypeId::of::<::oxide_boot::__private::AppConfig>(),
                "AppConfig",
            )])))
            .build()
        }

        #defaults_registration
    })
}
