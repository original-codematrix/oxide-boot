use proc_macro2::TokenStream;
use quote::quote;
use syn::{Field, Fields, Ident, ItemStruct, parse::Parser};

pub struct ComponentArgs {
    pub post_construct: Option<Ident>,
    pub pre_destroy: Option<Ident>,
}

pub fn parse_args(args: TokenStream) -> syn::Result<ComponentArgs> {
    let mut out = ComponentArgs { post_construct: None, pre_destroy: None };
    if args.is_empty() {
        return Ok(out);
    }
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("post_construct") {
            out.post_construct = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("pre_destroy") {
            out.pre_destroy = Some(meta.value()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("expected `post_construct = <method>` or `pre_destroy = <method>`"))
        }
    });
    parser.parse2(args)?;
    Ok(out)
}

fn has_default_attr(field: &Field) -> bool {
    field.attrs.iter().any(|a| a.path().is_ident("default"))
}

pub fn expand(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args = parse_args(args)?;
    let mut item: ItemStruct = syn::parse2(item)?;
    let ident = item.ident.clone();

    let Fields::Named(fields) = &mut item.fields else {
        return Err(syn::Error::new_spanned(
            &item.ident,
            "#[component] requires a struct with named fields (unit and tuple structs are not supported)",
        ));
    };

    let mut deps = Vec::new();
    let mut constructors = Vec::new();
    for field in fields.named.iter_mut() {
        // Unreachable: `fields` was destructured from `Fields::Named` above,
        // so every field here has an ident by definition. Also compile-time
        // only (macro expansion) — a panic here fails the build with a
        // (less friendly) message, it can never reach a running program.
        let name = field.ident.clone().expect("named field");
        let ty = field.ty.clone();
        if has_default_attr(field) {
            field.attrs.retain(|a| !a.path().is_ident("default"));
            constructors.push(quote! { #name: ::core::default::Default::default() });
        } else {
            deps.push(quote! {
                (::core::any::TypeId::of::<#ty>(), ::core::any::type_name::<#ty>())
            });
            constructors.push(quote! { #name: __builder.resolve::<#ty>()? });
            field.ty = syn::parse_quote! { ::oxide_boot::__private::Inject<#ty> };
        }
    }

    // Generated `expect`s below are safe by construction, at runtime: the
    // core assembler runs every component's `construct` closure to
    // completion before any `post_construct`/`pre_destroy` hook runs (see
    // `oxide_core::assemble`), so `#ident` is unconditionally already in the
    // context by the time either hook fires. Same pattern as the
    // hand-written component descriptors in oxide-web/oxide-mongo/etc.
    let maybe_post_construct = args.post_construct.as_ref().map(|method| {
        quote! {
            .post_construct(|__ctx: &::oxide_boot::__private::Context| {
                let __me = __ctx.get::<#ident>().expect("component was just constructed");
                ::std::boxed::Box::pin(async move { __me.#method().await })
            })
        }
    });
    let maybe_pre_destroy = args.pre_destroy.as_ref().map(|method| {
        quote! {
            .pre_destroy(|__ctx: &::oxide_boot::__private::Context| {
                let __me = __ctx.get::<#ident>().expect("component exists during shutdown");
                ::std::boxed::Box::pin(async move { __me.#method().await })
            })
        }
    });

    Ok(quote! {
        #item

        ::oxide_boot::__private::inventory::submit! {
            ::oxide_boot::__private::ComponentDescriptor::builder(
                stringify!(#ident),
                ::core::any::TypeId::of::<#ident>,
                |__builder| {
                    let __value = #ident { #(#constructors),* };
                    __builder.insert(__value);
                    ::core::result::Result::Ok(())
                },
            )
            .deps(|| <[_]>::into_vec(::std::boxed::Box::new([#(#deps),*])))
            #maybe_post_construct
            #maybe_pre_destroy
            .build()
        }
    })
}
