//! `#[configuration]` and `#[bean]` — Spring's `@Configuration` / `@Bean`.
//!
//! `#[component]` covers the case where the container can build the type
//! itself: a struct whose fields are other components. That does not reach a
//! type the application does not own -- a driver handle, a client from
//! another crate, anything constructed by a builder -- and it cannot produce
//! two instances of one type.
//!
//! A configuration is a plain `impl` block whose methods are factories:
//!
//! ```ignore
//! pub struct Clients;
//!
//! #[configuration]
//! impl Clients {
//!     #[bean(primary)]
//!     fn orders(props: HttpProps) -> HttpClient { HttpClient::new(&props.orders_url) }
//!
//!     #[bean(name = "billing")]
//!     fn billing(props: HttpProps) -> HttpClient { HttpClient::new(&props.billing_url) }
//! }
//! ```
//!
//! Each `#[bean]` method becomes a component in its own right: its
//! parameters are its dependencies, resolved from the container, and its
//! return value is registered. Methods take no `self` -- a configuration is
//! a namespace, not state, and giving it state would raise the question of
//! who constructs *it*.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{FnArg, ImplItem, ItemImpl, Type};

#[derive(Default)]
struct BeanArgs {
    name: Option<String>,
    primary: bool,
}

/// Parses `#[bean]`, `#[bean(primary)]`, `#[bean(name = "x")]`,
/// `#[bean(name = "x", primary)]`.
fn parse_bean_args(attr: &syn::Attribute) -> syn::Result<BeanArgs> {
    let mut out = BeanArgs::default();
    if matches!(attr.meta, syn::Meta::Path(_)) {
        return Ok(out);
    }
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("primary") {
            out.primary = true;
            Ok(())
        } else if meta.path.is_ident("name") {
            let value: syn::LitStr = meta.value()?.parse()?;
            out.name = Some(value.value());
            Ok(())
        } else {
            Err(meta.error("expected `name = \"...\"` or `primary`"))
        }
    })?;
    Ok(out)
}

/// `Some(T)` when `ty` is written `Inject<T>`, matched by the last path
/// segment so `oxide_boot::Inject<T>` and a bare `Inject<T>` both work.
fn inject_inner(ty: &Type) -> Option<Type> {
    let Type::Path(path) = ty else { return None };
    let last = path.path.segments.last()?;
    if last.ident != "Inject" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else { return None };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(inner) => Some(inner.clone()),
        _ => None,
    })
}

pub fn expand(_args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let mut block: ItemImpl = syn::parse2(item)?;
    let self_ty = &block.self_ty;

    let mut registrations = Vec::new();

    for item in &mut block.items {
        let ImplItem::Fn(method) = item else { continue };
        let Some(index) = method.attrs.iter().position(|a| a.path().is_ident("bean")) else { continue };
        // Removed rather than left in place: `#[bean]` is this macro's own
        // marker and would be an unknown attribute to the compiler.
        let attr = method.attrs.remove(index);
        let args = parse_bean_args(&attr)?;

        if method.sig.asyncness.is_some() {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "#[bean] methods are synchronous: they run during container construction, before any runtime work \
                 is allowed. Do the async part in a `#[component(post_construct = ...)]` hook on the produced type.",
            ));
        }
        if method.sig.inputs.iter().any(|a| matches!(a, FnArg::Receiver(_))) {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "#[bean] methods take no `self`: a configuration is a namespace, not a component, so nothing \
                 constructs it. Take what you need as parameters instead — they are resolved from the container.",
            ));
        }

        let method_name = &method.sig.ident;
        let return_ty = match &method.sig.output {
            syn::ReturnType::Type(_, ty) => (**ty).clone(),
            syn::ReturnType::Default => {
                return Err(syn::Error::new_spanned(
                    &method.sig,
                    "#[bean] must return the type it produces; a method returning `()` registers nothing",
                ));
            }
        };

        // Each parameter is a dependency, resolved by type. Reference
        // parameters are rejected: the container hands out `Inject<T>`, and
        // a borrow could not outlive the construction closure.
        let mut param_types = Vec::new();
        for arg in &method.sig.inputs {
            let FnArg::Typed(pat) = arg else { continue };
            if matches!(&*pat.ty, Type::Reference(_)) {
                return Err(syn::Error::new_spanned(
                    &pat.ty,
                    "#[bean] parameters are taken by value: the container owns its components and hands out \
                     cheap handles, so a borrow here could not outlive construction. Use `T` rather than `&T`.",
                ));
            }
            param_types.push((*pat.ty).clone());
        }

        // A parameter written as `Inject<T>` is handed the container's own
        // handle; anything else is cloned out of it. Cheap handles
        // (`RedisTemplate`, `KafkaProducer`, ...) read better written
        // plainly, and a type that is not `Clone` is still reachable by
        // asking for `Inject<T>` instead of gaining a derive it does not
        // want.
        let resolve_params = param_types.iter().map(|ty| match inject_inner(ty) {
            Some(inner) => quote! { __builder.resolve::<#inner>()? },
            None => quote! { (*__builder.resolve::<#ty>()?).clone() },
        });
        let deps = param_types.iter().map(|ty| {
            let resolved = inject_inner(ty).unwrap_or_else(|| ty.clone());
            quote! { (::core::any::TypeId::of::<#resolved>(), ::core::stringify!(#resolved)) }
        });

        let bean_name = match &args.name {
            Some(name) => quote! { ::core::option::Option::Some(#name) },
            None => quote! { ::core::option::Option::None },
        };
        let primary = args.primary;
        // The descriptor's identity has to be unique per bean, not per type:
        // two beans of the same type would otherwise collide in `assemble`'s
        // duplicate check. A zero-sized marker type per method provides that.
        let marker = quote::format_ident!("__OxideBean_{}", method_name);
        let label = format!("{}::{}", quote!(#self_ty), method_name);

        registrations.push(quote! {
            #[doc(hidden)]
            #[allow(non_camel_case_types)]
            pub struct #marker;

            ::oxide_boot::__private::inventory::submit! {
                ::oxide_boot::__private::ComponentDescriptor::builder(
                    #label,
                    ::core::any::TypeId::of::<#marker>,
                    |__builder| {
                        let __value = <#self_ty>::#method_name( #(#resolve_params),* );
                        __builder.insert_qualified::<#return_ty>(#bean_name, #primary, __value);
                        ::core::result::Result::Ok(())
                    },
                )
                .deps(|| <[_]>::into_vec(::std::boxed::Box::new([#(#deps),*])))
                .build()
            }
        });
    }

    if registrations.is_empty() {
        return Err(syn::Error::new_spanned(
            &block.self_ty,
            "#[configuration] found no #[bean] methods — either mark the factory methods with #[bean], or drop \
             the attribute",
        ));
    }

    Ok(quote! {
        #block
        #(#registrations)*
    })
}
