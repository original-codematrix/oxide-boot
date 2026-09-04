use proc_macro2::TokenStream;
use quote::quote;
use syn::{Field, Fields, Ident, ItemStruct, parse::Parser};

pub struct ComponentArgs {
    pub post_construct: Option<Ident>,
    /// Whether `post_construct`'s method takes `(&Context)`, written
    /// `post_construct = method(ctx)` — see [`parse_hook_value`].
    pub post_construct_with_ctx: bool,
    pub pre_destroy: Option<Ident>,
    /// Whether `pre_destroy`'s method takes `(&Context)`, written
    /// `pre_destroy = method(ctx)` — see [`parse_hook_value`].
    pub pre_destroy_with_ctx: bool,
    /// Shutdown slot, same semantics as `ComponentDescriptor::destroy_order`
    /// (see `oxide_core::container`): defaults to `0`, the framework's
    /// normal-component default, when the argument is absent.
    pub destroy_order: i32,
    /// Traits this component should also be resolvable as, written
    /// `provides(Greeter, Shutdownable)`. Each becomes a `TraitBinding`, so
    /// the component turns up in `ctx.get_all::<dyn Greeter>()` while
    /// `ctx.get::<Self>()` keeps working unchanged.
    pub provides: Vec<syn::Path>,
}

/// Parses a `post_construct`/`pre_destroy` value: either a bare method name
/// (`start`), or a method name followed by a parenthesized `ctx` marker
/// (`start(ctx)`) requesting the assembled `&Context` as that method's one
/// argument. The marker is needed because the macro only sees the struct,
/// not the method's real signature, so it cannot infer whether the method
/// wants `(&self)` or `(&self, &Context)` — the generated hook closure has
/// to know which shape to call before the method itself is even resolved.
fn parse_hook_value(input: syn::parse::ParseStream) -> syn::Result<(Ident, bool)> {
    let method: Ident = input.parse()?;
    if !input.peek(syn::token::Paren) {
        return Ok((method, false));
    }
    let content;
    syn::parenthesized!(content in input);
    let bad_arg = || {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "expected `{method}(ctx)` (only `ctx` is accepted as a hook argument), e.g. `post_construct = {method}(ctx)`"
            ),
        )
    };
    let arg: Ident = content.parse().map_err(|_| bad_arg())?;
    if arg != "ctx" || !content.is_empty() {
        return Err(bad_arg());
    }
    Ok((method, true))
}

pub fn parse_args(args: TokenStream) -> syn::Result<ComponentArgs> {
    let mut out = ComponentArgs {
        post_construct: None,
        post_construct_with_ctx: false,
        pre_destroy: None,
        pre_destroy_with_ctx: false,
        destroy_order: 0,
        provides: Vec::new(),
    };
    if args.is_empty() {
        return Ok(out);
    }
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("post_construct") {
            let (method, with_ctx) = parse_hook_value(meta.value()?)?;
            out.post_construct = Some(method);
            out.post_construct_with_ctx = with_ctx;
            Ok(())
        } else if meta.path.is_ident("pre_destroy") {
            let (method, with_ctx) = parse_hook_value(meta.value()?)?;
            out.pre_destroy = Some(method);
            out.pre_destroy_with_ctx = with_ctx;
            Ok(())
        } else if meta.path.is_ident("destroy_order") {
            let value = meta.value()?;
            let negative = value.parse::<Option<syn::Token![-]>>().is_ok_and(|minus| minus.is_some());
            let lit: syn::LitInt = value
                .parse()
                .map_err(|_| meta.error("expected `destroy_order = <integer literal>`, e.g. `destroy_order = -100`"))?;
            // Parsed as `i64`, sign applied, *then* range-checked against
            // `i32` — parsing the magnitude straight into `i32` would
            // reject `i32::MIN` (`-2147483648`): its magnitude,
            // `2147483648`, overflows `i32` even though the signed value
            // itself fits exactly.
            let magnitude: i64 = lit
                .base10_parse()
                .map_err(|_| syn::Error::new_spanned(&lit, "`destroy_order` must fit in an `i32`"))?;
            let signed = if negative { -magnitude } else { magnitude };
            out.destroy_order = i32::try_from(signed)
                .map_err(|_| syn::Error::new_spanned(&lit, "`destroy_order` must fit in an `i32`"))?;
            Ok(())
        } else if meta.path.is_ident("provides") {
            // `provides(A, B)` rather than `provides = A`: a component can
            // implement several traits, and a list reads better than a
            // repeated key.
            meta.parse_nested_meta(|inner| {
                out.provides.push(inner.path.clone());
                Ok(())
            })
            .map_err(|_| meta.error("expected `provides(TraitName)` or `provides(TraitA, TraitB)`"))?;
            if out.provides.is_empty() {
                return Err(meta.error("`provides(...)` needs at least one trait, e.g. `provides(Greeter)`"));
            }
            Ok(())
        } else {
            Err(meta.error(
                "expected `post_construct = <method>`, `pre_destroy = <method>`, `destroy_order = <integer>` \
                 or `provides(<Trait>)`",
            ))
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
    // Context-aware hooks (`method(ctx)`) clone the `&Context` they're
    // called with into an owned value before moving it into the `async
    // move` block: `HookFn`'s future must be `'static` for any input
    // lifetime, so it cannot borrow the `&Context` argument. `Context`'s
    // `Clone` is `Arc`-cheap (see its doc comment in `oxide_core::container`)
    // and this clone lives only in the transient hook future — never stored
    // in a field, which is the one place that clone would be unsound (an
    // `Arc` cycle through the context that owns this very component).
    let maybe_post_construct = args.post_construct.as_ref().map(|method| {
        if args.post_construct_with_ctx {
            quote! {
                .post_construct(|__ctx: &::oxide_boot::__private::Context| {
                    let __me = __ctx.get::<#ident>().expect("component was just constructed");
                    let __ctx = ::core::clone::Clone::clone(__ctx);
                    ::std::boxed::Box::pin(async move { __me.#method(&__ctx).await })
                })
            }
        } else {
            quote! {
                .post_construct(|__ctx: &::oxide_boot::__private::Context| {
                    let __me = __ctx.get::<#ident>().expect("component was just constructed");
                    ::std::boxed::Box::pin(async move { __me.#method().await })
                })
            }
        }
    });
    let maybe_pre_destroy = args.pre_destroy.as_ref().map(|method| {
        if args.pre_destroy_with_ctx {
            quote! {
                .pre_destroy(|__ctx: &::oxide_boot::__private::Context| {
                    let __me = __ctx.get::<#ident>().expect("component exists during shutdown");
                    let __ctx = ::core::clone::Clone::clone(__ctx);
                    ::std::boxed::Box::pin(async move { __me.#method(&__ctx).await })
                })
            }
        } else {
            quote! {
                .pre_destroy(|__ctx: &::oxide_boot::__private::Context| {
                    let __me = __ctx.get::<#ident>().expect("component exists during shutdown");
                    ::std::boxed::Box::pin(async move { __me.#method().await })
                })
            }
        }
    });

    let destroy_order = args.destroy_order;

    // One binding per declared trait. The coercion `Arc<Self> -> Arc<dyn T>`
    // has to happen here, where the concrete type is still known; the
    // container only ever sees the already-coerced handle, erased again so it
    // can live in a `TypeId`-keyed map.
    let trait_bindings = args.provides.iter().map(|path| {
        quote! {
            ::oxide_boot::__private::inventory::submit! {
                ::oxide_boot::__private::TraitBinding::new(
                    stringify!(#path),
                    stringify!(#ident),
                    ::core::any::TypeId::of::<dyn #path>,
                    ::core::any::TypeId::of::<#ident>,
                    |__ctx: &::oxide_boot::__private::Context| {
                        let __concrete = __ctx.get::<#ident>()?;
                        let __as_trait: ::std::sync::Arc<dyn #path> = __concrete.into_arc();
                        ::core::option::Option::Some(
                            ::std::sync::Arc::new(__as_trait)
                                as ::std::sync::Arc<dyn ::core::any::Any + ::core::marker::Send + ::core::marker::Sync>
                        )
                    },
                )
            }
        }
    });

    Ok(quote! {
        #item

        #(#trait_bindings)*

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
            .destroy_order(#destroy_order)
            .build()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn args(input: proc_macro2::TokenStream) -> ComponentArgs {
        parse_args(input).expect("valid arguments")
    }

    fn error(input: proc_macro2::TokenStream) -> String {
        // `ComponentArgs` holds `syn` types that are not `Debug`, so
        // `expect_err` is not available; match instead.
        match parse_args(input) {
            Ok(_) => panic!("expected a rejection"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn no_arguments_means_no_hooks_and_the_default_slot() {
        let a = args(quote! {});
        assert!(a.post_construct.is_none());
        assert!(a.pre_destroy.is_none());
        assert!(a.provides.is_empty());
        // Zero is the ordinary teardown slot; telemetry sits at 100 so it
        // outlives everything that produces spans.
        assert_eq!(a.destroy_order, 0);
    }

    #[test]
    fn a_hook_can_ask_for_the_context() {
        let plain = args(quote! { post_construct = connect });
        assert_eq!(plain.post_construct.as_ref().map(ToString::to_string), Some("connect".to_string()));
        assert!(!plain.post_construct_with_ctx);

        let with_ctx = args(quote! { post_construct = connect(ctx) });
        assert!(with_ctx.post_construct_with_ctx, "the (ctx) form was not recognised");
    }

    #[test]
    fn only_ctx_is_accepted_as_a_hook_argument() {
        // A hook taking anything else would compile into a call the
        // container cannot satisfy; rejecting it here names the problem.
        let message = error(quote! { post_construct = connect(other) });
        assert!(message.contains("ctx"), "was: {message}");
    }

    #[test]
    fn destroy_order_accepts_negative_values() {
        // Negative slots tear down *before* the default, which is how a
        // component can be stopped ahead of its dependencies.
        assert_eq!(args(quote! { destroy_order = -100 }).destroy_order, -100);
        assert_eq!(args(quote! { destroy_order = 100 }).destroy_order, 100);
    }

    #[test]
    fn a_destroy_order_outside_i32_is_rejected() {
        // Silently wrapping would reorder shutdown in a way nobody could
        // trace back to the literal.
        let message = error(quote! { destroy_order = 99999999999999999999 });
        assert!(message.contains("i32"), "was: {message}");
    }

    #[test]
    fn provides_takes_one_or_several_traits() {
        let one = args(quote! { provides(Notifier) });
        assert_eq!(one.provides.len(), 1);

        let several = args(quote! { provides(Notifier, Auditable) });
        assert_eq!(several.provides.len(), 2);
    }

    #[test]
    fn provides_combines_with_the_other_arguments() {
        let a = args(quote! { post_construct = start, provides(Notifier), destroy_order = 10 });
        assert!(a.post_construct.is_some());
        assert_eq!(a.provides.len(), 1);
        assert_eq!(a.destroy_order, 10);
    }

    #[test]
    fn an_unknown_argument_lists_what_is_accepted() {
        let message = error(quote! { on_start = go });
        for expected in ["post_construct", "pre_destroy", "destroy_order", "provides"] {
            assert!(message.contains(expected), "'{expected}' missing from: {message}");
        }
    }
}
