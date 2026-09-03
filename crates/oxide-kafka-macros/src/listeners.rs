//! Expansion for `#[kafka_listeners]`: turns an impl block's
//! `#[kafka_listener(topic = "...", group_id = "...")]`-annotated methods into
//! `ListenerDescriptor` registrations collected by `inventory`.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{FnArg, ImplItem, ItemImpl, LitStr, Pat, Type};

pub fn expand(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    if !args.is_empty() {
        return Err(syn::Error::new_spanned(
            args,
            "#[kafka_listeners] does not take arguments (use #[kafka_listener(topic = \"...\")] on methods)",
        ));
    }
    let mut item: ItemImpl = syn::parse2(item)
        .map_err(|e| syn::Error::new(e.span(), "#[kafka_listeners] must be applied to an impl block"))?;

    // Guard: reject generic impl blocks (same rule as #[controller]).
    if !item.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(&item.generics, "#[kafka_listeners] does not support generic impl blocks"));
    }
    if let Some((_, trait_path, _)) = &item.trait_ {
        return Err(syn::Error::new_spanned(
            trait_path,
            "#[kafka_listeners] must be applied to an inherent impl, not a trait impl",
        ));
    }

    let self_ty = item.self_ty.clone();
    let mut registrations = Vec::new();

    for impl_item in item.items.iter_mut() {
        let ImplItem::Fn(method) = impl_item else { continue };

        // Find exactly one #[kafka_listener(topic = "...", group_id = "...")].
        let mut listener_attr: Option<usize> = None;
        let mut topic: Option<String> = None;
        let mut group_id: Option<String> = None;
        for (idx, attr) in method.attrs.iter().enumerate() {
            if !attr.path().is_ident("kafka_listener") {
                continue;
            }
            if listener_attr.is_some() {
                return Err(syn::Error::new_spanned(attr, "only one #[kafka_listener] attribute per method"));
            }
            listener_attr = Some(idx);
            match &attr.meta {
                syn::Meta::Path(_) => {
                    // `#[kafka_listener]` with no args: topic stays None, the
                    // listener_missing_topic guard below reports it clearly.
                }
                _ => {
                    attr.parse_nested_meta(|meta| {
                        if meta.path.is_ident("topic") {
                            let v: LitStr = meta.value()?.parse()?;
                            topic = Some(v.value());
                            Ok(())
                        } else if meta.path.is_ident("group_id") {
                            let v: LitStr = meta.value()?.parse()?;
                            group_id = Some(v.value());
                            Ok(())
                        } else {
                            Err(meta.error("expected `topic = \"...\"` or `group_id = \"...\"`"))
                        }
                    })?;
                }
            }
        }
        let Some(attr_idx) = listener_attr else { continue };
        let Some(topic) = topic else {
            return Err(syn::Error::new_spanned(
                &method.sig.ident,
                "#[kafka_listener] requires a `topic = \"...\"` argument",
            ));
        };
        method.attrs.remove(attr_idx);

        // Listener methods must take &self as their first parameter.
        let is_shared_self = method.sig.receiver().is_some_and(|r| r.reference.is_some() && r.mutability.is_none());
        if !is_shared_self {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "kafka listener methods must take &self as their first parameter",
            ));
        }

        // First non-self param is the payload type; the rest must be plain
        // `ident: MessageKey` / `ident: MessageHeaders` bindings.
        let mut params = Vec::new();
        for arg in method.sig.inputs.iter().skip(1) {
            let FnArg::Typed(pat_ty) = arg else { continue };
            let Pat::Ident(pat) = &*pat_ty.pat else {
                return Err(syn::Error::new_spanned(
                    &pat_ty.pat,
                    "kafka listener parameters must be plain `name: Type` bindings",
                ));
            };
            params.push((pat.ident.clone(), (*pat_ty.ty).clone()));
        }
        if params.is_empty() {
            return Err(syn::Error::new_spanned(
                &method.sig.ident,
                "a kafka listener must declare a payload parameter (the JSON-deserialized message type)",
            ));
        }
        let (payload_ident, payload_ty) = params.remove(0);

        // Classify the optional extra params by type-name trailing segment.
        let mut key_ident: Option<syn::Ident> = None;
        let mut headers_ident: Option<syn::Ident> = None;
        for (ident, ty) in &params {
            let is_named = |name: &str| -> bool {
                if let Type::Path(tp) = ty { tp.path.segments.last().is_some_and(|s| s.ident == name) } else { false }
            };
            if is_named("MessageKey") {
                key_ident = Some(ident.clone());
            } else if is_named("MessageHeaders") {
                headers_ident = Some(ident.clone());
            } else {
                return Err(syn::Error::new_spanned(
                    ty,
                    "kafka listener extra parameters must be `MessageKey` or `MessageHeaders`",
                ));
            }
        }

        let method_ident = method.sig.ident.clone();
        let listener_name = format!("{}::{}", self_ty_path_str(&self_ty), method_ident);
        let payload_destruct = &payload_ident;
        let group_lit = match &group_id {
            Some(g) => quote! { ::core::option::Option::Some(#g) },
            None => quote! { ::core::option::Option::None },
        };

        // Build the calls to the listener method with the optional key/headers
        // decoded from the ReceivedMessage.
        let key_arg = match &key_ident {
            Some(k) => quote! {
                let #k = ::oxide_kafka::MessageKey(__msg.key.and_then(|k| ::std::string::String::from_utf8(k).ok()));
            },
            None => quote! {},
        };
        let headers_arg = match &headers_ident {
            Some(h) => quote! {
                let #h = ::oxide_kafka::MessageHeaders(__msg.headers.clone());
            },
            None => quote! {},
        };
        let call_args = params.iter().map(|(ident, _)| ident.clone()).collect::<Vec<_>>();

        registrations.push(quote! {
            ::oxide_kafka::__private::inventory::submit! {
                ::oxide_kafka::ListenerDescriptor {
                    topic: #topic,
                    group_id: #group_lit,
                    listener_name: #listener_name,
                    start: |__ctx: &::oxide_kafka::__private::Context,
                            __infra: &::oxide_kafka::ListenerInfra| {
                        let _ = __infra;
                        let __this = __ctx.get::<#self_ty>().ok_or_else(|| {
                            ::oxide_kafka::KafkaError::Other(
                                ::std::format!("listener component not registered: {}", stringify!(#self_ty)),
                            )
                        })?;
                        ::std::result::Result::Ok(
                            ::oxide_kafka::__private::Box::new(move |__msg: ::oxide_kafka::ReceivedMessage| {
                                let __this = __this.clone();
                                ::oxide_kafka::__private::Box::pin(async move {
                                    let #payload_destruct: #payload_ty =
                                        ::oxide_kafka::__private::Deserialize::deserialize(
                                            &mut ::oxide_kafka::__private::Deserializer::from_slice(&__msg.payload),
                                        )
                                        // A malformed payload is a poison
                                        // pill: it will never deserialize no
                                        // matter how many times it's
                                        // retried, so mark it as such —
                                        // `run_with_retry` skips the retry
                                        // loop entirely for this marker and
                                        // goes straight to the DLT.
                                        .map_err(|e| ::oxide_kafka::__private::mark_poison_pill(::std::format!("deserialize: {e}")))?;
                                    #key_arg
                                    #headers_arg
                                    __this.#method_ident(#payload_destruct #(, #call_args)*)
                                        .await
                                        .map_err(|e| {
                                            ::oxide_kafka::__private::HandlerError::Handler(::std::format!("{e}"))
                                        })
                                })
                            })
                        )
                    },
                }
            }
        });
    }

    Ok(quote! {
        #item
        #(#registrations)*
    })
}

/// Renders the self type as a string for the `listener_name` field.
fn self_ty_path_str(ty: &syn::Type) -> String {
    if let Type::Path(tp) = ty {
        tp.path.segments.iter().map(|s| s.ident.to_string()).collect::<Vec<_>>().join("::")
    } else {
        ty.to_token_stream().to_string()
    }
}
