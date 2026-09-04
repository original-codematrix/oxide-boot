//! Expansion for `#[http_client(base_url = "...")]`: turns a route-annotated
//! trait into a reqwest-based client implementation registered as a component.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemTrait, LitStr, Pat, Type};

const ROUTE_ATTRS: [(&str, &str); 5] =
    [("get", "GET"), ("post", "POST"), ("put", "PUT"), ("delete", "DELETE"), ("patch", "PATCH")];

struct HttpClientArgs {
    base_url: String,
}

fn parse_args(attr: TokenStream) -> syn::Result<HttpClientArgs> {
    let meta: syn::Meta = syn::parse2(attr)?;
    match meta {
        syn::Meta::NameValue(nv) if nv.path.is_ident("base_url") => match &nv.value {
            syn::Expr::Lit(el) => match &el.lit {
                syn::Lit::Str(s) => Ok(HttpClientArgs { base_url: s.value() }),
                _ => Err(syn::Error::new_spanned(nv, "base_url must be a string literal")),
            },
            _ => Err(syn::Error::new_spanned(nv, "base_url must be a string literal")),
        },
        other => Err(syn::Error::new_spanned(other, "expected `base_url = \"...\"`")),
    }
}

pub fn expand(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args = parse_args(args)?;
    let base_url = args.base_url;
    let mut trait_def: ItemTrait = syn::parse2(item)?;
    let client_ident = trait_def.ident.clone();
    let client_name = name_to_snake(&client_ident.to_string());

    let mut method_impls: Vec<TokenStream> = Vec::new();
    for it in trait_def.items.iter_mut() {
        let syn::TraitItem::Fn(method) = it else { continue };
        let sig = &method.sig;
        let mut route: Option<(usize, &'static str, String)> = None;
        for (idx, attr) in method.attrs.iter().enumerate() {
            for (name, verb) in ROUTE_ATTRS {
                if attr.path().is_ident(name) {
                    if route.is_some() {
                        return Err(syn::Error::new_spanned(attr, "only one route attribute per method"));
                    }
                    let sub: String = match &attr.meta {
                        syn::Meta::Path(_) => String::new(),
                        _ => attr.parse_args::<LitStr>()?.value(),
                    };
                    route = Some((idx, verb, sub));
                }
            }
        }
        let Some((attr_idx, verb, sub_path)) = route else { continue };
        method.attrs.remove(attr_idx);
        let body = generate_method(sig, verb, &sub_path, &client_name)?;
        method_impls.push(quote! {
            #sig { #body }
        });
    }

    // Register the client as a plain component (same `inventory::submit!` shape
    // as `#[component]`) — no extra registry indirection. `base_url` may be a
    // `${key}` placeholder resolved from config, or a literal URL.
    let base_url_lit = base_url.clone();
    let client_name_lit = client_name.clone();

    // Per-client timeout properties, bound to `oxide.clients.<name>` (the
    // prefix is known at macro-expansion time since `client_name` is
    // derived from the trait's own identifier). `default` seeds the
    // defaults into the config tree so `connect-timeout-seconds` /
    // `request-timeout-seconds` are env-overridable even when the whole
    // `oxide.clients.<name>` section is absent from YAML — see
    // `oxide_core::config::ConfigDefaults`.
    let props_ident = format_ident!("{}Props", client_ident);
    let props_prefix = LitStr::new(&format!("oxide.clients.{client_name}"), proc_macro2::Span::call_site());

    Ok(quote! {
        /// Concrete client created by `#[http_client]`, backed by reqwest.
        pub struct #client_ident {
            pub __client: ::reqwest::Client,
            pub __base_url: ::std::string::String,
            /// Resolved once at construction rather than read per call, so
            /// the request path does no configuration lookup.
            pub __retry: ::oxide_client::ClientRetryProps,
        }

        impl #client_ident {
            #(#method_impls)*
        }

        #[derive(
            ::std::fmt::Debug,
            ::std::clone::Clone,
            ::oxide_client::__private::serde::Deserialize,
            ::oxide_client::__private::serde::Serialize,
            ::oxide_client::__private::ConfigProperties,
        )]
        #[config(prefix = #props_prefix, default)]
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        pub struct #props_ident {
            #[serde(rename = "connect-timeout-seconds", default = "::oxide_client::__private::default_connect_timeout_seconds")]
            pub connect_timeout_seconds: u64,
            #[serde(rename = "request-timeout-seconds", default = "::oxide_client::__private::default_request_timeout_seconds")]
            pub request_timeout_seconds: u64,
            /// Retry policy, off unless configured. See
            /// `oxide_client::ClientRetryProps` for why it is not on by
            /// default.
            #[serde(default)]
            pub retry: ::oxide_client::ClientRetryProps,
        }

        impl ::core::default::Default for #props_ident {
            fn default() -> Self {
                Self {
                    connect_timeout_seconds: ::oxide_client::__private::default_connect_timeout_seconds(),
                    request_timeout_seconds: ::oxide_client::__private::default_request_timeout_seconds(),
                    retry: ::core::default::Default::default(),
                }
            }
        }

        ::oxide_client::__private::inventory::submit! {
            ::oxide_client::__private::ComponentDescriptor::builder(
                #client_name_lit,
                ::core::any::TypeId::of::<#client_ident>,
                |__builder| {
                    let __config = __builder.resolve::<::oxide_client::__private::AppConfig>()?;
                    let __props = __builder.resolve::<#props_ident>()?;
                    let __base_url: ::std::string::String =
                        ::oxide_client::__private::resolve_base_url(#base_url_lit, &__config).map_err(|e| {
                            ::oxide_client::__private::ContextError::Construct {
                                type_name: #client_name_lit,
                                message: ::std::string::ToString::to_string(&e),
                            }
                        })?;
                    let __client = ::reqwest::Client::builder()
                        .connect_timeout(::std::time::Duration::from_secs(__props.connect_timeout_seconds))
                        .timeout(::std::time::Duration::from_secs(__props.request_timeout_seconds))
                        .build()
                        .map_err(|e| ::oxide_client::__private::ContextError::Construct {
                            type_name: #client_name_lit,
                            message: ::std::string::ToString::to_string(&e),
                        })?;
                    __builder.insert(#client_ident { __client, __base_url, __retry: __props.retry.clone() });
                    ::core::result::Result::Ok(())
                },
            )
            .deps(|| <[_]>::into_vec(::std::boxed::Box::new([(
                ::core::any::TypeId::of::<::oxide_client::__private::AppConfig>(),
                "AppConfig",
            ), (
                ::core::any::TypeId::of::<#props_ident>(),
                "http-client properties",
            )])))
            .build()
        }
    })
}

fn name_to_snake(s: &str) -> String {
    let mut out = String::new();
    let mut prev_upper = false;
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 && !prev_upper {
                out.push('_');
            }
            prev_upper = true;
            out.extend(c.to_lowercase());
        } else {
            prev_upper = false;
            out.push(c);
        }
    }
    let stripped = out.strip_suffix("client").unwrap_or(&out).to_string();
    let stripped = stripped.strip_suffix('_').unwrap_or(&stripped).to_string();
    if stripped.is_empty() { "client".to_string() } else { stripped }
}

fn generate_method(sig: &syn::Signature, verb: &str, sub_path: &str, client_name: &str) -> syn::Result<TokenStream> {
    // Collect params: path params (`Path<T>`) and body (`Json<T>`), plus plain
    // scalar params that become query strings.
    let mut path_params: Vec<syn::Ident> = Vec::new();
    let mut body_param: Option<syn::Ident> = None;
    let mut plain_params: Vec<syn::Ident> = Vec::new();

    for arg in sig.inputs.iter().skip(1) {
        let FnArg::Typed(pat_ty) = arg else { continue };
        let Pat::Ident(pid) = &*pat_ty.pat else {
            return Err(syn::Error::new_spanned(&pat_ty.pat, "client method parameters must be plain `name: Type`"));
        };
        let kind = type_last(&pat_ty.ty);
        match kind.as_deref() {
            Some("Path") => path_params.push(pid.ident.clone()),
            Some("Json") | Some("Body") => body_param = Some(pid.ident.clone()),
            _ => plain_params.push(pid.ident.clone()),
        }
    }

    let method_str = syn::Ident::new(verb, proc_macro2::Span::call_site());

    // Unwrap Path/Json wrappers. Path values are percent-encoded per segment
    // so a value containing `/`, `..` or `?` cannot change the route or
    // inject a query string — it stays a literal, encoded segment.
    let mut prelude = Vec::new();
    for id in &path_params {
        prelude.push(quote! {
            let #id = ::oxide_client::__private::encode_path_segment(&::std::format!("{}", #id.0));
        });
    }
    if let Some(id) = &body_param {
        prelude.push(quote! { let #id = #id.0; });
    }
    let _ = &path_params;

    // Build the URL format string with {name} placeholders replaced by {}.
    let mut fmt_str = sub_path.to_string();
    let mut fmt_args: Vec<syn::Ident> = Vec::new();
    // Replace all {name} tokens with the actual path params by regex-free scan.
    let path_ids: Vec<syn::Ident> = path_params.clone();
    for id in &path_ids {
        let key = id.to_string();
        if fmt_str.contains(&format!("{{{key}}}")) {
            fmt_str = fmt_str.replace(&format!("{{{key}}}"), "{}");
            fmt_args.push(id.clone());
        }
    }
    for id in &path_ids {
        if !fmt_args.contains(id) {
            // path param not referenced in URL — treat as error
            return Err(syn::Error::new_spanned(
                id,
                format!("path parameter `{id}` does not appear in the route path `{sub_path}`"),
            ));
        }
    }
    let fmt_lit = LitStr::new(&fmt_str, proc_macro2::Span::call_site());

    // Query params from any plain (non-path, non-body) params.
    let query_send = if plain_params.is_empty() {
        quote! {}
    } else {
        let pairs: Vec<TokenStream> = plain_params
            .iter()
            .map(|id| {
                let key = LitStr::new(&id.to_string(), proc_macro2::Span::call_site());
                quote! { (#key, &#id) }
            })
            .collect();
        quote! { __req = __req.query(&[#(#pairs),*]); }
    };

    let body_send = if let Some(b) = &body_param {
        quote! { __req = __req.json(&#b); }
    } else {
        quote! {}
    };

    let builder = if verb == "GET" {
        quote! { self.__client.get(&__url) }
    } else {
        quote! { self.__client.request(::reqwest::Method::#method_str, &__url) }
    };

    let metric_name = LitStr::new(verb, proc_macro2::Span::call_site());
    let client_lit = LitStr::new(client_name, proc_macro2::Span::call_site());
    // The `uri` metric label carries the route template known at compile
    // time (e.g. `/invoices/{id}`), not the concrete interpolated path —
    // otherwise cardinality is unbounded across distinct path-param values.
    let route_template_lit = LitStr::new(sub_path, proc_macro2::Span::call_site());

    let send_call = quote! {
        ::oxide_client::__private::send_with_retry(
            &self.__client, __req, #client_lit, #metric_name, #route_template_lit, &self.__retry)
    };

    // Parse the return type: `Result<T, _>` → deserialize the body as `T`;
    // a non-Result return (e.g. the raw `reqwest::Response`) is readonly.
    let ok_ty = returned_ok_type(&sig.output);
    let final_expr = match ok_ty {
        Some(ty) => quote! {
            let __resp = #send_call.await?;
            __resp.json::<#ty>().await.map_err(|e| ::oxide_client::ClientError::Json(e.to_string()))
        },
        None => quote! { #send_call.await },
    };

    Ok(quote! {
        #(#prelude)*
        let __url = ::std::format!("{}{}", self.__base_url, ::std::format!(#fmt_lit #(, #fmt_args)*));
        let mut __req = #builder;
        #body_send
        #query_send
        #final_expr
    })
}

/// Extracts the `T` in `-> Result<T, E>`, or `None` for non-Result returns.
fn returned_ok_type(out: &syn::ReturnType) -> Option<syn::Type> {
    let syn::ReturnType::Type(_, ty) = out else { return None };
    let path = match &**ty {
        Type::Path(tp) => tp,
        _ => return None,
    };
    let last = path.path.segments.last()?;
    if last.ident != "Result" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else { return None };
    let first = args.args.first()?;
    match first {
        syn::GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    }
}

fn type_last(ty: &Type) -> Option<String> {
    if let Type::Path(tp) = ty { tp.path.segments.last().map(|s| s.ident.to_string()) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `name_to_snake` derives the config prefix (`oxide.clients.<name>`) and
    // the `client` metric label from the trait's identifier. Getting it
    // wrong means configuration silently does not apply and dashboards group
    // by the wrong key -- neither of which fails loudly.

    #[test]
    fn camel_case_becomes_snake_case() {
        assert_eq!(name_to_snake("NotesClient"), "notes");
        assert_eq!(name_to_snake("BillingServiceClient"), "billing_service");
    }

    #[test]
    fn the_client_suffix_is_dropped_with_its_separator() {
        // `notes_client` would make every config key read
        // `oxide.clients.notes_client.*`, which nobody would write.
        assert_eq!(name_to_snake("NotesClient"), "notes");
        assert_eq!(name_to_snake("Notes"), "notes", "a trait without the suffix keeps its name");
    }

    #[test]
    fn a_trait_called_client_keeps_a_usable_name() {
        // Stripping the suffix would leave an empty prefix, and
        // `oxide.clients..connect-timeout-seconds` binds to nothing.
        assert_eq!(name_to_snake("Client"), "client");
    }

    #[test]
    fn consecutive_capitals_are_not_split_between_every_letter() {
        // `HTTPClient` must not become `h_t_t_p_client`.
        assert_eq!(name_to_snake("HTTPClient"), "httpclient".strip_suffix("client").unwrap_or("httpclient"));
    }

    #[test]
    fn a_lowercase_name_passes_through() {
        assert_eq!(name_to_snake("notes"), "notes");
    }
}
