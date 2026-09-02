//! Expansion for `#[http_client(base_url = "...")]`: turns a route-annotated
//! trait into a reqwest-based client implementation registered as a component.

use proc_macro2::TokenStream;
use quote::quote;
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

    Ok(quote! {
        /// Concrete client created by `#[http_client]`, backed by reqwest.
        pub struct #client_ident {
            pub __client: ::reqwest::Client,
            pub __base_url: ::std::string::String,
        }

        impl #client_ident {
            #(#method_impls)*
        }

        ::oxide_client::__private::inventory::submit! {
            ::oxide_client::__private::ComponentDescriptor::builder(
                #client_name_lit,
                ::core::any::TypeId::of::<#client_ident>,
                |__builder| {
                    let __config = __builder.resolve::<::oxide_client::__private::AppConfig>()?;
                    let __client = ::reqwest::Client::new();
                    let __base_url: ::std::string::String =
                        ::oxide_client::__private::resolve_base_url(#base_url_lit, &__config);
                    __builder.insert(#client_ident { __client, __base_url });
                    ::core::result::Result::Ok(())
                },
            )
            .deps(|| <[_]>::into_vec(::std::boxed::Box::new([(
                ::core::any::TypeId::of::<::oxide_client::__private::AppConfig>(),
                "AppConfig",
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

    // Unwrap Path/Json wrappers.
    let mut prelude = Vec::new();
    for id in &path_params {
        prelude.push(quote! { let #id = #id.0; });
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
            return Err(syn::Error::new_spanned(id, format!("path parameter `{id}` does not appear in the route path `{sub_path}`")));
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
    let method_sig = sig.ident.to_string();
    let method_lit = LitStr::new(&method_sig, proc_macro2::Span::call_site());

    let send_call = quote! {
        ::oxide_client::__private::send(&self.__client, __req, #client_lit, #metric_name, #method_lit)
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
    if let Type::Path(tp) = ty {
        tp.path.segments.last().map(|s| s.ident.to_string())
    } else {
        None
    }
}