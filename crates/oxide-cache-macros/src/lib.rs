//! `#[cacheable]` — Spring's `@Cacheable`, as an attribute on an async
//! method.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Expr, ImplItemFn, Lit, Meta};

/// Caches an async method's successful result.
///
/// ```ignore
/// #[cacheable(cache = "notes", key = "id")]
/// pub async fn find(&self, id: &str) -> Result<Option<Note>, ApiError> { ... }
/// ```
///
/// The method's enclosing type must have a field of type `CacheManager`
/// named `caches`, or the name given by `manager = "..."`. The return type
/// must be `Result<T, E>` where `T: Serialize + DeserializeOwned`.
///
/// # What is cached, and what is not
///
/// Only `Ok` is stored. An `Err` is returned to the caller untouched:
/// caching a failure turns a transient outage into a sticky one for the
/// length of the TTL. A cache backend that itself fails is logged and
/// treated as a miss, so a cache outage costs latency rather than
/// availability.
///
/// # Arguments
///
/// * `cache = "name"` — required; the cache to use, resolved by name.
/// * `key = "expr"` — required; a Rust expression over the method's
///   parameters, e.g. `"id"` or `"format!(\"{tenant}:{id}\")"`.
/// * `manager = "field"` — optional; the field holding the `CacheManager`
///   (default `caches`).
#[proc_macro_attribute]
pub fn cacheable(args: TokenStream, item: TokenStream) -> TokenStream {
    match expand(args.into(), item.into()) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct Args {
    cache: String,
    key: Expr,
    manager: syn::Ident,
}

fn parse_args(args: proc_macro2::TokenStream) -> syn::Result<Args> {
    let parsed = syn::parse2::<Meta>(quote! { cacheable(#args) })?;
    let Meta::List(list) = parsed else {
        return Err(syn::Error::new_spanned(&args, "expected `#[cacheable(cache = \"...\", key = \"...\")]`"));
    };

    let mut cache = None;
    let mut key = None;
    let mut manager = None;

    let nested =
        list.parse_args_with(syn::punctuated::Punctuated::<syn::MetaNameValue, syn::Token![,]>::parse_terminated)?;
    for nv in nested {
        let ident = nv.path.get_ident().map(ToString::to_string).unwrap_or_default();
        let Expr::Lit(lit) = &nv.value else {
            return Err(syn::Error::new_spanned(&nv.value, "expected a string literal"));
        };
        let Lit::Str(s) = &lit.lit else {
            return Err(syn::Error::new_spanned(&lit.lit, "expected a string literal"));
        };
        match ident.as_str() {
            "cache" => cache = Some(s.value()),
            // Parsed as an expression here rather than pasted as a string,
            // so a typo in the key is a compile error at this call site
            // instead of a runtime surprise.
            "key" => key = Some(s.parse::<Expr>()?),
            "manager" => manager = Some(syn::Ident::new(&s.value(), s.span())),
            other => {
                return Err(syn::Error::new_spanned(
                    &nv.path,
                    format!("unknown argument `{other}`; expected `cache`, `key` or `manager`"),
                ));
            }
        }
    }

    Ok(Args {
        cache: cache.ok_or_else(|| syn::Error::new_spanned(&args, "`cache = \"name\"` is required"))?,
        key: key.ok_or_else(|| syn::Error::new_spanned(&args, "`key = \"expr\"` is required"))?,
        manager: manager.unwrap_or_else(|| syn::Ident::new("caches", proc_macro2::Span::call_site())),
    })
}

fn expand(args: proc_macro2::TokenStream, item: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
    let args = parse_args(args)?;
    let mut method: ImplItemFn = syn::parse2(item)?;

    if method.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[cacheable] requires an `async fn`: reading a cache is itself asynchronous, so there is nothing \
             sensible to do on a blocking method",
        ));
    }

    let cache_name = &args.cache;
    let key_expr = &args.key;
    let manager = &args.manager;
    let body = &method.block;
    // The bare type, not `ReturnType`: the latter carries the `->`, which is
    // a syntax error inside the `let` binding below.
    let return_ty = match &method.sig.output {
        syn::ReturnType::Type(_, ty) => (**ty).clone(),
        syn::ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "#[cacheable] needs a return type of `Result<T, E>`; a method returning `()` has nothing to cache",
            ));
        }
    };

    // The original body becomes an inner async block, so `return` inside it
    // still means "return from the method" -- rewriting it as a closure
    // would change that.
    let wrapped = quote! {
        {
            let __cache_key: ::std::string::String = ::std::string::ToString::to_string(&#key_expr);
            let __cache = ::oxide_cache::CacheManager::get(&self.#manager, #cache_name);

            if let ::core::option::Option::Some(__c) = &__cache {
                match ::oxide_cache::get(__c.as_ref(), &__cache_key).await {
                    ::core::result::Result::Ok(::core::option::Option::Some(__hit)) => {
                        ::metrics::counter!("cache_requests_total", "cache" => #cache_name, "result" => "hit")
                            .increment(1);
                        return ::core::result::Result::Ok(__hit);
                    }
                    ::core::result::Result::Ok(::core::option::Option::None) => {}
                    // A cache that errors is a miss, not a failure: the
                    // authoritative source is right below.
                    ::core::result::Result::Err(__e) => {
                        ::tracing::warn!(error = %__e, cache = #cache_name, key = %__cache_key,
                                         "cache read failed; falling through");
                    }
                }
            }
            ::metrics::counter!("cache_requests_total", "cache" => #cache_name, "result" => "miss").increment(1);

            let __computed: #return_ty = async #body.await;

            if let (::core::result::Result::Ok(__value), ::core::option::Option::Some(__c)) = (&__computed, &__cache)
                && let ::core::result::Result::Err(__e) =
                    ::oxide_cache::put(__c.as_ref(), &__cache_key, __value, ::core::option::Option::None).await
            {
                // Failing to store is not failing the call: the caller
                // already has its answer.
                ::tracing::warn!(error = %__e, cache = #cache_name, key = %__cache_key, "cache write failed");
            }

            __computed
        }
    };

    method.block = syn::parse2(wrapped)?;
    Ok(quote! { #method })
}
