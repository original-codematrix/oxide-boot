//! `#[retryable]` — Spring's `@Retryable`.
//!
//! Rewrites an async method's body into a retry loop. No registration is
//! involved, so unlike `#[kafka_listeners]` or `#[scheduled_tasks]` this
//! needs no attribute on the enclosing impl block: it sits directly on the
//! method.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Expr, ImplItemFn, Lit, Meta};

struct Args {
    max_attempts: u32,
    backoff_ms: u64,
    multiplier: f64,
    max_backoff_ms: u64,
    jitter: f64,
    /// An expression over the bound name `error`, deciding whether to retry.
    retry_if: Option<Expr>,
    /// A method taking the final error and returning the same `Result`.
    recover: Option<syn::Ident>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff_ms: 100,
            multiplier: 2.0,
            max_backoff_ms: 30_000,
            jitter: 0.25,
            retry_if: None,
            recover: None,
        }
    }
}

fn parse_args(args: TokenStream) -> syn::Result<Args> {
    let mut out = Args::default();
    if args.is_empty() {
        return Ok(out);
    }
    let parsed = syn::parse2::<Meta>(quote! { retryable(#args) })?;
    let Meta::List(list) = parsed else {
        return Err(syn::Error::new_spanned(&args, "expected `#[retryable(max_attempts = 3, ...)]`"));
    };
    let nested =
        list.parse_args_with(syn::punctuated::Punctuated::<syn::MetaNameValue, syn::Token![,]>::parse_terminated)?;

    for nv in nested {
        let key = nv.path.get_ident().map(ToString::to_string).unwrap_or_default();
        let Expr::Lit(lit) = &nv.value else {
            return Err(syn::Error::new_spanned(&nv.value, "expected a literal"));
        };
        match (key.as_str(), &lit.lit) {
            ("max_attempts", Lit::Int(v)) => out.max_attempts = v.base10_parse()?,
            ("backoff_ms", Lit::Int(v)) => out.backoff_ms = v.base10_parse()?,
            ("max_backoff_ms", Lit::Int(v)) => out.max_backoff_ms = v.base10_parse()?,
            ("multiplier", Lit::Float(v)) => out.multiplier = v.base10_parse()?,
            ("multiplier", Lit::Int(v)) => out.multiplier = v.base10_parse::<u32>()? as f64,
            ("jitter", Lit::Float(v)) => out.jitter = v.base10_parse()?,
            ("jitter", Lit::Int(v)) => out.jitter = v.base10_parse::<u32>()? as f64,
            // Parsed as an expression at this call site, so a typo is a
            // compile error here rather than inside the expansion.
            ("retry_if", Lit::Str(v)) => out.retry_if = Some(v.parse()?),
            ("recover", Lit::Str(v)) => out.recover = Some(syn::Ident::new(&v.value(), v.span())),
            (other, _) => {
                return Err(syn::Error::new_spanned(
                    &nv,
                    format!(
                        "unknown or mistyped argument `{other}`; expected `max_attempts` (int), `backoff_ms` \
                         (int), `max_backoff_ms` (int), `multiplier` (float), `jitter` (float), `retry_if` \
                         (string expression) or `recover` (string method name)"
                    ),
                ));
            }
        }
    }

    if out.max_attempts == 0 {
        return Err(syn::Error::new_spanned(
            &args,
            "`max_attempts = 0` would never call the method at all; use 1 to disable retrying",
        ));
    }
    Ok(out)
}

pub fn expand(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args = parse_args(args)?;
    let mut method: ImplItemFn = syn::parse2(item)?;

    if method.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[retryable] requires an `async fn`: waiting between attempts must not block a runtime worker",
        ));
    }

    let return_ty = match &method.sig.output {
        syn::ReturnType::Type(_, ty) => (**ty).clone(),
        syn::ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "#[retryable] needs a return type of `Result<T, E>`; there is nothing to retry on otherwise",
            ));
        }
    };

    let body = &method.block;
    let operation = format!("{}", method.sig.ident);
    let Args { max_attempts, backoff_ms, multiplier, max_backoff_ms, jitter, retry_if, recover } = args;

    let should_retry = match &retry_if {
        // The predicate sees the error bound as `error`.
        Some(expr) => quote! { { let error = &__error; #expr } },
        None => quote! { true },
    };

    let on_exhausted = match &recover {
        Some(method) => quote! { return self.#method(__error).await },
        None => quote! { return ::core::result::Result::Err(__error) },
    };

    let wrapped = quote! {
        {
            let __policy = ::oxide_boot::retry::RetryPolicy {
                max_attempts: #max_attempts,
                initial_backoff: ::core::time::Duration::from_millis(#backoff_ms),
                multiplier: #multiplier,
                max_backoff: ::core::time::Duration::from_millis(#max_backoff_ms),
                jitter: #jitter,
            };
            let mut __attempt: u32 = 1;
            loop {
                // The body runs as an inner async block, so `?` and an early
                // `return` inside it yield this block's value and are
                // retried like any other error rather than escaping the loop.
                let __outcome: #return_ty = async #body.await;
                let __error = match __outcome {
                    ::core::result::Result::Ok(__value) => return ::core::result::Result::Ok(__value),
                    ::core::result::Result::Err(__error) => __error,
                };

                let __retry = #should_retry;
                if !__retry || __attempt >= #max_attempts {
                    if __attempt > 1 || !__retry {
                        ::oxide_boot::retry::record_exhausted(#operation, __attempt, &__error);
                    }
                    #on_exhausted;
                }

                __attempt += 1;
                let __delay = __policy.delay_for(__attempt);
                ::oxide_boot::retry::record_retry(#operation, __attempt, __delay, &__error);
                ::oxide_boot::__private::tokio::time::sleep(__delay).await;
            }
        }
    };

    method.block = syn::parse2(wrapped)?;
    Ok(quote! { #method })
}
