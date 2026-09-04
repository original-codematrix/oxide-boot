//! `#[scheduled_tasks]` / `#[scheduled]` — Spring's `@Scheduled`.
//!
//! The outer attribute sits on the impl block for the same reason
//! `#[kafka_listeners]` does: the registration `inventory` needs is a
//! module-level static, and an attribute macro on a method may only produce
//! impl items. The type name is also needed to resolve the component, and a
//! method-level attribute cannot see it.

use proc_macro::TokenStream;
use quote::quote;
use syn::{ImplItem, ItemImpl};

#[proc_macro_attribute]
pub fn scheduled_tasks(_args: TokenStream, item: TokenStream) -> TokenStream {
    match expand(item.into()) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Reserved so `#[scheduled(...)]` has a definition when a reader's editor
/// resolves it; the real work happens in [`scheduled_tasks`], which strips
/// the attribute before the compiler sees it.
#[proc_macro_attribute]
pub fn tasks_of(_args: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Every trigger is carried as text, because each may be a `${key:default}`
/// placeholder resolved at boot. An integer literal is accepted for the
/// interval forms and simply stringified.
enum Trigger {
    Cron(String),
    FixedDelay(String),
    FixedRate(String),
}

/// Accepts `30` or `"${poller.delay:30}"` for an interval.
fn interval_spec(meta: &syn::meta::ParseNestedMeta) -> syn::Result<String> {
    let value = meta.value()?;
    let lookahead = value.lookahead1();
    if lookahead.peek(syn::LitInt) {
        Ok(value.parse::<syn::LitInt>()?.base10_digits().to_string())
    } else if lookahead.peek(syn::LitStr) {
        Ok(value.parse::<syn::LitStr>()?.value())
    } else {
        Err(lookahead.error())
    }
}

fn parse_trigger(attr: &syn::Attribute) -> syn::Result<Trigger> {
    let mut found: Option<Trigger> = None;
    attr.parse_nested_meta(|meta| {
        let set = |slot: &mut Option<Trigger>, value: Trigger, name: &str| -> syn::Result<()> {
            if slot.is_some() {
                return Err(meta
                    .error(format!("a task has exactly one trigger; `{name}` conflicts with the one already given")));
            }
            *slot = Some(value);
            Ok(())
        };
        if meta.path.is_ident("cron") {
            let lit: syn::LitStr = meta.value()?.parse()?;
            set(&mut found, Trigger::Cron(lit.value()), "cron")
        } else if meta.path.is_ident("fixed_delay_seconds") {
            let spec = interval_spec(&meta)?;
            set(&mut found, Trigger::FixedDelay(spec), "fixed_delay_seconds")
        } else if meta.path.is_ident("fixed_rate_seconds") {
            let spec = interval_spec(&meta)?;
            set(&mut found, Trigger::FixedRate(spec), "fixed_rate_seconds")
        } else {
            Err(meta.error("expected `cron = \"...\"`, `fixed_delay_seconds = N` or `fixed_rate_seconds = N`"))
        }
    })?;
    found.ok_or_else(|| {
        syn::Error::new_spanned(
            attr,
            "#[scheduled] needs a trigger: `cron = \"0 0 3 * * *\"`, `fixed_delay_seconds = 30` or \
             `fixed_rate_seconds = 30`",
        )
    })
}

fn expand(item: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
    let mut block: ItemImpl = syn::parse2(item)?;
    let self_ty = block.self_ty.clone();

    let mut registrations = Vec::new();

    for item in &mut block.items {
        let ImplItem::Fn(method) = item else { continue };
        let Some(index) = method.attrs.iter().position(|a| a.path().is_ident("scheduled")) else { continue };
        let attr = method.attrs.remove(index);
        let trigger = parse_trigger(&attr)?;

        if method.sig.asyncness.is_none() {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "#[scheduled] requires an `async fn`: a blocking task would occupy a runtime worker for its whole \
                 duration, which for a nightly job means a worker is gone all night",
            ));
        }

        let method_name = &method.sig.ident;
        let task_name = format!("{}::{}", quote!(#self_ty), method_name);

        let trigger_expr = match trigger {
            Trigger::Cron(expression) => quote! {
                ::oxide_scheduler::Trigger::Cron(::std::string::String::from(#expression))
            },
            Trigger::FixedDelay(spec) => quote! {
                ::oxide_scheduler::Trigger::FixedDelay(::std::string::String::from(#spec))
            },
            Trigger::FixedRate(spec) => quote! {
                ::oxide_scheduler::Trigger::FixedRate(::std::string::String::from(#spec))
            },
        };

        registrations.push(quote! {
            ::oxide_boot::__private::inventory::submit! {
                ::oxide_scheduler::ScheduledTask {
                    name: #task_name,
                    trigger: || #trigger_expr,
                    run: |__ctx: &::oxide_boot::__private::Context| {
                        // Resolved per run, not captured once: the closure
                        // is a `fn` pointer and cannot hold state, and this
                        // also means the task sees a replaced component in
                        // tests.
                        let __me = __ctx.get::<#self_ty>()?;
                        ::core::option::Option::Some(::std::boxed::Box::pin(async move {
                            __me.#method_name().await
                        }))
                    },
                }
            }
        });
    }

    if registrations.is_empty() {
        return Err(syn::Error::new_spanned(
            &block.self_ty,
            "#[scheduled_tasks] found no #[scheduled] methods — either mark them, or drop the attribute",
        ));
    }

    Ok(quote! {
        #block
        #(#registrations)*
    })
}
