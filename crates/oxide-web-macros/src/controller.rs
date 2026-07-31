//! Expansion for `#[controller]`: turns an `impl` block's route-annotated
//! methods into `RouteDescriptor` registrations collected by `inventory`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{FnArg, ImplItem, ItemImpl, LitStr, Pat};

const ROUTE_ATTRS: [(&str, &str); 5] =
    [("get", "GET"), ("post", "POST"), ("put", "PUT"), ("delete", "DELETE"), ("patch", "PATCH")];

/// Joins a controller base path with a route sub-path, normalizing slashes.
/// Pure and unit-tested here since it has no dependency on macro machinery.
pub fn join_paths(base: &str, path: &str) -> String {
    let joined = format!("{}/{}", base.trim_end_matches('/'), path.trim_start_matches('/'));
    let joined = joined.trim_end_matches('/').to_string();
    if joined.is_empty() {
        "/".to_string()
    } else {
        joined
    }
}

pub fn expand(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let base_path = if args.is_empty() { String::new() } else { syn::parse2::<LitStr>(args)?.value() };
    let mut item: ItemImpl = syn::parse2(item)
        .map_err(|e| syn::Error::new(e.span(), "#[controller] must be applied to an impl block"))?;

    // Guard: reject generic impl blocks
    if !item.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(&item.generics, "#[controller] does not support generic impl blocks"));
    }

    // Guard: reject trait impl blocks
    if let Some((_, trait_path, _)) = &item.trait_ {
        return Err(syn::Error::new_spanned(trait_path, "#[controller] must be applied to an inherent impl, not a trait impl"));
    }

    let self_ty = item.self_ty.clone();
    let mut registrations = Vec::new();

    for impl_item in item.items.iter_mut() {
        let ImplItem::Fn(method) = impl_item else { continue };

        // Find exactly one route attribute (#[get]/#[post]/#[put]/#[delete]/#[patch]).
        let mut route: Option<(usize, &'static str, String)> = None;
        for (idx, attr) in method.attrs.iter().enumerate() {
            for (name, verb) in ROUTE_ATTRS {
                if attr.path().is_ident(name) {
                    if route.is_some() {
                        return Err(syn::Error::new_spanned(attr, "only one route attribute per method"));
                    }
                    let sub_path: String = match &attr.meta {
                        syn::Meta::Path(_) => String::new(),
                        _ => attr.parse_args::<LitStr>()?.value(),
                    };
                    route = Some((idx, verb, sub_path));
                }
            }
        }
        let Some((attr_idx, verb, sub_path)) = route else { continue };
        method.attrs.remove(attr_idx);

        // Route methods must take `&self` so `skip(1)` below is safe: without
        // this check a method missing `&self` (or taking `self`/`&mut self`)
        // would silently drop its first real parameter instead of erroring.
        let is_shared_self_receiver = method
            .sig
            .receiver()
            .is_some_and(|r| r.reference.is_some() && r.mutability.is_none());
        if !is_shared_self_receiver {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "controller route methods must take &self as their first parameter",
            ));
        }

        // Collect plain `ident: Type` params (skip &self).
        let mut params = Vec::new();
        for arg in method.sig.inputs.iter().skip(1) {
            let FnArg::Typed(pat_ty) = arg else { continue };
            let Pat::Ident(ident) = &*pat_ty.pat else {
                return Err(syn::Error::new_spanned(
                    &pat_ty.pat,
                    "controller method parameters must be plain `name: Type` bindings",
                ));
            };
            params.push((ident.ident.clone(), (*pat_ty.ty).clone()));
        }

        let full_path = join_paths(&base_path, &sub_path);
        let method_ident = method.sig.ident.clone();
        let axum_verb = syn::Ident::new(&verb.to_lowercase(), proc_macro2::Span::call_site());
        let param_names: Vec<_> = params.iter().map(|(n, _)| n).collect();
        let param_decls: Vec<_> = params.iter().map(|(n, t)| quote! { #n: #t }).collect();

        registrations.push(quote! {
            ::oxide_web::__private::inventory::submit! {
                ::oxide_web::RouteDescriptor {
                    method: #verb,
                    path: #full_path,
                    register: |__ctx, __router| {
                        let __this = __ctx.get::<#self_ty>().ok_or(
                            ::oxide_web::WebError::ControllerNotRegistered {
                                type_name: stringify!(#self_ty),
                                path: #full_path,
                            },
                        )?;
                        ::core::result::Result::Ok(__router.route(
                            #full_path,
                            ::oxide_web::__private::axum::routing::#axum_verb(
                                move |#(#param_decls),*| {
                                    let __this = __this.clone();
                                    async move { __this.#method_ident(#(#param_names),*).await }
                                },
                            ),
                        ))
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

#[cfg(test)]
mod tests {
    use super::join_paths;

    #[test]
    fn joins_base_and_subpath() {
        assert_eq!(join_paths("/api/users", "/{id}"), "/api/users/{id}");
        assert_eq!(join_paths("/api/users", ""), "/api/users");
        assert_eq!(join_paths("/api/users/", "/{id}"), "/api/users/{id}");
        assert_eq!(join_paths("", ""), "/");
    }
}
