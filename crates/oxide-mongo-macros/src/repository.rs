//! Expansion for `#[mongo_repository(document = T)]`.
//!
//! Turns a trait whose methods are CRUD names / derived queries / `#[query]`
//! escape hatches into an implementation backed by a `mongodb::Collection<T>`.
//! The macro's job is limited to parsing the method names into
//! `(field, operator)` pairs (known at compile time) and routing method
//! invocations to `::oxide_mongo::__private` runtime helpers; the actual
//! filtering happens at runtime.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, FnArg, Ident, ItemTrait, Result, Signature, TypePath};

struct RepositoryArgs {
    document: TypePath,
}

fn parse_repository_args(attr: proc_macro2::TokenStream) -> Result<RepositoryArgs> {
    use quote::ToTokens;
    let meta: syn::Meta = syn::parse2(attr)?;
    match meta {
        syn::Meta::NameValue(nv) if nv.path.is_ident("document") => {
            let document: TypePath = syn::parse2(nv.value.to_token_stream())?;
            Ok(RepositoryArgs { document })
        }
        other => Err(syn::Error::new_spanned(other, "expected `document = TypeName`")),
    }
}

pub fn expand(args: proc_macro2::TokenStream, item: proc_macro2::TokenStream) -> Result<TokenStream> {
    let args = parse_repository_args(args)?;
    let doc_ty = &args.document;
    let mut trait_def: ItemTrait = syn::parse2(item)?;
    let repo_name = trait_def.ident.clone();
    let impl_ident = Ident::new(&format!("{repo_name}Impl"), proc_macro2::Span::call_site());

    let mut methods: Vec<TokenStream> = Vec::new();
    for it in trait_def.items.iter_mut() {
        let syn::TraitItem::Fn(method) = it else { continue };
        let sig = &method.sig;
        let has_query = method.attrs.iter().any(|a| a.path().is_ident("query"));
        let body = if has_query {
            expand_query_method(&method.attrs, sig)?
        } else {
            expand_derived_or_crud(&sig.ident.to_string(), sig)?
        };
        methods.push(quote! {
            #sig
            {
                #body
            }
        });
        // Strip macro-managed helper attributes so the re-emitted trait is valid.
        method.attrs.retain(|a| !a.path().is_ident("query"));
    }

    Ok(quote! {
        #trait_def

        /// Concrete repository implementation, backed by a Mongo collection.
        pub struct #impl_ident {
            pub(crate) collection: mongodb::Collection<#doc_ty>,
        }

        impl #impl_ident {
            /// Obtains the collection for `T` from the context's `MongoClient`.
            pub(crate) fn new(collection: mongodb::Collection<#doc_ty>) -> Self {
                Self { collection }
            }
            #(#methods)*
        }
    })
}

fn arg_idents(sig: &Signature) -> Vec<syn::Ident> {
    sig.inputs
        .iter()
        .filter_map(|a| if let FnArg::Typed(pt) = a {
            if let syn::Pat::Ident(id) = &*pt.pat { Some(id.ident.clone()) } else { None }
        } else { None })
        .collect()
}

fn expand_derived_or_crud(method_name: &str, sig: &Signature) -> Result<TokenStream> {
    let args = arg_idents(sig);

    // CRUD helpers (id is `_id`).
    match method_name {
        "find_by_id" => {
            let id = &args[0];
            return ok(quote! { ::oxide_mongo::__private::find_by_id(&self.collection, ::oxide_mongo::__private::to_bson(&#id).map_err(::oxide_mongo::Error::Bson)?).await });
        }
        "find_all" => return ok(quote! { ::oxide_mongo::__private::find_all(&self.collection, ::oxide_mongo::__private::doc!{}).await }),
        "count" | "count_all" => return ok(quote! { ::oxide_mongo::__private::count(&self.collection, ::oxide_mongo::__private::doc!{}).await }),
        "delete_by_id" => {
            let id = &args[0];
            return ok(quote! { ::oxide_mongo::__private::delete_one(&self.collection, ::oxide_mongo::__private::doc!{ "_id": ::oxide_mongo::__private::to_bson(&#id).map_err(::oxide_mongo::Error::Bson)? }).await });
        }
        "save" => { let d = &args[0]; return ok(quote! { ::oxide_mongo::__private::save(&self.collection, #d).await }); }
        "insert" => { let d = &args[0]; return ok(quote! { ::oxide_mongo::__private::insert(&self.collection, #d).await }); }
        _ => {}
    }

    let Some((verb, cond_rest)) = derive_verb(method_name) else {
        return Err(syn::Error::new_spanned(
            &sig.ident,
            format!(
                "unsupported repository method '{method_name}': expected a supported verb (find_by/find_all_by/count_by/exists_by/delete_by) + field conditions, a CRUD name, or #[query(...)]"
            ),
        ));
    };

    let conditions = parse_conditions(&cond_rest, sig)?;
    let pairs: Vec<TokenStream> = conditions
        .iter()
        .map(|(f, op)| quote! { (#f, #op) })
        .collect();
    let bson_args: Vec<TokenStream> =
        args.iter().map(|a| quote! { ::oxide_mongo::__private::to_bson(&#a).map_err(::oxide_mongo::Error::Bson)? }).collect();

    match verb {
        "find" | "find_all" => ok(quote! {
            ::oxide_mongo::__private::find_all(&self.collection,
                ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*])).await
        }),
        "count" => ok(quote! {
            ::oxide_mongo::__private::count(&self.collection,
                ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*])).await
        }),
        "exists" => ok(quote! {
            ::oxide_mongo::__private::exists(&self.collection,
                ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*])).await
        }),
        "delete" => ok(quote! {
            ::oxide_mongo::__private::delete_many(&self.collection,
                ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*])).await
        }),
        _ => Err(syn::Error::new_spanned(&sig.ident, "unsupported derived verb")),
    }
}

fn ok(ts: TokenStream) -> Result<TokenStream> {
    Ok(ts)
}

fn derive_verb(name: &str) -> Option<(&'static str, String)> {
    if let Some(rest) = name.strip_prefix("find_all_by_") {
        return Some(("find_all", rest.to_string()));
    }
    if let Some(rest) = name.strip_prefix("find_by_") {
        return Some(("find", rest.to_string()));
    }
    if let Some(rest) = name.strip_prefix("count_by_") {
        return Some(("count", rest.to_string()));
    }
    if let Some(rest) = name.strip_prefix("exists_by_") {
        return Some(("exists", rest.to_string()));
    }
    if let Some(rest) = name.strip_prefix("delete_by_") {
        return Some(("delete", rest.to_string()));
    }
    None
}

/// Parses the condition part of a derived method into `(field_literal, op_literal)`
/// pairs (the field/op are known at compile time from the method name).
fn parse_conditions(cond_part: &str, sig: &Signature) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for seg in cond_part.split("_and_") {
        let (field, op) = parse_field_and_op(seg);
        out.push((field, op));
    }
    // Basic sanity: field names must be non-empty.
    if out.iter().any(|(f, _)| f.is_empty()) {
        return Err(syn::Error::new_spanned(&sig.ident, "derived query has an empty field name"));
    }
    Ok(out)
}

fn parse_field_and_op(field: &str) -> (String, String) {
    const OPS: [(&str, &str); 9] = [
        ("_greater_than_equal", "$gte"),
        ("_less_than_equal", "$lte"),
        ("_greater_than", "$gt"),
        ("_less_than", "$lt"),
        ("_is_not_null", "$ne"),
        ("_is_null", "$exists"),
        ("_in", "$in"),
        ("_not", "$ne"),
        ("_like", "$regex"),
    ];
    for (suffix, mongo_op) in OPS {
        if let Some(f) = field.strip_suffix(suffix) {
            return (f.to_string(), mongo_op.to_string());
        }
    }
    (field.to_string(), "$eq".to_string())
}

fn expand_query_method(attrs: &[Attribute], sig: &Signature) -> Result<TokenStream> {
    let filter_attr = attrs
        .iter()
        .find(|a| a.path().is_ident("query"))
        .ok_or_else(|| syn::Error::new_spanned(&sig.ident, "#[query] missing filter attribute"))?;
    let bad = || syn::Error::new_spanned(&sig.ident, "#[query] expects `filter = \"...\"` with `?1`/`?2` placeholders");
    let filter_str: syn::LitStr = if let syn::Meta::List(list) = &filter_attr.meta {
        let mut found: Option<syn::LitStr> = None;
        list.parse_nested_meta(|meta| {
            if meta.path.is_ident("filter") {
                let v: syn::Expr = meta.value()?.parse()?;
                if let syn::Expr::Lit(el) = v {
                    if let syn::Lit::Str(s) = el.lit {
                        found = Some(s);
                    }
                }
            }
            Ok(())
        })
        .map_err(|_| bad())?;
        found.ok_or_else(bad)?
    } else {
        return Err(bad());
    };
    let filter_lit = &filter_str;

    let args = arg_idents(sig);
    let bson_args: Vec<TokenStream> = args
        .iter()
        .map(|a| quote! { ::oxide_mongo::__private::to_bson(&#a).map_err(::oxide_mongo::Error::Bson)? })
        .collect();

    let name = &sig.ident.to_string();
    let driver = if name.starts_with("count") {
        quote! { ::oxide_mongo::__private::count(&self.collection, filter).await }
    } else if name.starts_with("delete") {
        quote! { ::oxide_mongo::__private::delete_many(&self.collection, filter).await }
    } else {
        quote! { ::oxide_mongo::__private::find_all(&self.collection, filter).await }
    };

    Ok(quote! {
        let filter = ::oxide_mongo::__private::raw_filter(#filter_lit, vec![#(#bson_args),*])?;
        #driver
    })
}