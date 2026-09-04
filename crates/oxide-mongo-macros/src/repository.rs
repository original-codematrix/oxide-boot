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
use syn::{Attribute, FnArg, GenericArgument, Ident, ItemTrait, PathArguments, Result, Signature, Type, TypePath};

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

    let impl_name_lit = impl_ident.to_string();
    let collection_unset_msg =
        format!("{impl_ident}'s collection is resolved in post_construct, after MongoClient connects");

    Ok(quote! {
        #trait_def

        /// Concrete repository implementation, backed by a Mongo collection.
        ///
        /// The collection handle is filled in `post_construct` rather than
        /// `construct`: building it needs the connected `mongodb::Client`
        /// from `MongoClient`, and that connection is itself established
        /// asynchronously in `MongoClient`'s own `post_construct` — which,
        /// by dependency order, runs before this repository's. `construct`
        /// closures across the whole context run before any
        /// `post_construct` hook, so the collection cannot be resolved that
        /// early.
        pub struct #impl_ident {
            pub(crate) collection: ::std::sync::OnceLock<mongodb::Collection<#doc_ty>>,
        }

        impl #impl_ident {
            pub(crate) fn new() -> Self {
                Self { collection: ::std::sync::OnceLock::new() }
            }

            fn collection(&self) -> &mongodb::Collection<#doc_ty> {
                self.collection.get().expect(#collection_unset_msg)
            }

            #(#methods)*
        }

        // Registers the repository as an injectable component — the same
        // `inventory::submit!` shape `#[http_client]` uses — so it can be
        // resolved from the `Context` instead of hand-constructed. It
        // depends on `MongoClient` (for the driver connection) and derives
        // its collection name from `#doc_ty`'s `DocumentMeta` impl.
        ::oxide_mongo::__private::inventory::submit! {
            ::oxide_mongo::__private::ComponentDescriptor::builder(
                #impl_name_lit,
                ::core::any::TypeId::of::<#impl_ident>,
                |__builder| {
                    // Fails fast (consistent with `deps()` below) if
                    // `MongoClient` isn't registered; the connected
                    // collection itself is resolved in `post_construct`.
                    let _ = __builder.resolve::<::oxide_mongo::MongoClient>()?;
                    __builder.insert(#impl_ident::new());
                    ::core::result::Result::Ok(())
                },
            )
            .deps(|| <[_]>::into_vec(::std::boxed::Box::new([(
                ::core::any::TypeId::of::<::oxide_mongo::MongoClient>(),
                "MongoClient",
            )])))
            .post_construct(|__ctx: &::oxide_mongo::__private::Context| {
                let __repo = __ctx.get::<#impl_ident>().expect(concat!(
                    stringify!(#impl_ident),
                    " exists (constructed above, in the same assemble() pass)"
                ));
                let __client = __ctx.get::<::oxide_mongo::MongoClient>().expect(
                    "MongoClient exists (declared as a dependency, so construct-order guarantees it)"
                );
                ::std::boxed::Box::pin(async move {
                    let __collection = __client.collection::<#doc_ty>(
                        <#doc_ty as ::oxide_mongo::__private::DocumentMeta>::collection_name(),
                    )?;
                    // `OnceLock::set` only fails if already set, which can't
                    // happen: `post_construct` hooks run at most once per
                    // component per boot.
                    let _ = __repo.collection.set(__collection);
                    ::core::result::Result::Ok(())
                })
            })
            .build()
        }
    })
}

fn arg_idents(sig: &Signature) -> Vec<syn::Ident> {
    typed_args(sig).into_iter().map(|(ident, _)| ident).collect()
}

/// The method's parameters as `(name, type)`, `self` excluded.
fn typed_args(sig: &Signature) -> Vec<(syn::Ident, syn::Type)> {
    sig.inputs
        .iter()
        .filter_map(|a| {
            if let FnArg::Typed(pt) = a {
                if let syn::Pat::Ident(id) = &*pt.pat { Some((id.ident.clone(), (*pt.ty).clone())) } else { None }
            } else {
                None
            }
        })
        .collect()
}

/// The trailing `Page` parameter, if the method declares one.
///
/// Matched on the last path segment, so `Page`, `oxide_mongo::Page` and an
/// aliased import all work. It must be *last*: the remaining parameters are
/// bound positionally to the conditions parsed out of the method name, and a
/// `Page` in the middle would shift them.
fn page_arg(sig: &Signature) -> Option<syn::Ident> {
    let args = typed_args(sig);
    let (ident, ty) = args.last()?;
    let syn::Type::Path(path) = ty else { return None };
    if path.path.segments.last()?.ident == "Page" { Some(ident.clone()) } else { None }
}

fn expand_derived_or_crud(method_name: &str, sig: &Signature) -> Result<TokenStream> {
    let page = page_arg(sig);
    let mut args = arg_idents(sig);
    // The page is not a condition value; leaving it in would bind it to the
    // last field in the method name.
    if page.is_some() {
        args.pop();
    }

    // CRUD helpers (id is `_id`).
    match method_name {
        "find_by_id" => {
            let id = &args[0];
            return ok(
                quote! { ::oxide_mongo::__private::find_by_id(self.collection(), ::oxide_mongo::__private::to_bson(&#id).map_err(::oxide_mongo::Error::Bson)?).await },
            );
        }
        "find_all" => {
            return match &page {
                Some(page) => ok(quote! {
                    ::oxide_mongo::__private::find_page(self.collection(), ::oxide_mongo::__private::doc!{}, &#page).await
                }),
                None => ok(
                    quote! { ::oxide_mongo::__private::find_all(self.collection(), ::oxide_mongo::__private::doc!{}).await },
                ),
            };
        }
        "count" | "count_all" => {
            return ok(
                quote! { ::oxide_mongo::__private::count(self.collection(), ::oxide_mongo::__private::doc!{}).await },
            );
        }
        "delete_by_id" => {
            let id = &args[0];
            return ok(
                quote! { ::oxide_mongo::__private::delete_one(self.collection(), ::oxide_mongo::__private::doc!{ "_id": ::oxide_mongo::__private::to_bson(&#id).map_err(::oxide_mongo::Error::Bson)? }).await },
            );
        }
        "save" => {
            let d = &args[0];
            return ok(quote! { ::oxide_mongo::__private::save(self.collection(), #d).await });
        }
        "insert" => {
            let d = &args[0];
            return ok(quote! { ::oxide_mongo::__private::insert(self.collection(), #d).await });
        }
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
    let expected_args = conditions.iter().filter(|(_, op)| !is_marker_op(op)).count();
    if args.len() != expected_args {
        return Err(syn::Error::new_spanned(
            &sig.ident,
            format!(
                "derived query '{method_name}' expects {expected_args} argument(s) matching its field \
                 conditions (marker conditions like _is_null/_is_not_null take none), found {}",
                args.len()
            ),
        ));
    }
    let pairs: Vec<TokenStream> = conditions.iter().map(|(f, op)| quote! { (#f, #op) }).collect();
    let bson_args: Vec<TokenStream> = args
        .iter()
        .map(|a| quote! { ::oxide_mongo::__private::to_bson(&#a).map_err(::oxide_mongo::Error::Bson)? })
        .collect();

    match verb {
        "find" => match classify_find_kind(sig)? {
            FindKind::One => ok(quote! {
                ::oxide_mongo::__private::find_one(self.collection(),
                    ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*])).await
            }),
            FindKind::Many => match &page {
                Some(page) => ok(quote! {
                    ::oxide_mongo::__private::find_page(self.collection(),
                        ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*]),
                        &#page).await
                }),
                None => ok(quote! {
                    ::oxide_mongo::__private::find_all(self.collection(),
                        ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*])).await
                }),
            },
        },
        "find_all" => match &page {
            Some(page) => ok(quote! {
                ::oxide_mongo::__private::find_page(self.collection(),
                    ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*]),
                    &#page).await
            }),
            None => ok(quote! {
                ::oxide_mongo::__private::find_all(self.collection(),
                    ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*])).await
            }),
        },
        "count" => ok(quote! {
            ::oxide_mongo::__private::count(self.collection(),
                ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*])).await
        }),
        "exists" => ok(quote! {
            ::oxide_mongo::__private::exists(self.collection(),
                ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*])).await
        }),
        "delete" => ok(quote! {
            ::oxide_mongo::__private::delete_many(self.collection(),
                ::oxide_mongo::__private::derived_filter(&[#(#pairs),*], vec![#(#bson_args),*])).await
        }),
        _ => Err(syn::Error::new_spanned(&sig.ident, "unsupported derived verb")),
    }
}

fn ok(ts: TokenStream) -> Result<TokenStream> {
    Ok(ts)
}

/// How a derived `find_by_<field>` method reports its match(es).
enum FindKind {
    /// Signature says `Option<T>`: take the first match, if any.
    One,
    /// Signature says `Vec<T>`: return every match.
    Many,
}

/// Classifies a `find_by_<field>` method's return type as `Option<T>` or
/// `Vec<T>` (the `T` in `Result<Option<T>, _>` / `Result<Vec<T>, _>`). Any
/// other shape — a bare type, a `Result<T, _>` without `Option`/`Vec`, a
/// non-`Result` return — is a compile-time `syn::Error` rather than a
/// silent fallback, since guessing wrong would silently change the method's
/// semantics (returning the first match vs. every match).
fn classify_find_kind(sig: &Signature) -> Result<FindKind> {
    let bad = || {
        syn::Error::new_spanned(
            &sig.ident,
            format!("derived query '{}' must return `Result<Option<T>, _>` or `Result<Vec<T>, _>`", sig.ident),
        )
    };
    let ok_ty = returned_ok_type(&sig.output).ok_or_else(bad)?;
    let Type::Path(tp) = &ok_ty else { return Err(bad()) };
    let Some(seg) = tp.path.segments.last() else { return Err(bad()) };
    if seg.ident == "Option" {
        Ok(FindKind::One)
    } else if seg.ident == "Vec" {
        Ok(FindKind::Many)
    } else {
        Err(bad())
    }
}

/// Extracts the `T` in `-> Result<T, E>`, or `None` for a non-`Result`
/// return type.
fn returned_ok_type(out: &syn::ReturnType) -> Option<Type> {
    let syn::ReturnType::Type(_, ty) = out else { return None };
    let Type::Path(path) = &**ty else { return None };
    let last = path.path.segments.last()?;
    if last.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &last.arguments else { return None };
    let first = args.args.first()?;
    match first {
        GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    }
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
        ("_is_not_null", "$notnull"),
        ("_is_null", "$null"),
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

/// `_is_null`/`_is_not_null` are marker operators: `derived_filter` fills
/// them in without a value, so they must not consume a method argument.
fn is_marker_op(op: &str) -> bool {
    op == "$null" || op == "$notnull"
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
                if let syn::Expr::Lit(el) = v
                    && let syn::Lit::Str(s) = el.lit
                {
                    found = Some(s);
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
        quote! { ::oxide_mongo::__private::count(self.collection(), filter).await }
    } else if name.starts_with("delete") {
        quote! { ::oxide_mongo::__private::delete_many(self.collection(), filter).await }
    } else {
        quote! { ::oxide_mongo::__private::find_all(self.collection(), filter).await }
    };

    Ok(quote! {
        let filter = ::oxide_mongo::__private::raw_filter(#filter_lit, vec![#(#bson_args),*])?;
        #driver
    })
}
