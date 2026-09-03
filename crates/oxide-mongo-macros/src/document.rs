//! Expansion for `#[derive(Document)]`: emits a `oxide_mongo::document::DocumentMeta`
//! implementation describing the collection, id field and indexes.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Field, Fields, LitStr};

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let ident = &input.ident;
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(ident, "#[derive(Document)] only supports structs"));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(ident, "#[derive(Document)] requires named fields"));
    };

    // Collection name: `#[collection("x")]` on the struct, else snake_case name.
    let collection: Option<LitStr> =
        input.attrs.iter().find(|a| a.path().is_ident("collection")).map(|a| a.parse_args::<LitStr>()).transpose()?;
    let collection_lit = match collection {
        Some(c) => quote! { #c },
        None => {
            let snake = convert_to_snake(&ident.to_string());
            quote! { #snake }
        }
    };

    // Id field: exactly one `#[id]` field.
    let mut id_field: Option<syn::Ident> = None;
    let mut field_names: Vec<String> = Vec::new();
    let mut indexes: Vec<TokenStream> = Vec::new();
    for field in &fields.named {
        // Unreachable: `fields` was destructured from `Fields::Named` above,
        // so every field here has an ident by definition. Also compile-time
        // only (macro expansion) — a panic here fails the build, it can
        // never reach a running program.
        let fname = field.ident.as_ref().expect("named field");
        field_names.push(fname.to_string());
        for attr in &field.attrs {
            if attr.path().is_ident("id") {
                if id_field.is_some() {
                    return Err(syn::Error::new_spanned(fname, "#[id] may only appear once"));
                }
                id_field = Some(fname.clone());
            } else if attr.path().is_ident("index") {
                indexes.push(parse_index_attr(field, fname)?);
            }
        }
    }
    let id_field = id_field
        .ok_or_else(|| syn::Error::new_spanned(ident, "#[derive(Document)] requires one field marked #[id]"))?;

    let field_names_lit = field_names;
    let indices_ty = quote! { &'static [oxide_mongo::document::IndexDecl] };

    Ok(quote! {
        impl oxide_mongo::document::DocumentMeta for #ident {
            fn collection_name() -> &'static str { #collection_lit }
            fn id_field() -> &'static str { stringify!(#id_field) }
            fn field_names() -> &'static [&'static str] { &[ #(#field_names_lit),* ] }
            fn indexes() -> #indices_ty { &[ #(#indexes),* ] }
        }
    })
}

fn parse_index_attr(field: &Field, ident: &syn::Ident) -> syn::Result<TokenStream> {
    // `#[index(fields = "email", unique)]` — emit a runtime IndexDecl. Fields
    // come as a comma-joined string; unique is a bare path.
    let mut unique = false;
    for attr in &field.attrs {
        if attr.path().is_ident("index") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("unique") {
                    unique = true;
                    Ok(())
                } else if meta.path.is_ident("fields") {
                    let _v: LitStr = meta.value()?.parse()?;
                    Ok(())
                } else {
                    Err(meta.error("expected `fields = \"...\"` or `unique`"))
                }
            })?;
        }
    }
    Ok(quote! {
        oxide_mongo::document::IndexDecl {
            fields: stringify!(#ident),
            unique: #unique,
        }
    })
}

fn convert_to_snake(s: &str) -> String {
    let mut out = String::new();
    let mut prev_upper = false;
    for (i, c) in s.char_indices() {
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
    out
}
