//! Procedural macros for Oxide Boot's MongoDB starter.
//!
//! - `#[derive(Document)]` — marks a struct as a Mongo document and records
//!   its collection name, id field, and index declarations.
//! - `#[mongo_repository(document = T)]` — turns a `trait` of derived-query /
//!   CRUD method signatures into a runtime repository backed by `oxide_mongo`.
//!   Generated code references paths under `::oxide_mongo::__private`.

mod document;
mod repository;

use proc_macro::TokenStream;

/// Marks a struct as a MongoDB document (collection + id + index metadata).
#[proc_macro_derive(Document, attributes(collection, id, index))]
pub fn derive_document(input: TokenStream) -> TokenStream {
    document::expand(input.into()).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// Turns a trait of repository method signatures into a Mongo repository.
#[proc_macro_attribute]
pub fn mongo_repository(args: TokenStream, item: TokenStream) -> TokenStream {
    repository::expand(args.into(), item.into()).unwrap_or_else(syn::Error::into_compile_error).into()
}
