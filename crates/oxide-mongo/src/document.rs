//! Document metadata produced by `#[derive(Document)]`.

/// A single index declaration produced by `#[index(...)]`.
#[derive(Debug, Clone)]
pub struct IndexDecl {
    pub fields: &'static str,
    pub unique: bool,
}

/// Metadata a `#[derive(Document)]`-annotated struct provides to the runtime.
pub trait DocumentMeta {
    fn collection_name() -> &'static str;
    fn id_field() -> &'static str;
    fn field_names() -> &'static [&'static str];
    fn indexes() -> &'static [IndexDecl];
}