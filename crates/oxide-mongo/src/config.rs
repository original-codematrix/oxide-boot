//! Mongo configuration and the `MongoClient` lifecycle component, plus the
//! low-level collection helpers generated repository code calls into.

use mongodb::bson::{Bson, Document, doc};
use mongodb::{Client, Collection};
use oxide_core::{ComponentDescriptor, Context};
use std::sync::{Arc, OnceLock};

/// Configuration bound to the `oxide.mongo` prefix. `uri` is required
/// (integration starter: missing section ⇒ fail-fast).
#[derive(Clone, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.mongo")]
pub struct MongoProps {
    #[serde(rename = "uri")]
    pub uri: String,
    #[serde(default = "default_database")]
    pub database: String,
    /// Ping the server at boot.
    #[serde(default = "default_true")]
    pub ping_on_start: bool,
}

// `uri` embeds the connection credentials, so a hand-written `Debug` redacts
// it instead of deriving one that would print it verbatim (e.g. into logs).
impl std::fmt::Debug for MongoProps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoProps")
            .field("uri", &"<redacted>")
            .field("database", &self.database)
            .field("ping_on_start", &self.ping_on_start)
            .finish()
    }
}

impl Default for MongoProps {
    fn default() -> Self {
        Self { uri: String::new(), database: default_database(), ping_on_start: default_true() }
    }
}

fn default_database() -> String {
    "oxide".to_string()
}
fn default_true() -> bool {
    true
}

/// Injectable handle to the connected MongoDB driver. The driver client is
/// created lazily at `post_construct` because `Client::with_uri_str` is async.
#[derive(Clone)]
pub struct MongoClient {
    uri: String,
    database: String,
    /// Filled during `post_construct`.
    client: Arc<OnceLock<Client>>,
}

impl MongoClient {
    /// Returns the driver `Client` (created at post_construct) or an error if
    /// boot hasn't finished yet.
    pub fn raw(&self) -> Result<&Client, crate::Error> {
        self.client.get().ok_or_else(|| crate::Error::NotStarted)
    }

    /// The configured database name.
    pub fn database_name(&self) -> &str {
        &self.database
    }

    /// A typed collection handle for a document type `T`.
    pub fn collection<T: Send + Sync>(&self, name: &str) -> Result<Collection<T>, crate::Error> {
        Ok(self.raw()?.database(&self.database).collection(name))
    }

    /// Pings the primary (used by the health indicator).
    pub async fn ping(&self) -> Result<(), crate::Error> {
        self.raw()?.database("admin").run_command(doc! { "ping": 1 }).await.map(|_| ()).map_err(Into::into)
    }

    fn new(uri: String, database: String) -> Self {
        Self { uri, database, client: Arc::new(OnceLock::new()) }
    }

    async fn connect(&self) -> Result<(), crate::Error> {
        if self.client.get().is_none() {
            let client = Client::with_uri_str(&self.uri).await?;
            let _ = self.client.set(client);
        }
        Ok(())
    }
}

inventory::submit! {
    ComponentDescriptor::builder(
        "MongoClient",
        std::any::TypeId::of::<MongoClient>,
        |b| {
            // `?` rather than `expect`: `resolve` returning `Err` here would
            // mean the `deps` declaration below and `assemble`'s dependency
            // check disagree, which should surface as a `ContextError`
            // (this closure's own return type), not panic.
            let props = b.resolve::<MongoProps>()?;
            b.insert(MongoClient::new(props.uri.clone(), props.database.clone()));
            Ok(())
        },
    )
    .deps(|| vec![(std::any::TypeId::of::<MongoProps>(), "MongoProps")])
    // Safe by construction: `assemble` runs every component's `construct`
    // closure (including this one, right above) before any `post_construct`
    // hook runs, so both `MongoClient` and `MongoProps` are unconditionally
    // already in the context by the time this fires.
    .post_construct(|ctx: &Context| {
        let client = ctx.get::<MongoClient>().expect("MongoClient exists");
        let props = ctx.get::<MongoProps>().expect("MongoProps exists").clone();
        Box::pin(async move {
            client.connect().await.map_err(|e| {
                // The driver's error text can echo back the connection string
                // (credentials included), so log it and keep the propagated
                // message generic.
                tracing::error!(error = %e, "mongo connection failed");
                oxide_core::ContextError::Construct {
                    type_name: "MongoClient",
                    message: "failed to connect to MongoDB".to_string(),
                }
            })?;
            if props.ping_on_start
                && let Err(e) = client.ping().await
            {
                tracing::error!(error = %e, "mongo ping at boot failed");
            }
            Ok(())
        })
    })
    .build()
}

// ---------------------------------------------------------------------------
// Collection helpers referenced by generated repository code.
// ---------------------------------------------------------------------------

pub async fn find_all<T>(collection: &Collection<T>, filter: Document) -> Result<Vec<T>, crate::Error>
where
    T: serde::de::DeserializeOwned + Send + Sync,
{
    use futures::TryStreamExt;
    let cursor = collection.find(filter).await?;
    cursor.try_collect::<Vec<T>>().await.map_err(Into::into)
}

pub async fn find_by_id<T>(collection: &Collection<T>, id: Bson) -> Result<Option<T>, crate::Error>
where
    T: serde::de::DeserializeOwned + Send + Sync,
{
    collection.find_one(doc! { "_id": id }).await.map_err(Into::into)
}

pub async fn count<T>(collection: &Collection<T>, filter: Document) -> Result<u64, crate::Error>
where
    T: Send + Sync,
{
    collection.count_documents(filter).await.map_err(Into::into)
}

pub async fn exists<T>(collection: &Collection<T>, filter: Document) -> Result<bool, crate::Error>
where
    T: Send + Sync,
{
    Ok(count(collection, filter).await? > 0)
}

pub async fn delete_many<T>(collection: &Collection<T>, filter: Document) -> Result<u64, crate::Error>
where
    T: Send + Sync,
{
    collection.delete_many(filter).await.map(|r| r.deleted_count).map_err(Into::into)
}

pub async fn delete_one<T>(collection: &Collection<T>, filter: Document) -> Result<u64, crate::Error>
where
    T: Send + Sync,
{
    collection.delete_one(filter).await.map(|r| r.deleted_count).map_err(Into::into)
}

pub async fn save<T>(collection: &Collection<T>, doc: &T) -> Result<(), crate::Error>
where
    T: serde::Serialize + Sync + Send,
{
    collection.insert_one(doc).await.map(|_| ()).map_err(Into::into)
}

pub async fn insert<T>(collection: &Collection<T>, doc: &T) -> Result<(), crate::Error>
where
    T: serde::Serialize + Sync + Send,
{
    collection.insert_one(doc).await.map(|_| ()).map_err(Into::into)
}

/// Merges the given filter documents into one (used by derived `_and_` queries).
pub fn merge(docs: impl IntoIterator<Item = Document>) -> Document {
    let mut out = Document::new();
    for d in docs {
        for (k, v) in d {
            out.insert(k, v);
        }
    }
    out
}

/// Builds a Mongo filter document from `(field, operator)` pairs and their
/// corresponding BSON values (produced by the derived-query macro from the
/// method name and arguments).
///
/// `$null` and `$notnull` are compile-time markers for `_is_null`/
/// `_is_not_null` conditions: they take no value, so they do not consume an
/// entry from `values` — every other operator consumes the next one in
/// order. `$regex` (from `_like`) escapes its value so the caller-supplied
/// string is matched as a literal substring, never interpreted as a regular
/// expression. The escape only applies to a `Bson::String` value; the
/// generated macro code only ever produces `$regex` conditions from string
/// arguments, but a direct caller passing another `Bson` variant here
/// bypasses escaping.
pub fn derived_filter(pairs: &[(&str, &str)], values: Vec<Bson>) -> Document {
    let mut out = Document::new();
    let mut values = values.into_iter();
    for (field, op) in pairs {
        match *op {
            "$null" => {
                out.insert(*field, Bson::Null);
            }
            "$notnull" => {
                let mut operator = Document::new();
                operator.insert("$ne", Bson::Null);
                out.insert(*field, operator);
            }
            "$eq" => {
                if let Some(value) = values.next() {
                    out.insert(*field, value);
                }
            }
            "$regex" => {
                if let Some(value) = values.next() {
                    let escaped = match value {
                        Bson::String(s) => Bson::String(regex::escape(&s)),
                        other => other,
                    };
                    let mut operator = Document::new();
                    operator.insert("$regex", escaped);
                    out.insert(*field, operator);
                }
            }
            _ => {
                if let Some(value) = values.next() {
                    let mut operator = Document::new();
                    operator.insert(op.to_string(), value);
                    out.insert(*field, operator);
                }
            }
        }
    }
    out
}

/// Builds a raw Mongo filter from a `#[query(filter = "...")]` template with
/// `?1`/`?2`/... positional placeholders substituted by the method arguments
/// (rendered as extended JSON), then parsed into a `Document`.
///
/// The template is scanned once, left to right: on `?` all following digits
/// are read as a single placeholder index and the rendered value is written
/// straight to the output, so `?1` never matches inside `?10`, and a
/// substituted value that itself contains something like `?2` is never
/// substituted again. The scan does not track JSON string-literal
/// boundaries, so a `?`-plus-digits run that happens to sit inside a quoted
/// string in the template is still treated as a placeholder.
pub fn raw_filter(template: &str, values: Vec<Bson>) -> crate::Result<Document> {
    let chars: Vec<char> = template.chars().collect();
    let mut out = String::with_capacity(template.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '?' {
            let start = i + 1;
            let mut end = start;
            while end < chars.len() && chars[end].is_ascii_digit() {
                end += 1;
            }
            if end > start {
                let digits: String = chars[start..end].iter().collect();
                // The scan above only ever collects ASCII digits into
                // `digits`, so this can only fail on integer overflow (an
                // implausibly long digit run) — a real, if unlikely,
                // request-time input rather than a startup invariant, so it
                // is surfaced as an error instead of panicking the caller.
                let index: usize =
                    digits.parse().map_err(|_| crate::Error::InvalidPlaceholder { digits: digits.clone() })?;
                let value = values.get(index - 1).ok_or(crate::Error::Placeholder { index })?;
                let rendered = serde_json::to_string(value).map_err(crate::Error::Json)?;
                out.push_str(&rendered);
                i = end;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    let json: serde_json::Value = serde_json::from_str(&out).map_err(crate::Error::Json)?;
    let bson: Bson = Bson::try_from(json).map_err(crate::Error::ExtJson)?;
    bson.as_document().cloned().ok_or(crate::Error::NotADocument)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_escapes_regex_metacharacters() {
        let d = derived_filter(&[("name", "$regex")], vec![Bson::String("a.*b".into())]);
        let sub = d.get_document("name").unwrap();
        assert_eq!(sub.get_str("$regex").unwrap(), regex::escape("a.*b"));
    }

    #[test]
    fn is_null_needs_no_argument_and_matches_null() {
        let d = derived_filter(&[("deleted_at", "$null")], vec![]);
        assert_eq!(d.get("deleted_at"), Some(&Bson::Null));
    }

    #[test]
    fn is_not_null_matches_ne_null() {
        let d = derived_filter(&[("deleted_at", "$notnull")], vec![]);
        assert_eq!(d.get_document("deleted_at").unwrap().get("$ne"), Some(&Bson::Null));
    }

    #[test]
    fn placeholder_ten_is_not_matched_by_placeholder_one() {
        let values: Vec<Bson> = (1..=10).map(Bson::Int32).collect();
        let d = raw_filter(r#"{ "a": ?1, "b": ?10 }"#, values).unwrap();
        assert_eq!(d.get("a"), Some(&Bson::Int32(1)));
        assert_eq!(d.get("b"), Some(&Bson::Int32(10)));
    }

    #[test]
    fn substituted_values_are_not_substituted_again() {
        let d =
            raw_filter(r#"{ "a": ?1, "b": ?2 }"#, vec![Bson::String("?2".into()), Bson::String("x".into())]).unwrap();
        assert_eq!(d.get_str("a").unwrap(), "?2");
        assert_eq!(d.get_str("b").unwrap(), "x");
    }

    #[test]
    fn missing_argument_for_placeholder_is_an_error() {
        assert!(raw_filter(r#"{ "a": ?2 }"#, vec![Bson::Int32(1)]).is_err());
    }

    #[test]
    fn debug_output_redacts_the_uri() {
        let props = MongoProps { uri: "mongodb://user:secret@host:27017".into(), ..Default::default() };
        let rendered = format!("{props:?}");
        assert!(!rendered.contains("secret"), "credentials leaked: {rendered}");
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn marker_first_does_not_shift_the_following_value_argument() {
        let d = derived_filter(&[("deleted_at", "$null"), ("status", "$eq")], vec![Bson::String("active".into())]);
        assert_eq!(d.get("deleted_at"), Some(&Bson::Null));
        assert_eq!(d.get_str("status").unwrap(), "active");
    }

    #[test]
    fn marker_second_does_not_consume_the_preceding_value_argument() {
        let d = derived_filter(&[("status", "$eq"), ("deleted_at", "$null")], vec![Bson::String("active".into())]);
        assert_eq!(d.get_str("status").unwrap(), "active");
        assert_eq!(d.get("deleted_at"), Some(&Bson::Null));
    }
}
