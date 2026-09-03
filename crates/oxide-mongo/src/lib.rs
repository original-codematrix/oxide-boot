//! Oxide Boot MongoDB starter: `#[derive(Document)]` + `#[mongo_repository]`
//! repositories on the official async `mongodb` driver, wired into the boot
//! lifecycle with a health indicator.

pub mod config;
pub mod document;
mod health;

pub use config::{MongoClient, MongoProps};

/// Internal paths referenced by generated macro code.
#[doc(hidden)]
pub mod __private {
    pub use crate::config::{
        count, delete_many, delete_one, derived_filter, exists, find_all, find_by_id, insert, merge, raw_filter, save,
    };
    pub use mongodb::bson::{self, Bson, Document, doc, to_bson};
    pub use mongodb::{Collection, Database};
    pub use oxide_core::{ComponentDescriptor, Context};
    pub use serde::de::DeserializeOwned;
}

/// Errors produced by the Mongo starter.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
    #[error("failed to serialize a value to BSON: {0}")]
    Bson(#[from] mongodb::bson::ser::Error),
    #[error("failed to render a BSON value as JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to parse the #[query] filter as extended JSON: {0}")]
    ExtJson(#[from] mongodb::bson::extjson::de::Error),
    #[error("the #[query] filter did not parse into a Mongo document")]
    NotADocument,
    #[error("#[query] filter references placeholder ?{index} but no matching argument was given")]
    Placeholder { index: usize },
    #[error("#[query] filter placeholder '?{digits}' is not a valid index")]
    InvalidPlaceholder { digits: String },
    #[error("mongo client has not been started yet (post_construct has not run)")]
    NotStarted,
}

/// Result alias for the Mongo starter.
pub type Result<T> = std::result::Result<T, Error>;

/// Re-exported for `#[mongo_repository]` users.
pub use oxide_mongo_macros::{Document, mongo_repository};
