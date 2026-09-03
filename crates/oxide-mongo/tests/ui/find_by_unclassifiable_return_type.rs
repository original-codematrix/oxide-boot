use oxide_mongo::Document;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Document)]
struct Item {
    #[id]
    #[serde(rename = "_id")]
    id: String,
    name: String,
}

// `find_by_name` returns neither `Option<T>` nor `Vec<T>` as its `Result`'s
// `Ok` type, so the macro cannot classify it as "first match" or "every
// match" and must reject it at compile time instead of guessing.
#[oxide_mongo::mongo_repository(document = Item)]
trait ItemRepository {
    async fn find_by_name(&self, name: String) -> oxide_mongo::Result<String>;
}

fn main() {}
