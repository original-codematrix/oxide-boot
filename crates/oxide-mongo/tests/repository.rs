//! Integration test for oxide-mongo: `#[derive(Document)]` + `#[mongo_repository]`
//! against a real MongoDB container (CRUD + derived queries).
//!
//! The repository is resolved from the DI `Context` after a full
//! `OxideApplication` boot, rather than hand-constructed — that's the proof
//! that `#[mongo_repository]` registers the generated `…Impl` as an
//! injectable component (the same `inventory::submit!` pattern
//! `#[http_client]` uses), not just that the generated type compiles.

use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use oxide_boot::OxideApplication;
use oxide_mongo::Document;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, Document, PartialEq)]
#[collection("users")]
struct User {
    #[id]
    #[serde(rename = "_id")]
    id: ObjectId,
    email: String,
    status: String,
    age: i32,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deleted_at: Option<String>,
}

#[allow(dead_code)]
#[oxide_mongo::mongo_repository(document = User)]
trait UserRepository {
    // CRUD
    async fn find_by_id(&self, id: ObjectId) -> oxide_mongo::Result<Option<User>>;
    async fn find_all(&self) -> oxide_mongo::Result<Vec<User>>;
    async fn save(&self, user: &User) -> oxide_mongo::Result<()>;
    async fn insert(&self, user: &User) -> oxide_mongo::Result<()>;
    async fn count(&self) -> oxide_mongo::Result<u64>;
    async fn delete_by_id(&self, id: ObjectId) -> oxide_mongo::Result<u64>;

    // Derived queries
    async fn find_all_by_status(&self, status: String) -> oxide_mongo::Result<Vec<User>>;
    async fn count_by_status(&self, status: String) -> oxide_mongo::Result<u64>;
    // `find_by_email` returns `Option<User>`: the macro must take the first
    // match rather than a `Vec`, per the method's own declared signature.
    async fn find_by_email(&self, email: String) -> oxide_mongo::Result<Option<User>>;
    async fn find_by_age_greater_than(&self, age: i32) -> oxide_mongo::Result<Vec<User>>;
    async fn find_all_by_name_like(&self, name: String) -> oxide_mongo::Result<Vec<User>>;
    // Mixed marker + value condition: verifies the argument-to-field
    // alignment when `_is_null` (a no-argument marker) is combined with a
    // regular value condition in the same derived query.
    async fn find_all_by_status_and_deleted_at_is_null(&self, status: String) -> oxide_mongo::Result<Vec<User>>;

    // Escape hatch: raw filter with positional placeholders.
    #[query(filter = r#"{ "status": ?1, "age": { "$gt": ?2 } }"#)]
    async fn find_special(&self, status: String, min_age: i32) -> oxide_mongo::Result<Vec<User>>;
}

fn user(n: i32, status: &str, age: i32) -> User {
    User {
        id: ObjectId::new(),
        email: format!("u{n}@x.com"),
        status: status.to_string(),
        age,
        name: format!("user{n}"),
        deleted_at: None,
    }
}

/// Waits until Mongo actually accepts writes (it can lag behind container
/// start), then drops the probe connection — the driver connection actually
/// exercised by the test is the one the DI-managed `MongoClient` opens
/// during boot.
async fn wait_until_ready(uri: &str) {
    let client = mongodb::Client::with_uri_str(uri).await.expect("connect");
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if client.database("admin").run_command(doc! { "ping": 1 }).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("mongo became ready");
}

#[tokio::test(flavor = "multi_thread")]
async fn repository_crud_and_derived_queries() {
    let uri = oxide_test::containers::mongo().await.endpoint;
    wait_until_ready(&uri).await;

    // Clean the shared fixture's collection before this test's own boot
    // connects to it.
    let cleanup = mongodb::Client::with_uri_str(&uri).await.expect("connect for cleanup");
    cleanup.database("oxide-test").collection::<User>("users").delete_many(doc! {}).await.expect("clean collection");

    let dir = tempfile::tempdir().expect("tempdir");
    let yml = format!(
        "oxide:\n  application:\n    name: mongo-repository-test\n  mongo:\n    uri: \"{uri}\"\n    database: \"oxide-test\"\n"
    );
    std::fs::write(dir.path().join("application.yml"), yml).expect("write application.yml");

    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
        .start()
        .await
        .expect("boot oxide app with mongo repository");

    // Proof of injection: the repository comes out of the DI context, not a
    // hand-built `UserRepositoryImpl::new(...)`.
    let repo = app.context().get::<UserRepositoryImpl>().expect("UserRepositoryImpl is registered as a component");

    // Upsert: saving the same id twice replaces the document instead of
    // erroring on a duplicate key or leaving two documents behind.
    let mut u1 = user(1, "active", 30);
    repo.save(&u1).await.expect("first save inserts");
    assert_eq!(repo.count().await.expect("count"), 1, "first save inserts exactly one document");
    u1.status = "inactive".to_string();
    u1.age = 31;
    repo.save(&u1).await.expect("second save with the same id upserts");
    assert_eq!(repo.count().await.expect("count"), 1, "second save with the same id does not insert a new document");
    let found = repo.find_by_id(u1.id).await.expect("find_by_id");
    assert_eq!(found, Some(u1.clone()), "the upserted document reflects the second save's fields, not the first's");

    // `insert` keeps insert-only semantics: a duplicate id is an error, not
    // collapsed into `save`'s upsert behavior.
    assert!(repo.insert(&u1).await.is_err(), "insert on a duplicate id must fail");
    assert_eq!(repo.count().await.expect("count"), 1, "the failed insert did not add a document");

    // Derived: find_all_by_status, count_by_status, find_by_email.
    let u2 = user(2, "inactive", 40);
    repo.save(&u2).await.expect("save second");
    let u3 = user(3, "active", 20);
    repo.save(&u3).await.expect("save third");
    let active = repo.find_all_by_status("active".to_string()).await.expect("active query");
    assert_eq!(active.len(), 1, "active users"); // u1 flipped to inactive above; u3 is active
    assert_eq!(repo.count_by_status("inactive".to_string()).await.unwrap(), 2, "inactive count");

    // `find_by_email` returns `Option<User>`.
    let by_email = repo.find_by_email(u3.email.clone()).await.unwrap();
    assert_eq!(by_email.map(|u| u.email), Some(u3.email.clone()), "find_by_email finds the match");
    let missing = repo.find_by_email("nobody@x.com".to_string()).await.unwrap();
    assert_eq!(missing, None, "find_by_email returns None for no match");

    // Derived operator: age > 25.
    assert_eq!(repo.find_by_age_greater_than(25).await.unwrap().len(), 2); // u1 (31), u2 (40)

    // Escape hatch: status=active AND age > 15 → only u3.
    let special = repo.find_special("active".to_string(), 15).await.expect("find_special");
    assert_eq!(special.len(), 1, "special query");
    assert_eq!(special[0].email, u3.email);

    // find_all + delete_by_id.
    assert_eq!(repo.find_all().await.unwrap().len(), 3);
    let deleted = repo.delete_by_id(u2.id).await.expect("delete");
    assert_eq!(deleted, 1);
    assert_eq!(repo.count().await.unwrap(), 2);

    // _like escapes regex metacharacters: a literal "a.*b" name does not
    // match a document named "axxb" (which a raw, unescaped regex would).
    let mut regexy = user(4, "active", 22);
    regexy.name = "a.*b".to_string();
    repo.save(&regexy).await.expect("save regexy");
    let mut plain = user(5, "active", 22);
    plain.name = "axxb".to_string();
    repo.save(&plain).await.expect("save plain");

    let matches = repo.find_all_by_name_like("a.*b".to_string()).await.expect("like query");
    assert_eq!(matches.len(), 1, "only the literal match should be found");
    assert_eq!(matches[0].name, "a.*b");

    // Mixed marker + value condition: status = "active" AND deleted_at is
    // null. Of the active users seeded above (u3, regexy, plain), mark one
    // as soft-deleted so the query must exclude it by both conditions, not
    // just one — an off-by-one in the argument-to-field alignment would
    // either bind "active" to the wrong field or miscount the marker.
    let mut soft_deleted = user(6, "active", 22);
    soft_deleted.deleted_at = Some("2026-01-01".to_string());
    repo.save(&soft_deleted).await.expect("save soft-deleted");
    let mut inactive_live = user(7, "inactive", 22);
    inactive_live.deleted_at = None;
    repo.save(&inactive_live).await.expect("save inactive live");

    let live_active = repo.find_all_by_status_and_deleted_at_is_null("active".to_string()).await.expect("mixed query");
    assert_eq!(live_active.len(), 3, "active and not soft-deleted"); // u3, regexy, plain
    assert!(live_active.iter().all(|u| u.status == "active" && u.deleted_at.is_none()));
    assert!(!live_active.iter().any(|u| u.id == soft_deleted.id), "soft-deleted must be excluded");

    app.stop().await.expect("app stops cleanly");
}
