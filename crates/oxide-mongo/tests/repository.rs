//! Integration test for oxide-mongo: `#[derive(Document)]` + `#[mongo_repository]`
//! against a real MongoDB container (CRUD + derived queries).

use mongodb::bson::oid::ObjectId;
use oxide_mongo::Document;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Document, PartialEq)]
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
    async fn count(&self) -> oxide_mongo::Result<u64>;
    async fn delete_by_id(&self, id: ObjectId) -> oxide_mongo::Result<u64>;

    // Derived queries
    async fn find_all_by_status(&self, status: String) -> oxide_mongo::Result<Vec<User>>;
    async fn count_by_status(&self, status: String) -> oxide_mongo::Result<u64>;
    async fn find_by_email(&self, email: String) -> oxide_mongo::Result<Vec<User>>;
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

#[tokio::test(flavor = "multi_thread")]
async fn repository_crud_and_derived_queries() {
    let uri = oxide_test::containers::mongo().await.endpoint;
    let client = mongodb::Client::with_uri_str(&uri).await.expect("connect");
    // Wait until Mongo actually accepts writes (it can lag behind container start).
    tokio::time::timeout(std::time::Duration::from_secs(90), async {
        loop {
            if client.database("admin").run_command(mongodb::bson::doc! { "ping": 1 }).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("mongo became ready");
    let users = client.database("oxide-test").collection::<User>("users");
    users.delete_many(mongodb::bson::doc! {}).await.expect("clean collection");
    let repo = UserRepositoryImpl::new(users);

    // CRUD save + find_by_id.
    let u1 = user(1, "active", 30);
    repo.save(&u1).await.expect("save");
    let found = repo.find_by_id(u1.id).await.expect("find_by_id");
    assert_eq!(found.map(|x| x.email), Some(u1.email.clone()));
    assert_eq!(repo.count().await.expect("count"), 1);

    // Derived: find_all_by_status, count_by_status, find_by_email.
    let u2 = user(2, "inactive", 40);
    repo.save(&u2).await.expect("save second");
    let u3 = user(3, "active", 20);
    repo.save(&u3).await.expect("save third");
    let active = repo.find_all_by_status("active".to_string()).await.expect("active query");
    assert_eq!(active.len(), 2, "active users");
    assert_eq!(repo.count_by_status("inactive".to_string()).await.unwrap(), 1, "inactive count");
    assert_eq!(repo.find_by_email("u1@x.com".to_string()).await.unwrap().len(), 1, "email count");

    // Derived operator: age > 25.
    assert_eq!(repo.find_by_age_greater_than(25).await.unwrap().len(), 2);

    // Escape hatch: status=active AND age > 25 → only u1 (age 30).
    let special = repo.find_special("active".to_string(), 25).await.expect("find_special");
    assert_eq!(special.len(), 1, "special query");
    assert_eq!(special[0].email, "u1@x.com");

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
    // null. Of the active users seeded above (u1, u3, regexy, plain), mark
    // one as soft-deleted so the query must exclude it by both conditions,
    // not just one — an off-by-one in the argument-to-field alignment would
    // either bind "active" to the wrong field or miscount the marker.
    let mut soft_deleted = user(6, "active", 22);
    soft_deleted.deleted_at = Some("2026-01-01".to_string());
    repo.save(&soft_deleted).await.expect("save soft-deleted");
    let mut inactive_live = user(7, "inactive", 22);
    inactive_live.deleted_at = None;
    repo.save(&inactive_live).await.expect("save inactive live");

    let live_active = repo.find_all_by_status_and_deleted_at_is_null("active".to_string()).await.expect("mixed query");
    assert_eq!(live_active.len(), 4, "active and not soft-deleted");
    assert!(live_active.iter().all(|u| u.status == "active" && u.deleted_at.is_none()));
    assert!(!live_active.iter().any(|u| u.id == soft_deleted.id), "soft-deleted must be excluded");
}
