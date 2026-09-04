//! Per-test database isolation against one shared MongoDB.
//!
//! Both tests below write to a collection of the same name at the same time.
//! Under the old pattern — one shared database, cleaned by dropping the
//! collection between tests — that is exactly the shape that fails only with
//! `cargo test`'s default parallelism and passes under `--test-threads=1`.

use mongodb::bson::doc;
use oxide_mongo_test::{MongoTest, unique_database};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Note {
    #[serde(rename = "_id")]
    id: u32,
    body: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn one_test_fills_a_shared_collection_name() {
    let t = MongoTest::start().await;
    let notes = t.collection::<Note>("notes");
    notes.insert_one(Note { id: 1, body: "first".into() }).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Exactly one document, and it is this test's: a shared database would
    // show the other test's insert here too.
    assert_eq!(notes.count_documents(doc! {}).await.unwrap(), 1);
    let found = notes.find_one(doc! { "_id": 1 }).await.unwrap().expect("its own document");
    assert_eq!(found.body, "first");

    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_other_test_uses_the_same_collection_name() {
    let t = MongoTest::start().await;
    let notes = t.collection::<Note>("notes");
    notes.insert_one(Note { id: 1, body: "second".into() }).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    assert_eq!(notes.count_documents(doc! {}).await.unwrap(), 1);
    let found = notes.find_one(doc! { "_id": 1 }).await.unwrap().expect("its own document");
    assert_eq!(found.body, "second", "the other test's document leaked into this database");

    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn each_fixture_gets_its_own_database() {
    let a = MongoTest::start().await;
    let b = MongoTest::start().await;
    assert_ne!(a.database(), b.database());

    a.collection::<Note>("shared").insert_one(Note { id: 7, body: "a".into() }).await.unwrap();
    assert_eq!(b.collection::<Note>("shared").count_documents(doc! {}).await.unwrap(), 0);

    a.stop().await.unwrap();
    b.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn await_count_polls_instead_of_sleeping() {
    let t = MongoTest::start().await;
    let notes = t.collection::<Note>("notes");

    // Insert from a task, so the wait has something to actually wait for --
    // this is the shape of "handler wrote something after responding".
    let handle = notes.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        handle.insert_one(Note { id: 1, body: "late".into() }).await.unwrap();
    });

    t.await_count::<Note>("notes", 1, std::time::Duration::from_secs(5)).await;
    t.stop().await.unwrap();
}

#[test]
fn database_names_do_not_repeat() {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..1_000 {
        assert!(seen.insert(unique_database()), "a database name repeated within one process");
    }
}
