//! The hash, list and set commands, and the repository above them.
//!
//! What is worth asserting here is not that `HSET` sets a field — that is
//! the driver's job — but the edges where a wrapper usually gets it wrong:
//! empty argument lists that Redis rejects as protocol errors, Redis's habit
//! of reporting "missing" and "empty" identically, negative list indices,
//! and the two sentinel values `TTL` returns instead of a duration.

use oxide_redis::{RedisEntity, RedisRepository};
use oxide_redis_test::RedisTest;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Note {
    id: String,
    title: String,
}

impl RedisEntity for Note {
    fn keyspace() -> &'static str {
        "note"
    }
    fn id(&self) -> String {
        self.id.clone()
    }
}

// ---------------------------------------------------------------------
// Hashes
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn hash_fields_round_trip() {
    let t = RedisTest::start().await;
    let r = t.template();

    assert!(r.hset("session:1", "user", "ada").await.unwrap(), "a new field reports true");
    assert!(!r.hset("session:1", "user", "grace").await.unwrap(), "overwriting reports false");
    assert_eq!(r.hget("session:1", "user").await.unwrap().as_deref(), Some("grace"));
    assert!(r.hexists("session:1", "user").await.unwrap());
    assert!(!r.hexists("session:1", "absent").await.unwrap());

    r.hset_all("session:1", &[("role", "admin"), ("tenant", "acme")]).await.unwrap();
    let all = r.hgetall("session:1").await.unwrap();
    assert_eq!(all.len(), 3, "was: {all:?}");
    assert_eq!(all.get("tenant").map(String::as_str), Some("acme"));

    assert_eq!(r.hdel("session:1", &["role", "never-set"]).await.unwrap(), 1, "only existing fields count");
    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_hash_reads_as_empty_not_as_an_error() {
    let t = RedisTest::start().await;
    // Redis does not distinguish "no such hash" from "hash with no fields",
    // so neither does this -- and it must not be an error either way.
    assert!(t.template().hgetall("never-written").await.unwrap().is_empty());
    assert_eq!(t.template().hget("never-written", "f").await.unwrap(), None);
    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_argument_lists_do_not_reach_the_server() {
    let t = RedisTest::start().await;
    let r = t.template();
    // Each of these is a protocol error if sent with no arguments, so they
    // are answered locally.
    r.hset_all("k", &[]).await.expect("HSET with no fields");
    assert_eq!(r.hdel("k", &[]).await.unwrap(), 0);
    assert_eq!(r.sadd("k2", &[]).await.unwrap(), 0);
    assert_eq!(r.srem("k2", &[]).await.unwrap(), 0);
    assert!(r.get_many(&[]).await.unwrap().is_empty());
    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn hincr_accumulates_per_field() {
    let t = RedisTest::start().await;
    let r = t.template();
    assert_eq!(r.hincr("stats", "views", 1).await.unwrap(), 1);
    assert_eq!(r.hincr("stats", "views", 4).await.unwrap(), 5);
    assert_eq!(r.hincr("stats", "clicks", 2).await.unwrap(), 2, "a different field counts separately");
    t.stop().await.unwrap();
}

// ---------------------------------------------------------------------
// Lists
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn lists_push_and_pop_from_both_ends() {
    let t = RedisTest::start().await;
    let r = t.template();

    r.rpush("queue", &["a", "b", "c"]).await.unwrap();
    r.lpush("queue", &["start"]).await.unwrap();
    assert_eq!(r.llen("queue").await.unwrap(), 4);

    // Negative indices are Redis's own convention: -1 is the last element.
    assert_eq!(r.lrange("queue", 0, -1).await.unwrap(), vec!["start", "a", "b", "c"]);
    assert_eq!(r.lrange("queue", 1, 2).await.unwrap(), vec!["a", "b"]);

    assert_eq!(r.lpop("queue").await.unwrap().as_deref(), Some("start"));
    assert_eq!(r.rpop("queue").await.unwrap().as_deref(), Some("c"));
    assert_eq!(r.llen("queue").await.unwrap(), 2);
    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn popping_an_empty_list_is_none() {
    let t = RedisTest::start().await;
    assert_eq!(t.template().lpop("empty").await.unwrap(), None);
    assert_eq!(t.template().llen("empty").await.unwrap(), 0);
    assert!(t.template().lrange("empty", 0, -1).await.unwrap().is_empty());
    t.stop().await.unwrap();
}

// ---------------------------------------------------------------------
// Sets
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn sets_report_what_was_actually_new() {
    let t = RedisTest::start().await;
    let r = t.template();

    assert_eq!(r.sadd("tags", &["a", "b", "a"]).await.unwrap(), 2, "the duplicate does not count");
    assert_eq!(r.sadd("tags", &["b", "c"]).await.unwrap(), 1);
    assert_eq!(r.scard("tags").await.unwrap(), 3);
    assert!(r.sismember("tags", "b").await.unwrap());
    assert!(!r.sismember("tags", "z").await.unwrap());

    let mut members = r.smembers("tags").await.unwrap();
    members.sort();
    assert_eq!(members, vec!["a", "b", "c"]);

    assert_eq!(r.srem("tags", &["a", "never-added"]).await.unwrap(), 1);
    t.stop().await.unwrap();
}

// ---------------------------------------------------------------------
// TTL
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ttl_distinguishes_no_expiry_from_no_key() {
    let t = RedisTest::start().await;
    let r = t.template();

    r.set("permanent", "v", None).await.unwrap();
    // Redis answers -1 for "no expiry" and -2 for "no key". Neither is a
    // duration, so both are None here rather than a negative Duration.
    assert_eq!(r.ttl("permanent").await.unwrap(), None);
    assert_eq!(r.ttl("never-written").await.unwrap(), None);

    r.set("temporary", "v", Some(Duration::from_secs(60))).await.unwrap();
    let ttl = r.ttl("temporary").await.unwrap().expect("an expiry was set");
    assert!(ttl.as_secs() > 50 && ttl.as_secs() <= 60, "ttl was {ttl:?}");

    assert!(r.persist("temporary").await.unwrap(), "removing an expiry reports true");
    assert_eq!(r.ttl("temporary").await.unwrap(), None);
    assert!(!r.persist("temporary").await.unwrap(), "there was no expiry left to remove");
    t.stop().await.unwrap();
}

// ---------------------------------------------------------------------
// Repository
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn repository_crud_round_trip() {
    let t = RedisTest::start().await;
    let repo = RedisRepository::<Note>::new(t.template().clone());

    assert_eq!(repo.count().await.unwrap(), 0);
    assert!(repo.find_all().await.unwrap().is_empty());

    repo.save(&Note { id: "1".into(), title: "first".into() }).await.unwrap();
    repo.save(&Note { id: "2".into(), title: "second".into() }).await.unwrap();

    assert_eq!(repo.count().await.unwrap(), 2);
    assert!(repo.exists("1").await.unwrap());
    assert_eq!(repo.find_by_id("1").await.unwrap().unwrap().title, "first");
    assert_eq!(repo.find_by_id("missing").await.unwrap(), None);

    let mut all = repo.find_all().await.unwrap();
    all.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(all.len(), 2);
    assert_eq!(all[1].title, "second");

    // Saving the same id replaces rather than duplicating.
    repo.save(&Note { id: "1".into(), title: "updated".into() }).await.unwrap();
    assert_eq!(repo.count().await.unwrap(), 2);
    assert_eq!(repo.find_by_id("1").await.unwrap().unwrap().title, "updated");

    assert!(repo.delete("1").await.unwrap());
    assert!(!repo.delete("1").await.unwrap(), "deleting twice reports false the second time");
    assert_eq!(repo.count().await.unwrap(), 1, "the index shrank with the entity");

    assert_eq!(repo.delete_all().await.unwrap(), 1);
    assert_eq!(repo.count().await.unwrap(), 0);
    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_id_containing_the_separator_is_rejected() {
    let t = RedisTest::start().await;
    let repo = RedisRepository::<Note>::new(t.template().clone());
    // `a:b` and `a` + `b` would otherwise land on colliding keys.
    let err = repo.save(&Note { id: "a:b".into(), title: "x".into() }).await.unwrap_err();
    assert!(err.to_string().contains("a:b"), "was: {err}");
    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn expired_entities_are_pruned_from_the_index() {
    let t = RedisTest::start().await;
    // One second is the shortest expiry Redis accepts.
    let repo = RedisRepository::<Note>::with_ttl(t.template().clone(), Duration::from_secs(1));
    repo.save(&Note { id: "fleeting".into(), title: "gone soon".into() }).await.unwrap();
    assert_eq!(repo.count().await.unwrap(), 1);

    tokio::time::sleep(Duration::from_millis(1500)).await;

    // A set member cannot expire with its key, so the id outlives the
    // entity; `find_all` has to treat that as deleted and clean up.
    assert!(repo.find_all().await.unwrap().is_empty(), "an expired entity was returned");
    assert_eq!(repo.count().await.unwrap(), 0, "the stale id was left in the index");
    t.stop().await.unwrap();
}
