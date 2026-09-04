//! The isolation these fixtures exist to provide.
//!
//! These tests run concurrently against one shared Dragonfly, by default,
//! and deliberately use the same key names as each other. Without a
//! per-test prefix they would interfere; with one they cannot.

use oxide_redis_test::{RedisTest, unique_prefix};

#[tokio::test(flavor = "multi_thread")]
async fn one_test_writes_a_shared_key_name() {
    let t = RedisTest::start().await;
    // The same key name as the next test, on purpose.
    t.template().set("contended", "from-first", None).await.unwrap();
    // Long enough that the other test is certainly running by now.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        t.template().get("contended").await.unwrap().as_deref(),
        Some("from-first"),
        "another test overwrote this key — the per-test prefix is not isolating"
    );
    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_other_test_writes_the_same_key_name() {
    let t = RedisTest::start().await;
    t.template().set("contended", "from-second", None).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(t.template().get("contended").await.unwrap().as_deref(), Some("from-second"));
    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn each_fixture_gets_its_own_prefix() {
    let a = RedisTest::start().await;
    let b = RedisTest::start().await;
    assert_ne!(a.prefix(), b.prefix());

    a.template().set("same-name", "a", None).await.unwrap();
    b.template().set("same-name", "b", None).await.unwrap();
    assert_eq!(a.template().get("same-name").await.unwrap().as_deref(), Some("a"));
    assert_eq!(b.template().get("same-name").await.unwrap().as_deref(), Some("b"));

    a.stop().await.unwrap();
    b.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn extra_configuration_reaches_the_application() {
    // The prefix stays this fixture's own even when the caller adds config.
    let t = RedisTest::builder().env("OXIDE_REDIS_COMMAND_TIMEOUT_MILLIS", "5000").start().await;
    assert!(t.prefix().starts_with("t-"));
    t.template().set("configured", "yes", None).await.unwrap();
    assert_eq!(t.template().get("configured").await.unwrap().as_deref(), Some("yes"));
    t.stop().await.unwrap();
}

#[test]
fn prefixes_do_not_repeat() {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..1_000 {
        assert!(seen.insert(unique_prefix()), "a prefix repeated within one process");
    }
}
