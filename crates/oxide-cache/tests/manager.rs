//! The cache manager, both usage styles, and the failure cases.
//!
//! Two styles are supported on purpose and both are exercised here:
//!
//! * **Raw** — you build the key, you serialise the value. `get_raw` and
//!   `put_raw` take and return plain strings and never look inside them.
//! * **Typed** — `oxide_cache::get`/`put` do the JSON for you.
//!
//! Neither is privileged, and they share one store, so a value written one
//! way is readable the other way.

use oxide_boot::prelude::*;
use oxide_cache::{Backend, CacheManager, CacheProps, CacheSpec};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Note {
    id: u32,
    title: String,
}

async fn boot(yaml: &str) -> oxide_boot::Application {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        format!(
            "oxide:\n  application:\n    name: cache-test\n  server:\n    port: 0\n  management:\n    port: 0\n{yaml}"
        ),
    )
    .expect("write application.yml");
    OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot")
}

fn manager(app: &oxide_boot::Application) -> CacheManager {
    (*app.context().get::<CacheManager>().expect("CacheManager is registered")).clone()
}

// ---------------------------------------------------------------------
// The raw path: your key, your serialisation
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn raw_strings_round_trip_with_a_hand_built_key() {
    let app = boot("").await;
    let cache = manager(&app).get("sessions").expect("default backend is available");

    // A key the caller composed, and a value the caller serialised.
    let key = format!("tenant:{}:user:{}", "acme", 42);
    let payload = serde_json::to_string(&Note { id: 1, title: "mine".into() }).unwrap();

    cache.put_raw(&key, payload.clone(), None).await.unwrap();
    let read = cache.get_raw(&key).await.unwrap().expect("the entry is there");
    assert_eq!(read, payload, "the cache must not touch the bytes it was given");

    // ...and the caller parses it back itself.
    let parsed: Note = serde_json::from_str(&read).unwrap();
    assert_eq!(parsed.title, "mine");

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_raw_and_typed_paths_share_one_store() {
    let app = boot("").await;
    let cache = manager(&app).get("mixed").unwrap();

    oxide_cache::put(cache.as_ref(), "k", &Note { id: 7, title: "typed".into() }, None).await.unwrap();
    // Written typed, read raw: the raw form is the JSON the typed path
    // produced, not an opaque encoding.
    let raw = cache.get_raw("k").await.unwrap().unwrap();
    assert_eq!(raw, r#"{"id":7,"title":"typed"}"#);

    cache.put_raw("k2", r#"{"id":8,"title":"raw"}"#.into(), None).await.unwrap();
    let typed: Note = oxide_cache::get(cache.as_ref(), "k2").await.unwrap().unwrap();
    assert_eq!(typed.id, 8);

    app.stop().await.unwrap();
}

// ---------------------------------------------------------------------
// Naming and configuration
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn named_caches_are_separate_stores() {
    let app =
        boot("  cache:\n    caches:\n      images: { backend: in-memory }\n      notes: { backend: in-memory }\n")
            .await;
    let m = manager(&app);
    let images = m.get("images").unwrap();
    let notes = m.get("notes").unwrap();

    images.put_raw("same-key", "an image".into(), None).await.unwrap();
    notes.put_raw("same-key", "a note".into(), None).await.unwrap();

    assert_eq!(images.get_raw("same-key").await.unwrap().as_deref(), Some("an image"));
    assert_eq!(notes.get_raw("same-key").await.unwrap().as_deref(), Some("a note"));
    assert_eq!(m.configured_names(), vec!["images", "notes"]);

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unconfigured_name_still_works_on_the_default_backend() {
    let app = boot("").await;
    let m = manager(&app);
    // Adding `#[cacheable(cache = "new-thing")]` must not require a config
    // change first.
    let cache = m.get("never-configured").expect("created on demand");
    cache.put_raw("k", "v".into(), None).await.unwrap();
    assert_eq!(cache.get_raw("k").await.unwrap().as_deref(), Some("v"));
    // ...and asking twice returns the same store, not a fresh empty one.
    assert_eq!(m.get("never-configured").unwrap().get_raw("k").await.unwrap().as_deref(), Some("v"));
    assert!(m.configured_names().is_empty(), "an on-demand cache is not a configured one");

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_configured_ttl_expires_entries() {
    let app = boot("  cache:\n    caches:\n      short: { backend: in-memory, ttl-seconds: 1 }\n").await;
    let cache = manager(&app).get("short").unwrap();
    cache.put_raw("k", "v".into(), None).await.unwrap();
    assert_eq!(cache.get_raw("k").await.unwrap().as_deref(), Some("v"));
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(cache.get_raw("k").await.unwrap(), None, "the configured TTL was not applied");
    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn evict_and_clear_are_scoped_to_one_cache() {
    let app = boot("  cache:\n    caches:\n      a: { backend: in-memory }\n      b: { backend: in-memory }\n").await;
    let m = manager(&app);
    let (a, b) = (m.get("a").unwrap(), m.get("b").unwrap());

    a.put_raw("k", "1".into(), None).await.unwrap();
    b.put_raw("k", "2".into(), None).await.unwrap();

    assert!(a.evict("k").await.unwrap());
    assert!(!a.evict("k").await.unwrap(), "evicting twice reports false");
    assert_eq!(b.get_raw("k").await.unwrap().as_deref(), Some("2"), "evicting in 'a' emptied 'b'");

    b.put_raw("k2", "3".into(), None).await.unwrap();
    b.clear().await.unwrap();
    assert_eq!(b.get_raw("k2").await.unwrap(), None);

    app.stop().await.unwrap();
}

// ---------------------------------------------------------------------
// Negative cases
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_backend_with_no_factory_fails_the_boot_by_name() {
    // `backend: redis` without `oxide-redis` linked in. Booting anyway would
    // give a cache that never hits and nobody would notice.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: cache-test\n  server:\n    port: 0\n  management:\n    port: 0\n  cache:\n    caches:\n      notes: { backend: redis }\n",
    )
    .unwrap();
    let err = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap_err();
    let text = err.to_string();
    assert!(text.contains("notes"), "the failure does not name the cache: {text}");
    assert!(text.to_lowercase().contains("redis"), "the failure does not name the backend: {text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_value_that_is_not_valid_json_fails_the_typed_read_and_names_the_key() {
    let app = boot("").await;
    let cache = manager(&app).get("mixed").unwrap();
    cache.put_raw("corrupt", "not json".into(), None).await.unwrap();

    let err = oxide_cache::get::<Note>(cache.as_ref(), "corrupt").await.unwrap_err();
    assert!(err.to_string().contains("corrupt"), "the error does not name the key: {err}");
    // The raw path is unaffected -- the value was never meant to be JSON.
    assert_eq!(cache.get_raw("corrupt").await.unwrap().as_deref(), Some("not json"));

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn reading_a_key_that_was_never_written_is_a_miss_not_an_error() {
    let app = boot("").await;
    let cache = manager(&app).get("empty").unwrap();
    assert_eq!(cache.get_raw("nothing").await.unwrap(), None);
    assert_eq!(oxide_cache::get::<Note>(cache.as_ref(), "nothing").await.unwrap(), None);
    app.stop().await.unwrap();
}

#[test]
fn defaults_are_what_the_documentation_claims() {
    let props = CacheProps::default();
    // In-memory by default so `#[cacheable]` works before any configuration
    // exists; a shared cache is an explicit decision.
    assert_eq!(props.default_backend, Backend::InMemory);
    assert!(props.caches.is_empty());
    // A cache without a bound is a memory leak with extra steps.
    assert_eq!(CacheSpec::default().max_entries, 10_000);
    assert_eq!(CacheSpec::default().ttl(), None);
}
