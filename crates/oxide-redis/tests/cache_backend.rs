//! Redis as a cache backend, and `#[cacheable]` end to end.

use oxide_boot::{OxideApplication, component};
use oxide_cache::{CacheManager, cacheable};
use oxide_redis_test::{dragonfly, unique_prefix};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Note {
    id: String,
    title: String,
}

/// Counts how often the expensive path actually ran, per key.
///
/// Per key, not a single total: these tests run concurrently in one binary,
/// and a shared counter would be incremented by whichever other test happens
/// to be running -- which is exactly what a first version of this file did,
/// producing "the body ran 7 times" for a test that called it twice.
static LOADS: std::sync::Mutex<Option<HashMap<String, usize>>> = std::sync::Mutex::new(None);

fn record_load(id: &str) {
    let mut guard = LOADS.lock().unwrap_or_else(|p| p.into_inner());
    *guard.get_or_insert_with(HashMap::new).entry(id.to_string()).or_insert(0) += 1;
}

fn loads_for(id: &str) -> usize {
    LOADS.lock().unwrap_or_else(|p| p.into_inner()).as_ref().and_then(|m| m.get(id)).copied().unwrap_or(0)
}

#[component]
pub struct NoteLoader {
    caches: CacheManager,
}

impl NoteLoader {
    #[cacheable(cache = "notes", key = "id")]
    pub async fn load(&self, id: &str) -> Result<Option<Note>, String> {
        record_load(id);
        Ok(match id {
            "missing" => None,
            _ => Some(Note { id: id.to_string(), title: format!("note {id}") }),
        })
    }

    #[cacheable(cache = "notes", key = "format!(\"{tenant}:{id}\")")]
    pub async fn load_for_tenant(&self, tenant: &str, id: &str) -> Result<Option<Note>, String> {
        record_load(&format!("{tenant}:{id}"));
        Ok(Some(Note { id: id.to_string(), title: format!("{tenant}/{id}") }))
    }

    #[cacheable(cache = "notes", key = "id")]
    pub async fn always_fails(&self, id: &str) -> Result<Option<Note>, String> {
        record_load(id);
        Err(format!("upstream is down for {id}"))
    }
}

async fn boot(backend: &str) -> oxide_boot::Application {
    let url = dragonfly().await.endpoint;
    let prefix = unique_prefix();
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        format!(
            "oxide:\n  application:\n    name: cache-backend-test\n  server:\n    port: 0\n  management:\n    port: 0\n  redis:\n    url: \"{url}\"\n    key-prefix: \"{prefix}\"\n  cache:\n    caches:\n      notes: {{ backend: {backend} }}\n"
        ),
    )
    .expect("write application.yml");
    OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot")
}

fn loader(app: &oxide_boot::Application) -> Arc<NoteLoader> {
    app.context().get::<NoteLoader>().expect("NoteLoader").into_arc()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_second_call_does_not_run_the_body() {
    let app = boot("redis").await;
    let l = loader(&app);
    let first = l.load("a1").await.unwrap().unwrap();
    let second = l.load("a1").await.unwrap().unwrap();

    assert_eq!(first, second);
    assert_eq!(loads_for("a1"), 1, "the body ran more than once; the cache did not hit");
    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_successful_none_is_cached_too() {
    let app = boot("redis").await;
    let l = loader(&app);
    assert_eq!(l.load("missing").await.unwrap(), None);
    assert_eq!(l.load("missing").await.unwrap(), None);

    // "This does not exist" is a real answer. Not caching it is what lets a
    // storm of lookups for one missing key reach the database every time.
    assert_eq!(loads_for("missing"), 1, "a cached None was recomputed");
    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_error_is_never_cached() {
    let app = boot("redis").await;
    let l = loader(&app);
    assert!(l.always_fails("boom").await.is_err());
    assert!(l.always_fails("boom").await.is_err());

    // Caching a failure turns a transient outage into a sticky one for the
    // length of the TTL, and the caller cannot tell the difference.
    assert_eq!(loads_for("boom"), 2, "an Err was served from the cache");
    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_composed_key_separates_tenants() {
    let app = boot("redis").await;
    let l = loader(&app);

    let acme = l.load_for_tenant("acme", "1").await.unwrap().unwrap();
    let other = l.load_for_tenant("other", "1").await.unwrap().unwrap();

    // Same id, different tenant: a key built only from `id` would have
    // served acme's note to the other tenant.
    assert_eq!(acme.title, "acme/1");
    assert_eq!(other.title, "other/1");
    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_in_memory_backend_behaves_the_same() {
    // The same assertions against the other backend: switching a cache's
    // storage must be a configuration change and nothing else.
    let app = boot("in-memory").await;
    let l = loader(&app);
    let first = l.load("m1").await.unwrap().unwrap();
    let second = l.load("m1").await.unwrap().unwrap();
    assert_eq!(first, second);
    assert_eq!(loads_for("m1"), 1);
    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cache_is_reachable_directly_for_raw_values() {
    let app = boot("redis").await;
    let m = (*app.context().get::<CacheManager>().unwrap()).clone();
    let cache = m.get("notes").expect("the redis backend is registered");

    // The same named cache the macro uses, addressed by hand: your key,
    // your bytes.
    cache.put_raw("hand-written", "{\"anything\":true}".into(), None).await.unwrap();
    assert_eq!(cache.get_raw("hand-written").await.unwrap().as_deref(), Some("{\"anything\":true}"));

    app.stop().await.unwrap();
}
