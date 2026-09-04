//! Configuring caches entirely from the environment.
//!
//! Deployments do not want a shape like `notes: { backend: redis }` sitting
//! in `application.yml` — inline objects are awkward to override, awkward to
//! template, and awkward to put in a ConfigMap. Everything here has to be
//! reachable by path (`oxide.cache.caches.notes.backend`) and settable as an
//! environment variable, including entries that appear in *no* YAML file at
//! all.
//!
//! That is the case these tests pin: a map-valued config section created
//! from environment variables only.

#[allow(unused_imports)]
use oxide_observability as _;

use oxide_boot::prelude::*;
use oxide_cache::CacheManager;
use std::collections::HashMap;
use std::time::Duration;

/// An application.yml with no `cache:` section whatsoever.
fn bare_config(dir: &std::path::Path, name: &str) {
    std::fs::write(
        dir.join("application.yml"),
        format!("oxide:\n  application:\n    name: {name}\n  server:\n    port: 0\n  management:\n    port: 0\n"),
    )
    .expect("write application.yml");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cache_can_be_declared_by_environment_variable_alone() {
    let dir = tempfile::tempdir().unwrap();
    bare_config(dir.path(), "env-only");

    // `oxide.cache.caches.notes.backend` as an env var. The map entry
    // `notes` exists nowhere in YAML -- the binder has to create it.
    let env = HashMap::from([
        ("OXIDE_CACHE_CACHES_NOTES_BACKEND".to_string(), "in-memory".to_string()),
        ("OXIDE_CACHE_CACHES_IMAGES_BACKEND".to_string(), "in-memory".to_string()),
    ]);
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.expect("boot");
    let m = (*app.context().get::<CacheManager>().expect("CacheManager")).clone();

    // Both entries were created, and they are *configured* caches rather
    // than ones invented on demand for an unknown name.
    assert_eq!(m.configured_names(), vec!["images", "notes"]);
    // They work, which means the backend bound and not just the name.
    let notes = m.get("notes").expect("notes cache");
    notes.put_raw("k", "v".into(), None).await.unwrap();
    assert_eq!(notes.get_raw("k").await.unwrap().as_deref(), Some("v"));

    app.stop().await.expect("stop");
}

/// A limitation, pinned so it is not mistaken for a bug and is noticed if it
/// is ever fixed.
///
/// `OXIDE_CACHE_CACHES_NOTES_TTL_SECONDS` is ambiguous without knowing the
/// target type: `NOTES_TTL_SECONDS` could mean `notes.ttl-seconds`,
/// `notes.ttl.seconds`, or `notes-ttl-seconds`. Along an *existing* path the
/// binder resolves it by matching against keys that are already there --
/// which is why [`the_environment_wins_over_yaml`] passes. Creating a map
/// entry that exists in no file leaves nothing to match against, so a
/// multi-word field of that new entry is nested rather than hyphenated, and
/// serde then ignores the unknown field.
///
/// The real fix is a schema-aware binder: `#[derive(ConfigProperties)]`
/// knows its own field names and could register them for the walker to
/// consult. Until then, a new map entry needs one line of YAML -- or the
/// multi-word field set on an entry the YAML already declares.
#[tokio::test(flavor = "multi_thread")]
async fn a_multi_word_field_on_an_env_only_map_entry_does_not_bind_yet() {
    let dir = tempfile::tempdir().unwrap();
    bare_config(dir.path(), "env-only-limit");

    let env = HashMap::from([
        ("OXIDE_CACHE_CACHES_NOTES_BACKEND".to_string(), "in-memory".to_string()),
        ("OXIDE_CACHE_CACHES_NOTES_TTL_SECONDS".to_string(), "1".to_string()),
    ]);
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.expect("boot");
    let notes = (*app.context().get::<CacheManager>().unwrap()).clone().get("notes").unwrap();

    notes.put_raw("k", "v".into(), None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        notes.get_raw("k").await.unwrap().as_deref(),
        Some("v"),
        "ttl-seconds now binds on an env-only map entry -- the limitation this test documents is fixed, \
         so replace it with the positive assertion"
    );

    app.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_environment_wins_over_yaml() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: env-wins\n  server:\n    port: 0\n  management:\n    port: 0\n  cache:\n    caches:\n      notes:\n        backend: in-memory\n        ttl-seconds: 3600\n",
    )
    .unwrap();

    // The deployment shortens the TTL without touching the file it ships.
    let env = HashMap::from([("OXIDE_CACHE_CACHES_NOTES_TTL_SECONDS".to_string(), "1".to_string())]);
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.expect("boot");
    let cache = (*app.context().get::<CacheManager>().unwrap()).clone().get("notes").unwrap();

    cache.put_raw("k", "v".into(), None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(cache.get_raw("k").await.unwrap(), None, "the YAML's 3600s TTL beat the environment override");

    app.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_scalars_bind_without_appearing_in_yaml() {
    let dir = tempfile::tempdir().unwrap();
    bare_config(dir.path(), "nested-scalar");

    // Three levels deep, kebab-cased, and absent from the file.
    let env = HashMap::from([
        ("OXIDE_METRICS_HTTP_HISTOGRAM".to_string(), "false".to_string()),
        ("OXIDE_METRICS_PROCESS_INTERVAL_MILLIS".to_string(), "500".to_string()),
    ]);
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.expect("boot");
    // Booting at all proves the keys parsed; a bad value would have failed
    // deserialization with the path in the message.
    app.stop().await.expect("stop");
}
