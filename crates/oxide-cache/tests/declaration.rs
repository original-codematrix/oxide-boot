//! Caches declared in code, configured (optionally) in YAML.
//!
//! The name already lives in the code — `#[cacheable(cache = "notes")]` is
//! where it is written and where it is referenced. Repeating it in
//! `application.yml` only to attach a TTL is duplication, and a deployment
//! that forgets the YAML silently gets framework defaults rather than the
//! ones the code intended.
//!
//! So the declaration carries the defaults, and configuration overrides
//! them. Note that no test here writes `inventory::submit!`: the
//! registration mechanism is hidden behind `declare_cache!`, the same way
//! `#[component]` hides it.

#[allow(unused_imports)]
use oxide_observability as _;

use oxide_boot::prelude::*;
use oxide_cache::{Backend, CacheManager};
use std::collections::HashMap;
use std::time::Duration;

oxide_cache::declare_cache!("plain");
oxide_cache::declare_cache!("short-lived", ttl_seconds = 1);
oxide_cache::declare_cache!("tuned", backend = Backend::InMemory, ttl_seconds = 3600, max_entries = 64);

fn bare(dir: &std::path::Path) {
    std::fs::write(
        dir.join("application.yml"),
        "oxide:\n  application:\n    name: declared\n  server:\n    port: 0\n  management:\n    port: 0\n",
    )
    .expect("write application.yml");
}

async fn boot(dir: &std::path::Path, env: HashMap<String, String>) -> oxide_boot::Application {
    OxideApplication::builder().base_dir(dir).env(env).start().await.expect("boot")
}

fn manager(app: &oxide_boot::Application) -> CacheManager {
    (*app.context().get::<CacheManager>().expect("CacheManager")).clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn declared_caches_exist_without_any_yaml() {
    let dir = tempfile::tempdir().unwrap();
    bare(dir.path());
    let app = boot(dir.path(), HashMap::new()).await;
    let m = manager(&app);

    // All three, from code alone.
    assert_eq!(m.configured_names(), vec!["plain", "short-lived", "tuned"]);
    let plain = m.get("plain").expect("declared cache");
    plain.put_raw("k", "v".into(), None).await.unwrap();
    assert_eq!(plain.get_raw("k").await.unwrap().as_deref(), Some("v"));

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declared_ttl_applies_without_configuration() {
    let dir = tempfile::tempdir().unwrap();
    bare(dir.path());
    let app = boot(dir.path(), HashMap::new()).await;
    let cache = manager(&app).get("short-lived").unwrap();

    cache.put_raw("k", "v".into(), None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(cache.get_raw("k").await.unwrap(), None, "the declared ttl_seconds = 1 was not applied");

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn configuration_overrides_a_declaration() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: declared\n  server:\n    port: 0\n  management:\n    port: 0\n  cache:\n    caches:\n      tuned:\n        ttl-seconds: 1\n",
    )
    .unwrap();
    let app = boot(dir.path(), HashMap::new()).await;
    let cache = manager(&app).get("tuned").unwrap();

    // The declaration says 3600s; the deployment says 1.
    cache.put_raw("k", "v".into(), None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(cache.get_raw("k").await.unwrap(), None, "the declaration beat the configuration");

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn overriding_one_field_keeps_the_rest_of_the_declaration() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: declared\n  server:\n    port: 0\n  management:\n    port: 0\n  cache:\n    caches:\n      short-lived:\n        max-entries: 5\n",
    )
    .unwrap();
    let app = boot(dir.path(), HashMap::new()).await;
    let cache = manager(&app).get("short-lived").unwrap();

    // Only `max-entries` was configured. A merge that replaced the whole
    // spec would have reset the declared TTL to "no expiry", and this entry
    // would still be there.
    cache.put_raw("k", "v".into(), None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        cache.get_raw("k").await.unwrap(),
        None,
        "configuring max-entries silently dropped the declared ttl_seconds"
    );

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_environment_still_wins_over_a_declaration() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: declared\n  server:\n    port: 0\n  management:\n    port: 0\n  cache:\n    caches:\n      tuned:\n        ttl-seconds: 3600\n",
    )
    .unwrap();
    let env = HashMap::from([("OXIDE_CACHE_CACHES_TUNED_TTL_SECONDS".to_string(), "1".to_string())]);
    let app = boot(dir.path(), env).await;
    let cache = manager(&app).get("tuned").unwrap();

    cache.put_raw("k", "v".into(), None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(cache.get_raw("k").await.unwrap(), None, "the environment did not win");

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_undeclared_name_is_still_served_on_demand() {
    let dir = tempfile::tempdir().unwrap();
    bare(dir.path());
    let app = boot(dir.path(), HashMap::new()).await;
    let m = manager(&app);

    // Adding `#[cacheable(cache = "ad-hoc")]` must work before anyone
    // remembers to declare it -- it just gets framework defaults.
    let cache = m.get("ad-hoc").expect("created on demand");
    cache.put_raw("k", "v".into(), None).await.unwrap();
    assert_eq!(cache.get_raw("k").await.unwrap().as_deref(), Some("v"));
    assert!(!m.configured_names().contains(&"ad-hoc"), "an on-demand cache is not a declared one");

    app.stop().await.unwrap();
}
