//! Custom bucket bounds, and the empty-list trap.
//!
//! Its own binary: bucket bounds are baked into the recorder when it is
//! installed, so this cannot share a process with another boot.

// Forces this crate to be linked into the test binary. Nothing below names
// an item from it, and without the reference the linker drops it -- taking
// the `inventory` registrations for the router layer and the
// `/actuator/prometheus` route with it, so the scrape 404s.
#[allow(unused_imports)]
use oxide_observability as _;

use oxide_boot::prelude::*;
use oxide_web::ManagementServer;
use std::collections::HashMap;

#[tokio::test]
async fn custom_buckets_replace_the_defaults() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: http-metrics-buckets\n  server:\n    port: 0\n  management:\n    port: 0\n    endpoints:\n      exposure: \"health,info,prometheus\"\n  metrics:\n    http:\n      buckets: [0.01, 0.1, 1.0]\n",
    )
    .expect("write application.yml");

    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot");
    let main = app.context().get::<oxide_web::WebServer>().expect("web server").local_addr().expect("addr");
    let mgmt = app.context().get::<ManagementServer>().expect("management server").local_addr().expect("addr");

    let _ = reqwest::get(format!("http://{main}/anything")).await;
    let body = reqwest::get(format!("http://{mgmt}/actuator/prometheus")).await.unwrap().text().await.unwrap();

    let buckets: Vec<&str> = body.lines().filter(|l| l.starts_with("http_server_requests_seconds_bucket")).collect();
    assert!(!buckets.is_empty(), "no histogram at all:\n{body}");

    // Three configured bounds plus the implicit +Inf: four series per triple
    // instead of twelve.
    let le_values: std::collections::BTreeSet<&str> =
        buckets.iter().filter_map(|l| l.split("le=\"").nth(1)).filter_map(|rest| rest.split('"').next()).collect();
    assert_eq!(le_values.len(), 4, "expected 3 bounds + Inf, got: {le_values:?}");
    assert!(le_values.contains("0.1"), "configured bound missing: {le_values:?}");
    assert!(!le_values.contains("0.005"), "a default bound survived the override: {le_values:?}");

    app.stop().await.expect("stop");
}
