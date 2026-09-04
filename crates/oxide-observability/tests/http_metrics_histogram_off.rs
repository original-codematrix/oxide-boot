//! `histogram: false` — the counter survives, the buckets do not.
//!
//! Its own binary: the Prometheus recorder is process-global, so a boot that
//! disabled the histogram would still see a recorder another test installed.

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
async fn disabling_the_histogram_keeps_rate_and_errors_available() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: http-metrics-off\n  server:\n    port: 0\n  management:\n    port: 0\n    endpoints:\n      exposure: \"health,info,prometheus\"\n  metrics:\n    http:\n      histogram: false\n",
    )
    .expect("write application.yml");

    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot");
    let main = app.context().get::<oxide_web::WebServer>().expect("web server").local_addr().expect("addr");
    let mgmt = app.context().get::<ManagementServer>().expect("management server").local_addr().expect("addr");

    let _ = reqwest::get(format!("http://{main}/anything")).await;
    let body = reqwest::get(format!("http://{mgmt}/actuator/prometheus")).await.unwrap().text().await.unwrap();

    // Fourteen series per triple become one, and everything a rate or an
    // error-ratio panel needs is still there.
    assert!(
        body.lines().any(|l| l.starts_with("http_server_requests_total")),
        "the counter went missing with the histogram:\n{body}"
    );
    assert!(
        !body.lines().any(|l| l.starts_with("http_server_requests_seconds_bucket")),
        "buckets were emitted despite `histogram: false`:\n{body}"
    );

    app.stop().await.expect("stop");
}
