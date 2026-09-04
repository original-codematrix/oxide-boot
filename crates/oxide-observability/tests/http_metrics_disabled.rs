//! `enabled: false` — no request metrics at all.
//!
//! Its own binary, for the same reason as the sibling tests: the recorder is
//! process-global.

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
async fn disabling_request_metrics_emits_neither_counter_nor_histogram() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: http-metrics-disabled\n  server:\n    port: 0\n  management:\n    port: 0\n    endpoints:\n      exposure: \"health,info,prometheus\"\n  metrics:\n    http:\n      enabled: false\n",
    )
    .expect("write application.yml");

    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot");
    let main = app.context().get::<oxide_web::WebServer>().expect("web server").local_addr().expect("addr");
    let mgmt = app.context().get::<ManagementServer>().expect("management server").local_addr().expect("addr");

    let _ = reqwest::get(format!("http://{main}/anything")).await;
    let body = reqwest::get(format!("http://{mgmt}/actuator/prometheus")).await.unwrap().text().await.unwrap();

    assert!(!body.contains("http_server_requests_total"), "request counter emitted despite `enabled: false`");
    assert!(!body.contains("http_server_requests_seconds"), "request histogram emitted despite `enabled: false`");
    // The endpoint itself still works, and process metrics are unaffected --
    // turning off *request* metrics must not turn off the system ones.
    assert!(body.contains("process_resident_memory_bytes"), "process metrics were switched off too:\n{body}");

    app.stop().await.expect("stop");
}
