//! Bounding what the request metrics cost.
//!
//! A latency histogram is the most expensive thing a framework emits by
//! default: fourteen series per method/route/status triple (twelve buckets
//! plus `_sum` and `_count`) where a counter costs one. Twenty routes across
//! the usual status codes reach four figures, and nobody chose that.
//!
//! Each test runs in its own binary because the Prometheus recorder is
//! process-global *and* its bucket configuration is baked in when it is
//! installed — a second boot in the same process reuses the first one's
//! recorder, so a config assertion there would be checking the previous
//! test's settings.

// Forces this crate to be linked into the test binary. Nothing below names
// an item from it, and without the reference the linker drops it -- taking
// the `inventory` registrations for the router layer and the
// `/actuator/prometheus` route with it, so the scrape 404s.
#[allow(unused_imports)]
use oxide_observability as _;

use oxide_boot::prelude::*;
use oxide_web::ManagementServer;
use std::collections::HashMap;

async fn scrape_with(yaml: &str) -> (oxide_boot::Application, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        format!(
            "oxide:\n  application:\n    name: http-metrics-test\n  server:\n    port: 0\n  management:\n    port: 0\n    endpoints:\n      exposure: \"health,info,prometheus\"\n{yaml}"
        ),
    )
    .expect("write application.yml");

    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot");
    let main = app.context().get::<oxide_web::WebServer>().expect("web server").local_addr().expect("addr");
    let mgmt = app.context().get::<ManagementServer>().expect("management server").local_addr().expect("addr");

    // Any path will do: an unmatched request is still measured, recorded
    // under `uri="UNMATCHED"` so a scanner cannot inflate label cardinality.
    let _ = reqwest::get(format!("http://{main}/anything")).await;
    let body = reqwest::get(format!("http://{mgmt}/actuator/prometheus"))
        .await
        .expect("scrape")
        .text()
        .await
        .expect("scrape body");
    (app, body)
}

fn count_lines(body: &str, prefix: &str) -> usize {
    body.lines().filter(|l| l.starts_with(prefix)).count()
}

#[tokio::test]
async fn the_counter_is_always_there_and_the_histogram_is_on_by_default() {
    let (app, body) = scrape_with("").await;

    // The counter carries rate, throughput and error ratio at one series per
    // method/route/status.
    assert!(count_lines(&body, "http_server_requests_total") >= 1, "no request counter:\n{body}");
    // Default keeps latency available.
    assert!(count_lines(&body, "http_server_requests_seconds_bucket") > 0, "no histogram by default:\n{body}");

    app.stop().await.expect("stop");
}
