//! `oxide.metrics.process.enabled = false` must leave the process gauges
//! entirely unregistered.
//!
//! This lives in its own test binary on purpose. The Prometheus recorder is
//! process-global, so once *any* test in a binary boots an app with the
//! sampler on, `process_resident_memory_bytes` is registered for the rest of
//! that process and a later "it is absent" assertion cannot distinguish
//! "never registered" from "registered by the test before me". Its own binary
//! means its own recorder.

use oxide_boot::prelude::*;
use std::collections::HashMap;

#[tokio::test]
async fn disabled_sampler_registers_no_process_gauges() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: process-metrics-disabled\n  server:\n    port: 0\n  management:\n    port: 0\n",
    )
    .unwrap();

    let env = HashMap::from([
        ("OXIDE_MANAGEMENT_ENDPOINTS_EXPOSURE".to_string(), "health,info,prometheus".to_string()),
        ("OXIDE_METRICS_PROCESS_ENABLED".to_string(), "false".to_string()),
        // Short enough that a sampler which ignored `enabled` would have run
        // several times before the scrape below, rather than merely not having
        // reached its first tick yet.
        ("OXIDE_METRICS_PROCESS_INTERVAL_MILLIS".to_string(), "100".to_string()),
    ]);
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.unwrap();
    let mgmt = app.context().get::<oxide_web::ManagementServer>().unwrap().local_addr().unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let scrape = reqwest::get(format!("http://{mgmt}/actuator/prometheus")).await.unwrap().text().await.unwrap();

    for metric in ["process_resident_memory_bytes", "process_virtual_memory_bytes", "process_cpu_seconds_total"] {
        assert!(!scrape.contains(metric), "{metric} published despite enabled=false; scrape: {scrape}");
    }

    app.stop().await.unwrap();
}
