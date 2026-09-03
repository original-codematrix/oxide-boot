//! `oxide.management.health.show-details` gates whether a `DOWN` indicator's
//! `detail` string is echoed in `/actuator/health` (default `"never"`, like
//! Spring Boot's Actuator) — a Mongo/Kafka driver error can contain hosts or
//! connection strings, which must not leak on an unauthenticated endpoint.

use oxide_boot::prelude::*;
use oxide_web::{HealthIndicator, HealthStatus, ManagementServer};
use std::collections::HashMap;

static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: web-test\n  server:\n    port: 0\n  management:\n    port: 0\n",
    )
    .unwrap();
    dir
}

inventory::submit! {
    HealthIndicator {
        name: "demo",
        register: |_ctx| {
            Some(Box::new(|| {
                Box::pin(async { HealthStatus::Down { detail: "mongodb://user:pass@10.0.0.5:27017 unreachable".to_string() } })
            }))
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn details_are_hidden_by_default() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let mgmt_addr = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();

    let body: serde_json::Value =
        reqwest::get(format!("http://{mgmt_addr}/actuator/health")).await.unwrap().json().await.unwrap();

    assert_eq!(body["components"]["demo"]["status"], "DOWN");
    assert!(body["components"]["demo"].get("detail").is_none(), "detail leaked: {body}");

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn details_are_shown_when_configured() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let mut env = HashMap::new();
    env.insert("OXIDE_MANAGEMENT_HEALTH_SHOW_DETAILS".to_string(), "always".to_string());
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.unwrap();
    let mgmt_addr = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();

    let body: serde_json::Value =
        reqwest::get(format!("http://{mgmt_addr}/actuator/health")).await.unwrap().json().await.unwrap();

    assert_eq!(body["components"]["demo"]["status"], "DOWN");
    assert!(body["components"]["demo"]["detail"].is_string());

    app.stop().await.unwrap();
}
