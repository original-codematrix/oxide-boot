//! rest-api integration test: boots the example against a real Redpanda
//! container and an in-memory (port-0) web/management server, and proves the
//! kafka starter end to end:
//! - note creation publishes a `notes-events` record and the listener consumes
//!   it (`kafka_listener_seconds` for `notes-events` appears in the scrape);
//! - `/actuator/health` reports `components.kafka.status == "UP"`;
//! - the scrape shows `kafka_producer_send_seconds` and `kafka_listener_seconds`.
//!
//! One Redpanda container per test binary (pinned host port 9092) with atexit
//! cleanup, mirroring the oxide-kafka harness.

use oxide_boot::prelude::*;
use oxide_web::{ManagementServer, WebServer};
use rest_api::Note;
use std::collections::HashMap;

/// Shared Redpanda container on the pinned host port 9092 (auto-created topic
/// on produce), removed at process exit.
static CONTAINER: tokio::sync::OnceCell<ContainerRef> = tokio::sync::OnceCell::const_new();
static CONTAINER_ID: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

struct ContainerRef {
    _node: testcontainers::ContainerAsync<testcontainers::GenericImage>,
}

extern "C" fn cleanup_container() {
    let id = CONTAINER_ID.lock().map(|g| g.clone()).ok().flatten();
    if let Some(id) = id {
        let _ = std::process::Command::new("docker").args(["rm", "-f", &id]).output();
    }
}

async fn ensure_container() {
    CONTAINER
        .get_or_init(|| async {
            use testcontainers::core::IntoContainerPort;
            use testcontainers::runners::AsyncRunner;
            use testcontainers::ImageExt;
            let build = || {
                testcontainers::GenericImage::new("redpandadata/redpanda", "v24.2.1")
                    .with_mapped_port(9092, 9092.tcp())
                    .with_cmd([
                        "redpanda",
                        "start",
                        "--mode",
                        "dev-container",
                        "--overprovisioned",
                        "--smp",
                        "1",
                        "--memory",
                        "1G",
                        "--set",
                        "redpanda.empty_seed_starts_cluster=true",
                    ])
            };
            let mut node = None;
            for attempt in 0..8 {
                match build().start().await {
                    Ok(n) => {
                        node = Some(n);
                        break;
                    }
                    Err(_) if attempt < 7 => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
                    Err(e) => panic!("rest-api kafka test needs Docker: {e}"),
                }
            }
            let node = node.expect("container start attempted");
            *CONTAINER_ID.lock().expect("id lock") = Some(node.id().to_string());
            static REGISTERED: std::sync::Once = std::sync::Once::new();
            REGISTERED.call_once(|| {
                unsafe { libc::atexit(cleanup_container) };
            });
            ContainerRef { _node: node }
        })
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_fetch_and_kafka_flow() {
    ensure_container().await;
    let env = HashMap::from([
        ("OXIDE_SERVER_PORT".to_string(), "0".to_string()),
        ("OXIDE_MANAGEMENT_PORT".to_string(), "0".to_string()),
        ("OXIDE_KAFKA_BOOTSTRAP_SERVERS".to_string(), "localhost:9092".to_string()),
    ]);
    let app = OxideApplication::builder().base_dir(env!("CARGO_MANIFEST_DIR")).env(env).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();
    let mgmt_addr = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();
    let base = format!("http://{addr}/api/notes");
    let client = reqwest::Client::new();

    client.post(&base).json(&Note { id: 7, text: "first".into() }).send().await.unwrap();
    let fetched: Note = client.get(format!("{base}/7")).send().await.unwrap().json().await.unwrap();
    assert_eq!(fetched.text, "first");

    let missing = client.get(format!("{base}/8")).send().await.unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(missing.headers()["content-type"], "application/problem+json");

    // Health: the kafka component must be UP (broker reachable).
    let health = client.get(format!("http://{mgmt_addr}/actuator/health")).send().await.unwrap();
    let health_body = health.text().await.unwrap();
    assert!(
        health_body.contains("\"kafka\"") && health_body.contains("\"status\":\"UP\""),
        "kafka health component should be UP, got: {health_body}"
    );

    // Scrape: note creation should have produced + been consumed by the
    // listener — both producer and listener kafka metrics must appear with the
    // `notes-events` topic label.
    let scrape = async {
        for _ in 0..40 {
            let s =
                client.get(format!("http://{mgmt_addr}/actuator/prometheus")).send().await.unwrap().text().await.unwrap();
            if s.contains("kafka_producer_send_seconds") && s.contains("kafka_listener_seconds") {
                return s;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        client.get(format!("http://{mgmt_addr}/actuator/prometheus")).send().await.unwrap().text().await.unwrap()
    }
    .await;
    assert!(scrape.contains("kafka_producer_send_seconds"), "producer send metric missing: {scrape}");
    assert!(scrape.contains("topic=\"notes-events\""), "notes-events topic label missing: {scrape}");
    assert!(scrape.contains("kafka_listener_seconds"), "listener metric missing: {scrape}");

    app.stop().await.unwrap();
}