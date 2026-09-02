//! Redpanda test harness shared by the oxide-kafka integration-test binaries.
//!
//! Each test binary starts a single shared Redpanda container (testcontainers
//! is expensive), cached in a process-wide `OnceCell`. Call the async
//! `bootstrap_servers()` (or `kafka_props()`) from within a tokio runtime to
//! trigger the one-time container start. Tests in a binary serialize on the
//! established BOOT_LOCK discipline; topics are unique per test
//! (`{test}-{nanos}`) so binaries are repeatable.

#![allow(dead_code)]

use oxide_kafka::{ConsumerProps, KafkaProps, ProducerProps, RetryProps};
use rdkafka::consumer::Consumer;
use std::time::Duration;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::sync::OnceCell;

/// Shared between the harness and the atexit cleanup closure. The container
/// lives in a `'static OnceCell` which Rust never drops, so removal must be
/// triggered explicitly at process exit.
static CONTAINER_ID_CELL: std::sync::OnceLock<std::sync::Mutex<Option<String>>> = std::sync::OnceLock::new();

fn container_id_holder() -> &'static std::sync::Mutex<Option<String>> {
    CONTAINER_ID_CELL.get_or_init(|| std::sync::Mutex::new(None))
}

extern "C" fn cleanup_container() {
    let id = container_id_holder().lock().ok().and_then(|g| g.clone());
    if let Some(id) = id {
        let _ = std::process::Command::new("docker").args(["rm", "-f", &id]).output();
    }
}

fn register_container_cleanup(id: String) {
    *container_id_holder().lock().expect("id lock") = Some(id);
    // Register the atexit handler exactly once, so the container is removed
    // (via the docker CLI, which needs no runtime) when the test process exits.
    static REGISTERED: std::sync::Once = std::sync::Once::new();
    REGISTERED.call_once(|| unsafe {
        libc::atexit(cleanup_container);
    });
}

/// A single shared Redpanda container, started lazily on first use.
static CONTAINER: OnceCell<ContainerRef> = OnceCell::const_new();

/// Owns the running container plus its port mapping.
struct ContainerRef {
    _container: testcontainers::ContainerAsync<GenericImage>,
    bootstrap_servers: String,
}

async fn container() -> &'static ContainerRef {
    CONTAINER
        .get_or_init(|| async {
            // 9092 is the client (kafka) listener. Redpanda `dev-container`
            // mode advertises `localhost:9092`, so bind the host port 9092 to
            // the container's 9092 — otherwise the advertised address is not
            // reachable from the host and non-trivial clients (producers after
            // reconnecting to the advertised broker) fail on the "connection
            // refused" path. This is the standard single-test-container setup.
            let build_image = || {
                GenericImage::new("redpandadata/redpanda", "v24.2.1")
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
            // Retry container start a few times: each test binary runs on the
            // pinned host port 9092, and a preceding binary's container may
            // still be holding that port while teardown finishes. A short
            // backoff lets the port-freeing complete instead of failing the
            // whole integration suite on a race.
            let mut node = None;
            for attempt in 0..8 {
                match build_image().start().await {
                    Ok(n) => {
                        node = Some(n);
                        break;
                    }
                    Err(e) if attempt < 7 => {
                        eprintln!("[harness] container start attempt {attempt} failed ({e}); retrying");
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                    Err(e) => {
                        panic!(
                            "Kafka integration tests require Docker — start Docker Desktop, or skip these tests: {e}"
                        )
                    }
                }
            }
            let node = node.expect("container start attempted at least once");
            let id = node.id().to_string();
            register_container_cleanup(id);
            let port = node.get_host_port_ipv4(9092.tcp()).await.expect("query mapped port");
            let address = format!("127.0.0.1:{port}");
            eprintln!("[harness] redpanda bootstrap = {address}; waiting for broker readiness");
            // Wait until the broker is actually ready at the KAFKA protocol
            // level (a TCP-accept alone isn't enough: Redpanda can accept the
            // connection before it can handle produces). A metadata fetch is
            // the smoke test we already rely on, so reuse it as the probe.
            let ready = tokio::time::timeout(std::time::Duration::from_secs(60), async {
                loop {
                    let mut config = rdkafka::ClientConfig::new();
                    config.set("bootstrap.servers", address.as_str());
                    if let Ok(consumer) = config.create::<rdkafka::consumer::BaseConsumer>() {
                        if consumer.fetch_metadata(None, std::time::Duration::from_secs(1)).is_ok() {
                            break;
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            })
            .await;
            if ready.is_err() {
                panic!("redpanda broker at {address} did not become ready within 60s");
            }
            eprintln!("[harness] redpanda broker ready");
            ContainerRef { _container: node, bootstrap_servers: address }
        })
        .await
}

/// The Redpanda `bootstrap.servers` string for the running shared container.
/// Must be awaited from within a tokio runtime.
pub async fn bootstrap_servers() -> &'static str {
    &container().await.bootstrap_servers
}

/// A topic name unique to the given test/prefix.
pub fn unique_topic(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// Builds a `KafkaProps` pointing at the shared Redpanda container, with a
/// small retry backoff so integration tests stay fast. Must be awaited from
/// within a tokio runtime (it triggers the container start).
pub async fn kafka_props() -> KafkaProps {
    let bootstrap = bootstrap_servers().await.to_string();
    let retry = RetryProps { backoff_initial_ms: 50, ..Default::default() };
    KafkaProps {
        bootstrap_servers: bootstrap,
        consumer: ConsumerProps { retry, ..Default::default() },
        producer: ProducerProps::default(),
        properties: [("message.timeout.ms".to_string(), "10000".to_string())].into_iter().collect(),
    }
}

/// Sleeps `ms` before continuing (keeps tests from busy-looping polls).
pub fn wait(ms: u64) {
    std::thread::sleep(Duration::from_millis(ms));
}