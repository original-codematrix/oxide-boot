//! End-to-end proof for `examples/kafka`: boots the app against a real
//! Redpanda container on port 9092. On boot `OrderProducer` publishes an
//! `Order` to `orders`; an observer listener (registered in this test, same
//! topic, independent group) counts the received record so we can assert
//! produce→consume works in one go.

use kafka::Order;
use oxide_boot::OxideApplication;
use oxide_kafka::kafka_listeners;
use rdkafka::consumer::Consumer;
use std::collections::HashMap;
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

static RECEIVED: AtomicUsize = AtomicUsize::new(0);

#[oxide_boot::component]
struct ObservingListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl ObservingListener {
    #[kafka_listener(topic = "orders", group_id = "example-test")]
    async fn on_order(&self, _order: Order) -> Result<(), String> {
        RECEIVED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// Shared Redpanda container on the pinned host port 9092, removed at exit.
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

async fn ensure_broker_ready() {
    CONTAINER
        .get_or_init(|| async {
            let build = || {
                GenericImage::new("redpandadata/redpanda", "v24.2.1").with_mapped_port(9092, 9092.tcp()).with_cmd([
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
                    Err(e) => panic!("kafka example test needs Docker: {e}"),
                }
            }
            let node = node.expect("node");
            *CONTAINER_ID.lock().expect("lock") = Some(node.id().to_string());
            static R: Once = Once::new();
            R.call_once(|| {
                unsafe { libc::atexit(cleanup_container) };
            });
            // Wait until the broker is ready at the KAFKA protocol level.
            tokio::time::timeout(std::time::Duration::from_secs(60), async {
                loop {
                    let mut config = rdkafka::ClientConfig::new();
                    config.set("bootstrap.servers", "localhost:9092");
                    if let Ok(c) = config.create::<rdkafka::consumer::BaseConsumer>()
                        && c.fetch_metadata(None, std::time::Duration::from_secs(1)).is_ok()
                    {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            })
            .await
            .expect("redpanda became ready");
            ContainerRef { _node: node }
        })
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_publishes_and_listener_consumes() {
    ensure_broker_ready().await;

    let dir = std::env::temp_dir().join(format!("kafka-example-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("application.yml"),
        "oxide:\n  application:\n    name: kafka-example-test\n  kafka:\n    bootstrap-servers: \"localhost:9092\"\n",
    )
    .unwrap();

    let app = OxideApplication::builder().base_dir(&dir).env(HashMap::new()).start().await.expect("boot kafka example");

    // OrderProducer.post_construct sent the Order successfully (boot would have
    // errored otherwise); wait for the observer listener to consume it.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline && RECEIVED.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let received = RECEIVED.load(Ordering::SeqCst);
    assert!(received >= 1, "the order published on boot should be consumed, got {received}");

    app.stop().await.expect("stop");
    let _ = std::fs::remove_dir_all(&dir);
}
