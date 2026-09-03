//! End-to-end proof for `examples/kafka`: boots the app against a real,
//! shared Redpanda container. On boot `OrderProducer` publishes an
//! `Order` to `orders`; an observer listener (registered in this test, same
//! topic, independent group) counts the received record so we can assert
//! produce→consume works in one go.

use kafka::Order;
use oxide_boot::OxideApplication;
use oxide_kafka::kafka_listeners;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

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

#[tokio::test(flavor = "multi_thread")]
async fn producer_publishes_and_listener_consumes() {
    // Shared, reused Redpanda container (see `oxide_test::containers` module
    // docs); the fixture waits for broker readiness at the KAFKA protocol
    // level itself. `redpanda.endpoint` drives the generated yml below rather
    // than assuming a hardcoded default.
    let redpanda = oxide_test::containers::redpanda().await;

    let dir = std::env::temp_dir().join(format!("kafka-example-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("application.yml"),
        format!(
            // Port 0 on both servers. The defaults (8080/8081) are bound by
            // anything else running locally -- another test binary, or the
            // load-test stack in `deploy/` -- and this test does not use the
            // HTTP surface at all, so there is nothing to gain by claiming them.
            "oxide:\n  application:\n    name: kafka-example-test\n  server:\n    port: 0\n  management:\n    port: 0\n  kafka:\n    bootstrap-servers: \"{}\"\n",
            redpanda.endpoint
        ),
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
