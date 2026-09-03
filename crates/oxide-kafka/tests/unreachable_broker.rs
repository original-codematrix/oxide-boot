//! Integration test binary: boots against a `bootstrap-servers` that nothing
//! is listening on. This must fail `start()` with a `BootError`, not panic
//! the test process — Task 11 (turn panics into errors).
//!
//! Deliberately does NOT use `harness::` (no Redpanda container needed): the
//! whole point is that no broker is reachable. A dummy listener is declared
//! so `oxide-kafka`'s inventory-registered components (`KafkaProducer`,
//! `KafkaListenerManager`) actually link into and run in this test binary —
//! without any reference into the crate, nothing would force the linker to
//! pull in its `inventory::submit!` registrations.

use oxide_boot::OxideApplication;
use oxide_kafka::kafka_listeners;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
struct Ev {
    #[allow(dead_code)]
    id: u32,
}

#[oxide_boot::component]
struct DummyListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl DummyListener {
    #[kafka_listener(topic = "unreachable-broker-test")]
    async fn on_event(&self, _ev: Ev) -> Result<(), String> {
        Ok(())
    }
}

/// Binds an ephemeral local port and immediately drops the listener, so the
/// returned port is very likely free but nothing is listening on it.
fn unreachable_bootstrap_servers() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    format!("127.0.0.1:{port}")
}

#[tokio::test(flavor = "multi_thread")]
async fn unreachable_broker_fails_boot_instead_of_panicking() {
    let dir = tempfile::tempdir().expect("tempdir");
    let yml = format!("oxide:\n  kafka:\n    bootstrap-servers: \"{}\"\n", unreachable_bootstrap_servers());
    std::fs::write(dir.path().join("application.yml"), yml).expect("write application.yml");

    let result = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await;

    assert!(result.is_err(), "expected boot to fail with an error, not panic or succeed");
}
