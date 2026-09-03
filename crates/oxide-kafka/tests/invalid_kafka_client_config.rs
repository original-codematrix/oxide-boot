//! Integration test binary: an invalid librdkafka option injected through
//! `oxide.kafka.properties` must fail `KafkaProducer` construction and, in
//! turn, `start()` — this is the genuine failing-test-first for Task 11's
//! `expect()` -> `Result` conversion in `crates/oxide-kafka/src/producer.rs`
//! (`config.create()` truly can fail, unlike broker-reachability, which
//! `rdkafka` only ever discovers lazily — see `unreachable_broker.rs`).
//!
//! Deliberately does NOT use `harness::`: `create()` rejects the bogus
//! `security.protocol` value before any network I/O happens.

use oxide_boot::OxideApplication;
use oxide_kafka::KafkaProducer;
use std::collections::HashMap;

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        concat!(
            "oxide:\n",
            "  server:\n    port: 0\n",
            "  management:\n    port: 0\n",
            "  kafka:\n",
            "    bootstrap-servers: \"127.0.0.1:1\"\n",
            "    properties:\n",
            "      security.protocol: \"not-a-protocol\"\n",
        ),
    )
    .expect("write application.yml");
    dir
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_kafka_client_config_fails_boot_with_error() {
    // Forces the linker to actually pull in oxide-kafka's `inventory::submit!`
    // component registrations (KafkaProducer's included) into this test
    // binary — with zero references into the crate, Rust never even passes
    // its rlib to the linker, and the crate's components silently never run.
    let _ = std::any::TypeId::of::<KafkaProducer>();

    let dir = test_config_dir();

    let result = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await;

    let err = result.expect_err("boot must fail: 'not-a-protocol' is not a valid librdkafka security.protocol");
    assert!(
        matches!(err, oxide_boot::BootError::Context(_)),
        "expected the boot failure to come from a ContextError::Construct (KafkaProducer construction), got: {err}"
    );
}
