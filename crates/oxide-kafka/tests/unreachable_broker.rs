//! Integration test binary: boots against a `bootstrap-servers` nothing is
//! listening on. Pins the REAL contract (Task 11, fix round 1): `rdkafka`
//! connects lazily, so an unreachable broker must NOT fail boot — that
//! would turn a broker outage (e.g. mid Kubernetes rollout) into a crash
//! loop for a condition the pod would otherwise recover from. The crate's
//! own `kafka` health indicator (`oxide-kafka/src/health.rs`) is the
//! intended way to observe this: it reports the `kafka` component `DOWN`
//! on `/actuator/health` while the app keeps serving everything else.
//!
//! Deliberately does NOT use `harness::` (no Redpanda container needed):
//! the whole point is that no broker is reachable. `127.0.0.1:1` is used
//! rather than binding-and-dropping an ephemeral port — port 1 is
//! privileged/reserved and deterministically has nothing listening,
//! without a bind/drop race.

use oxide_boot::OxideApplication;
use oxide_kafka::KafkaProducer;
use std::collections::HashMap;

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  server:\n    port: 0\n  management:\n    port: 0\n  kafka:\n    bootstrap-servers: \"127.0.0.1:1\"\n",
    )
    .expect("write application.yml");
    dir
}

#[tokio::test(flavor = "multi_thread")]
async fn unreachable_broker_boots_without_panicking() {
    let dir = test_config_dir();

    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await;
    let app = app.expect("boot must succeed against an unreachable-but-syntactically-valid broker");

    // `KafkaProducer` construction itself does not probe the broker (see its
    // doc comment) — it's present in the context regardless of reachability.
    assert!(app.context().get::<KafkaProducer>().is_some(), "KafkaProducer must still be constructed");

    let mgmt_addr = app.context().get::<oxide_web::ManagementServer>().unwrap().local_addr().unwrap();
    let body: serde_json::Value =
        reqwest::get(format!("http://{mgmt_addr}/actuator/health")).await.unwrap().json().await.unwrap();
    // `oxide.management.health.show-details` defaults to "never", so only
    // the per-component status (not the driver error `detail`) is visible
    // here; `health` is exposed by default (`oxide.management.endpoints.exposure`).
    assert_eq!(
        body["components"]["kafka"]["status"],
        serde_json::json!("DOWN"),
        "kafka health indicator must report DOWN against an unreachable broker: {body}"
    );

    app.stop().await.expect("clean shutdown even though the broker was never reachable");
}
