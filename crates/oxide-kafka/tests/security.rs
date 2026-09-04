//! What librdkafka was actually compiled with.
//!
//! `oxide.kafka.properties` passes any key straight to the driver, so the
//! configuration side always "works" -- `security.protocol=SASL_SSL` is
//! accepted and stored regardless. Whether the *library* can honour it is a
//! build-time question, and getting it wrong fails on the first connection
//! to a secured cluster, in production, with "built without SASL support".
//!
//! librdkafka validates `security.protocol` and `sasl.mechanism` when the
//! client is *created*, against what it was compiled with -- so constructing
//! a client is enough to prove the capability, and nothing here contacts a
//! broker.

use std::collections::HashMap;

/// Configuring a mechanism librdkafka was not built for fails when the
/// client is created, which is what makes this testable without a broker.
fn client_accepts(properties: &[(&str, &str)]) -> Result<(), String> {
    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", "127.0.0.1:1");
    for (k, v) in properties {
        config.set(*k, *v);
    }
    config.create::<rdkafka::producer::BaseProducer>().map(|_| ()).map_err(|e| e.to_string())
}

#[test]
fn tls_is_compiled_in() {
    // `security.protocol=SSL` is rejected outright by a build without SSL.
    client_accepts(&[("security.protocol", "SSL")])
        .expect("librdkafka was built without SSL -- enable oxide-kafka's `security` feature");
}

#[test]
fn scram_sha_512_is_compiled_in() {
    // The mechanism a managed Kafka (MSK, Confluent, Redpanda Cloud) asks
    // for. SCRAM needs the SSL build for its hashing.
    client_accepts(&[
        ("security.protocol", "SASL_SSL"),
        ("sasl.mechanism", "SCRAM-SHA-512"),
        ("sasl.username", "user"),
        ("sasl.password", "secret"),
    ])
    .expect("librdkafka was built without SASL/SCRAM -- enable oxide-kafka's `security` feature");
}

#[test]
fn scram_sha_256_and_plain_are_compiled_in() {
    for mechanism in ["SCRAM-SHA-256", "PLAIN"] {
        client_accepts(&[
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", mechanism),
            ("sasl.username", "user"),
            ("sasl.password", "secret"),
        ])
        .unwrap_or_else(|e| panic!("mechanism {mechanism} unavailable: {e}"));
    }
}

#[test]
fn mutual_tls_material_is_loaded_at_client_creation() {
    // Real files, not placeholder paths: librdkafka *parses* the
    // certificate when the client is constructed, long before it connects.
    // That is what makes this testable offline -- and it means a wrong path
    // in production fails at startup rather than on first produce.
    let dir = tempfile::tempdir().expect("tempdir");
    let cert = rcgen::generate_simple_self_signed(vec!["kafka.test".to_string()]).expect("generate");
    let ca_path = dir.path().join("ca.pem");
    let cert_path = dir.path().join("client.pem");
    let key_path = dir.path().join("client.key");
    std::fs::write(&ca_path, cert.cert.pem()).expect("write ca");
    std::fs::write(&cert_path, cert.cert.pem()).expect("write cert");
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).expect("write key");

    client_accepts(&[
        ("security.protocol", "SSL"),
        ("ssl.ca.location", ca_path.to_str().unwrap()),
        ("ssl.certificate.location", cert_path.to_str().unwrap()),
        ("ssl.key.location", key_path.to_str().unwrap()),
    ])
    .expect("mTLS material rejected");
}

#[test]
fn a_missing_certificate_fails_at_startup_not_on_first_send() {
    // The other half of the above, stated as the property that matters
    // operationally.
    let err = client_accepts(&[("security.protocol", "SSL"), ("ssl.ca.location", "/nonexistent/ca.pem")])
        .expect_err("a missing CA file must not be discovered on the first produce");
    assert!(err.contains("ssl.ca.location"), "the failure does not name the setting: {err}");
}

#[test]
fn the_properties_passthrough_reaches_the_client_config() {
    // The config side: everything above has to be reachable from YAML or the
    // environment without the framework knowing each key.
    let props = oxide_kafka::KafkaProps {
        bootstrap_servers: "broker:9093".to_string(),
        consumer: Default::default(),
        producer: Default::default(),
        properties: HashMap::from([
            ("security.protocol".to_string(), "SASL_SSL".to_string()),
            ("sasl.mechanism".to_string(), "SCRAM-SHA-512".to_string()),
        ]),
    };
    let config = props.client_config(&[]);
    assert_eq!(config.get("security.protocol"), Some("SASL_SSL"));
    assert_eq!(config.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
    assert_eq!(config.get("bootstrap.servers"), Some("broker:9093"));
}
