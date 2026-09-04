//! Metrics that exist before anything has happened, and request-span slots.
//!
//! The point of pre-registration is an alert that can be written before the
//! first incident: `rate(orders_failed_total[5m])` returns no series at all
//! for a counter that has never fired, so a rule built on it is silently
//! inert exactly while nothing has gone wrong -- and stays inert when the
//! first failure lands in a series nobody was watching.

use oxide_boot::prelude::*;
use oxide_observability::{RequestField, record_request_field};
use oxide_web::ManagementServer;
use std::collections::HashMap;

// No `inventory::submit!` anywhere: the registration mechanism is an
// implementation detail, hidden the same way `#[component]` hides it.
oxide_observability::declare_counter!(
    "registry_probe_failures_total",
    "Failures seen by the registry integration test"
);

oxide_observability::declare_gauge!(
    "registry_probe_queue_depth",
    "Queue depth reported by the registry integration test"
);

static LABEL_SETS: &[&[(&str, &str)]] = &[&[("outcome", "ok")], &[("outcome", "rejected")]];

oxide_observability::declare_counter!("registry_probe_outcomes_total", labels = LABEL_SETS);

async fn scrape() -> (oxide_boot::Application, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: registry-test\n  server:\n    port: 0\n  management:\n    port: 0\n",
    )
    .expect("write application.yml");
    let env =
        HashMap::from([("OXIDE_MANAGEMENT_ENDPOINTS_EXPOSURE".to_string(), "health,info,prometheus".to_string())]);
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.expect("boot");
    let mgmt = app.context().get::<ManagementServer>().expect("management server").local_addr().expect("addr");
    let body = reqwest::get(format!("http://{mgmt}/actuator/prometheus"))
        .await
        .expect("scrape")
        .text()
        .await
        .expect("scrape body");
    (app, body)
}

#[tokio::test]
async fn a_declared_counter_renders_as_zero_before_it_ever_fires() {
    let (app, body) = scrape().await;

    // Nothing in this test increments it. Without pre-registration the line
    // would be absent entirely, which is the failure this exists to prevent.
    let line = body
        .lines()
        .find(|l| l.starts_with("registry_probe_failures_total "))
        .unwrap_or_else(|| panic!("counter missing from scrape:\n{body}"));
    assert_eq!(line.split_whitespace().nth(1), Some("0"), "line was: {line}");

    app.stop().await.expect("stop");
}

#[tokio::test]
async fn a_declared_gauge_renders_as_zero() {
    let (app, body) = scrape().await;
    let line = body
        .lines()
        .find(|l| l.starts_with("registry_probe_queue_depth "))
        .unwrap_or_else(|| panic!("gauge missing from scrape:\n{body}"));
    assert_eq!(line.split_whitespace().nth(1), Some("0"), "line was: {line}");
    app.stop().await.expect("stop");
}

#[tokio::test]
async fn every_declared_label_combination_gets_its_own_zero_series() {
    let (app, body) = scrape().await;

    // A labelled counter is one series per combination, and only the
    // combinations named up front can be rendered as zero -- which is why
    // `with_labels` takes the values rather than just the keys.
    for outcome in ["ok", "rejected"] {
        let needle = format!("registry_probe_outcomes_total{{outcome=\"{outcome}\"}}");
        let line = body
            .lines()
            .find(|l| l.starts_with(&needle))
            .unwrap_or_else(|| panic!("series for outcome={outcome} missing from scrape:\n{body}"));
        assert_eq!(line.split_whitespace().nth(1), Some("0"), "line was: {line}");
    }

    app.stop().await.expect("stop");
}

#[tokio::test]
async fn the_description_reaches_the_scrape() {
    let (app, body) = scrape().await;
    assert!(
        body.contains("# HELP registry_probe_failures_total Failures seen by the registry integration test"),
        "HELP line missing:\n{body}"
    );
    app.stop().await.expect("stop");
}

#[test]
fn recording_a_request_field_outside_a_request_is_harmless() {
    // No request span is current here. tracing drops the record, and the
    // call must not panic -- a handler helper that blows up when called from
    // a background task would be worse than one that does nothing.
    record_request_field(RequestField::Tenant, "acme");
    record_request_field(RequestField::Principal, 42);
}
