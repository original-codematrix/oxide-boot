use oxide_boot::prelude::*;
use oxide_web::{ManagementServer, WebServer};
use rest_api::Note;
use std::collections::HashMap;

#[tokio::test]
async fn create_fetch_and_missing_note_flow() {
    let env = HashMap::from([
        ("OXIDE_SERVER_PORT".to_string(), "0".to_string()), // env override, dogfooded
        ("OXIDE_MANAGEMENT_PORT".to_string(), "0".to_string()), // avoid colliding on the real 8081 default
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

    // oxide-observability is active purely by being a dependency (the
    // starter principle) — scrape the management-port Prometheus endpoint
    // and confirm both the custom counter and the automatic HTTP histogram
    // showed up.
    let scrape =
        client.get(format!("http://{mgmt_addr}/actuator/prometheus")).send().await.unwrap().text().await.unwrap();
    assert!(scrape.contains("notes_created_total"), "scrape: {scrape}");
    assert!(scrape.contains("http_server_requests_seconds"), "scrape: {scrape}");

    app.stop().await.unwrap();
}
