//! Proves `oxide-test` boots an app on ephemeral ports and serves it through
//! the preconfigured test client, including via the management client.

use oxide_boot::component;
use oxide_test::TestApp;
use oxide_web::controller;
use std::path::PathBuf;

#[component]
struct PingController {}

#[controller("/ping")]
impl PingController {
    #[get("")]
    async fn ping(&self) -> String {
        "pong".to_string()
    }
}

fn write_config(dir: &PathBuf) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("application.yml"), "oxide:\n  application:\n    name: oxide-test-demo\n").unwrap();
}

#[tokio::test]
async fn test_app_boots_and_clients_reach_servers() {
    let dir = std::env::temp_dir().join(format!("oxide-test-{}", std::process::id()));
    write_config(&dir);

    let app = TestApp::builder().base_dir(&dir).start().await.expect("boot under test");

    // Main server reachable through the client.
    let resp = app.client().get("/ping").send().await.expect("get");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "pong");

    // Management client hits the actuator health endpoint.
    let health = app.management_client().get("/actuator/health/liveness").send().await.expect("mgmt get");
    assert_eq!(health.status(), 200);

    app.stop().await.expect("stop");
    let _ = std::fs::remove_dir_all(&dir);
}