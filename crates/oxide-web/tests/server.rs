use oxide_boot::prelude::*;
use oxide_web::WebServer;
use std::collections::HashMap;

static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: web-test\n  server:\n    port: 0\n",
    )
    .unwrap();
    dir
}

#[tokio::test]
async fn boots_serves_health_and_shuts_down() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    let server = app.context().get::<WebServer>().unwrap();
    let addr = server.local_addr().expect("server started");
    let base = format!("http://{addr}");

    let live: serde_json::Value =
        reqwest::get(format!("{base}/actuator/health/liveness")).await.unwrap().json().await.unwrap();
    assert_eq!(live["status"], "UP");

    let ready = reqwest::get(format!("{base}/actuator/health")).await.unwrap();
    assert_eq!(ready.status(), 200);

    app.stop().await.unwrap();
    assert!(reqwest::get(format!("{base}/actuator/health")).await.is_err(), "server must be down after stop");
}
