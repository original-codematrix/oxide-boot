use oxide_boot::prelude::*;
use oxide_web::ManagementServer;
use std::collections::HashMap;

static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_config_dir(extra: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        format!("oxide:\n  application:\n    name: obs-test\n{extra}"),
    )
    .unwrap();
    dir
}

#[tokio::test]
async fn boot_installs_prometheus_recorder_and_custom_metrics_render() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir("  server:\n    port: 0\n  management:\n    port: 0\n");
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    metrics::counter!("obs_test_counter_total", "kind" => "unit").increment(3);
    let rendered = oxide_observability::prometheus_handle().render();
    assert!(rendered.contains("obs_test_counter_total"), "render was: {rendered}");
    assert!(rendered.contains("kind=\"unit\""), "render was: {rendered}");

    app.stop().await.unwrap();
}

#[tokio::test]
async fn second_boot_in_same_process_does_not_panic_on_recorder() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir("  server:\n    port: 0\n  management:\n    port: 0\n");
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    app.stop().await.unwrap(); // reaching here twice across tests proves OnceLock guarding
}

#[tokio::test]
async fn prometheus_endpoint_scrapes_custom_metrics() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir("  server:\n    port: 0\n  management:\n    port: 0\n");
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let mgmt = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();

    metrics::counter!("scrape_probe_total").increment(1);
    let res = reqwest::get(format!("http://{mgmt}/actuator/prometheus")).await.unwrap();
    assert_eq!(res.status(), 200);
    assert!(res.headers()["content-type"].to_str().unwrap().starts_with("text/plain"));
    assert!(res.text().await.unwrap().contains("scrape_probe_total"));

    app.stop().await.unwrap();
}
