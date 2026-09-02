use oxide_boot::prelude::*;
use oxide_web::ManagementServer;
use std::collections::HashMap;

static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[oxide_boot::component]
struct PingController {}

#[oxide_web::controller("/ping")]
impl PingController {
    #[get("/{name}")]
    async fn ping(&self, name: axum::extract::Path<String>) -> String {
        format!("pong {}", *name)
    }
}

fn test_config_dir(extra: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), format!("oxide:\n  application:\n    name: obs-test\n{extra}"))
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
async fn prometheus_route_is_absent_unless_exposed() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir("  server:\n    port: 0\n  management:\n    port: 0\n");
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let mgmt_addr = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();

    let status = reqwest::get(format!("http://{mgmt_addr}/actuator/prometheus")).await.unwrap().status();
    assert_eq!(status, 404);

    app.stop().await.unwrap();
}

#[tokio::test]
async fn prometheus_endpoint_scrapes_custom_metrics() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir("  server:\n    port: 0\n  management:\n    port: 0\n");
    let env =
        HashMap::from([("OXIDE_MANAGEMENT_ENDPOINTS_EXPOSURE".to_string(), "health,info,prometheus".to_string())]);
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.unwrap();
    let mgmt = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();

    metrics::counter!("scrape_probe_total").increment(1);
    let res = reqwest::get(format!("http://{mgmt}/actuator/prometheus")).await.unwrap();
    assert_eq!(res.status(), 200);
    assert!(res.headers()["content-type"].to_str().unwrap().starts_with("text/plain"));
    assert!(res.text().await.unwrap().contains("scrape_probe_total"));

    app.stop().await.unwrap();
}

#[tokio::test]
async fn http_requests_are_measured_with_route_template_labels() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir("  server:\n    port: 0\n  management:\n    port: 0\n");
    let env =
        HashMap::from([("OXIDE_MANAGEMENT_ENDPOINTS_EXPOSURE".to_string(), "health,info,prometheus".to_string())]);
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.unwrap();
    let main = app.context().get::<oxide_web::WebServer>().unwrap().local_addr().unwrap();
    let mgmt = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();

    reqwest::get(format!("http://{main}/ping/world")).await.unwrap();
    let scrape = reqwest::get(format!("http://{mgmt}/actuator/prometheus")).await.unwrap().text().await.unwrap();
    assert!(scrape.contains("http_server_requests_seconds"), "scrape: {scrape}");
    assert!(scrape.contains("uri=\"/ping/{name}\""), "route template label missing: {scrape}");
    assert!(scrape.contains("status=\"200\""), "scrape: {scrape}");
    // Proves histogram rendering (bucketed `_bucket{...,le="..."}` series),
    // not the default Prometheus summary — quantiles must be aggregatable
    // across instances via `histogram_quantile()` in Grafana.
    assert!(scrape.contains("http_server_requests_seconds_bucket{"), "no bucket series: {scrape}");
    assert!(scrape.contains("le=\""), "no le= bucket label: {scrape}");

    app.stop().await.unwrap();
}
