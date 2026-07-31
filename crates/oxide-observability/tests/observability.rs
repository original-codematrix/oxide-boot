use oxide_boot::prelude::*;
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
    let dir = test_config_dir("");
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
    let dir = test_config_dir("");
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    app.stop().await.unwrap(); // reaching here twice across tests proves OnceLock guarding
}
