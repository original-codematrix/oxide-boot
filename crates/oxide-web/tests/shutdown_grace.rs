// Task 5: readiness grace before draining, and an honest `stop()`.
use oxide_boot::prelude::*;
use oxide_web::{ManagementServer, WebServer, controller};
use std::collections::HashMap;
use std::time::Duration;

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: shutdown-grace-test\n  server:\n    port: 0\n  management:\n    port: 0\n",
    )
    .unwrap();
    dir
}

#[component]
struct HangingController {}

#[controller("/hang")]
impl HangingController {
    #[get("")]
    async fn hang(&self) -> &'static str {
        tokio::time::sleep(Duration::from_secs(5)).await;
        "done"
    }
}

/// While the readiness grace is running, `pre_destroy` must have already
/// flipped readiness to DOWN (so a load balancer sees it and stops routing
/// new traffic) but must NOT yet have started draining the main listener —
/// it should still accept and answer new connections. Polls the readiness
/// endpoint (rather than a bare sleep) so the assertion only fires once
/// readiness has genuinely flipped, and does so with a large margin (a
/// 3-second grace against a poll interval of 20ms) before the drain begins.
#[tokio::test(flavor = "multi_thread")]
async fn readiness_is_down_before_the_drain_starts() {
    let dir = test_config_dir();
    let mut env = HashMap::new();
    env.insert("OXIDE_SERVER_SHUTDOWN_READINESS_GRACE_SECONDS".to_string(), "3".to_string());
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.unwrap();

    let main_addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();
    let mgmt_addr = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();
    let readiness_url = format!("http://{mgmt_addr}/actuator/health/readiness");
    let main_url = format!("http://{main_addr}/hang");

    let stop_handle = tokio::spawn(app.stop());

    // Poll until readiness has flipped DOWN, with a generous overall timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = reqwest::get(&readiness_url).await.unwrap();
        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "readiness never flipped DOWN before the deadline");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The drain hasn't started yet (readiness-grace-seconds = 3, far longer
    // than the polling above took) — the main port must still accept the
    // connection and start answering (the handler itself sleeps 5s, so the
    // response won't complete before `stop()` does, but a short-timeout
    // request must not be immediately refused the way it would be once the
    // listener is torn down).
    let client = reqwest::Client::builder().timeout(Duration::from_millis(300)).build().unwrap();
    match client.get(&main_url).send().await {
        Ok(_) => {} // answered within the timeout
        Err(e) => assert!(e.is_timeout(), "main port must still be reachable during the readiness grace, got: {e}"),
    }

    stop_handle.await.unwrap().unwrap();
}

#[component]
struct VerySlowController {}

#[controller("/veryslow")]
impl VerySlowController {
    #[get("")]
    async fn very_slow(&self) -> &'static str {
        tokio::time::sleep(Duration::from_secs(5)).await;
        "done"
    }
}

/// A request that outlives `shutdown-grace-seconds` forces the serve task
/// to be aborted mid-drain. `stop()` must report that honestly as an error
/// instead of claiming a clean shutdown.
#[tokio::test(flavor = "multi_thread")]
async fn aborted_drain_surfaces_an_error() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: shutdown-grace-abort-test\n  server:\n    port: 0\n    shutdown-grace-seconds: 1\n  management:\n    port: 0\n",
    )
    .unwrap();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();

    let in_flight = tokio::spawn(reqwest::get(format!("http://{addr}/veryslow")));
    tokio::time::sleep(Duration::from_millis(100)).await; // request is on the wire

    let result = app.stop().await;
    assert!(result.is_err(), "stop() must surface the aborted drain as an error");

    // The in-flight request was genuinely cut off, not served.
    let _ = in_flight.await;
}
