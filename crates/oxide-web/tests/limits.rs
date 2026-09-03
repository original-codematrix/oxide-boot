use oxide_boot::prelude::*;
use oxide_web::{WebServer, controller};
use std::collections::HashMap;

static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    // `request-timeout-seconds`/`max-body-bytes` must already exist as keys for
    // the env-override "relaxed matching" to bind a multi-segment env var (e.g.
    // OXIDE_SERVER_REQUEST_TIMEOUT_SECONDS) onto the hyphenated key — see
    // `oxide_core::config`'s `set_existing`/`set_relaxed`. Declaring them here
    // with their normal defaults lets the tests override just the one value
    // they care about via env.
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: limits-test\n  server:\n    port: 0\n    request-timeout-seconds: 30\n    max-body-bytes: 2097152\n  management:\n    port: 0\n    request-timeout-seconds: 30\n    max-body-bytes: 2097152\n",
    )
    .unwrap();
    dir
}

#[component]
struct SlowController {}

#[controller("/slow")]
impl SlowController {
    #[get("")]
    async fn slow(&self) -> &'static str {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        "done"
    }
}

#[component]
struct EchoController {}

#[controller("/echo")]
impl EchoController {
    #[post("")]
    async fn echo(&self, body: String) -> String {
        body
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_handler_hits_the_request_timeout() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let mut env = HashMap::new();
    env.insert("OXIDE_SERVER_REQUEST_TIMEOUT_SECONDS".to_string(), "1".to_string());
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();

    let resp = reqwest::Client::new().get(format!("http://{addr}/slow")).send().await.unwrap();
    assert_eq!(resp.status(), 503);

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_body_is_rejected() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let mut env = HashMap::new();
    env.insert("OXIDE_SERVER_MAX_BODY_BYTES".to_string(), "1024".to_string());
    let app = OxideApplication::builder().base_dir(dir.path()).env(env).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();

    let big = "x".repeat(4096);
    let resp = reqwest::Client::new().post(format!("http://{addr}/echo")).body(big).send().await.unwrap();
    assert_eq!(resp.status(), 413);

    app.stop().await.unwrap();
}
