//! Integration tests for the Redis starter against a real Dragonfly server.
//!
//! Dragonfly, not Redis: it is what the showcase example and the load test
//! run against, so the starter meets the server it will actually meet in
//! anger. The wire protocol is the same.
//!
//! The template is resolved out of the DI `Context` after a full
//! `OxideApplication` boot rather than hand-constructed — that also proves
//! the `ComponentDescriptor` wiring and the `post_construct` connect, not
//! just that the command surface works.

use oxide_boot::OxideApplication;
use oxide_redis::RedisTemplate;
use std::collections::HashMap;
use std::time::Duration;

/// Boots an app whose only starter is Redis, and hands back the injected
/// template plus the app (which must outlive it — dropping the app tears the
/// context down).
async fn boot(prefix: &str) -> (oxide_boot::Application, RedisTemplate) {
    let url = oxide_test::containers::dragonfly().await.endpoint;
    let dir = tempfile::tempdir().expect("tempdir");
    // Port 0 on both servers: these tests run concurrently within the binary
    // and alongside other test binaries, so binding the documented defaults
    // (8080/8081) would make them collide on "address already in use".
    let yml = format!(
        "oxide:\n  application:\n    name: redis-template-test\n  server:\n    port: 0\n  management:\n    port: 0\n  redis:\n    url: \"{url}\"\n    key-prefix: \"{prefix}\"\n"
    );
    std::fs::write(dir.path().join("application.yml"), yml).expect("write application.yml");

    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
        .start()
        .await
        .expect("boot oxide app with the redis starter");

    // `get` hands back an `Inject<T>`; deref-clone to get the template itself,
    // which is cheap to clone by design.
    let template = (*app.context().get::<RedisTemplate>().expect("RedisTemplate is registered as a component")).clone();
    (app, template)
}

#[tokio::test(flavor = "multi_thread")]
async fn set_and_get_round_trip() {
    let (_app, t) = boot("t1:").await;
    t.set("greeting", "hello", None).await.unwrap();
    assert_eq!(t.get("greeting").await.unwrap().as_deref(), Some("hello"));
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_key_is_none_not_an_error() {
    let (_app, t) = boot("t2:").await;
    assert_eq!(t.get("never-written").await.unwrap(), None);
    assert!(!t.exists("never-written").await.unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn json_round_trip() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Note {
        id: u32,
        title: String,
    }
    let (_app, t) = boot("t3:").await;
    let note = Note { id: 7, title: "cached".into() };
    t.set_json("note:7", &note, None).await.unwrap();
    assert_eq!(t.get_json::<Note>("note:7").await.unwrap(), Some(note));
}

#[tokio::test(flavor = "multi_thread")]
async fn deserialize_failure_names_the_stored_key() {
    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Note {
        id: u32,
    }
    let (_app, t) = boot("t4:").await;
    t.set("note:bad", "not json", None).await.unwrap();
    let err = t.get_json::<Note>("note:bad").await.unwrap_err();
    // The *prefixed* key, so the message points at what is actually stored
    // rather than at the caller's unqualified name.
    assert!(err.to_string().contains("t4:note:bad"), "was: {err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_reports_whether_the_key_existed() {
    let (_app, t) = boot("t5:").await;
    t.set("doomed", "x", None).await.unwrap();
    assert!(t.delete("doomed").await.unwrap());
    assert!(!t.delete("doomed").await.unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn increment_accumulates() {
    let (_app, t) = boot("t6:").await;
    t.delete("counter").await.unwrap();
    assert_eq!(t.increment("counter", 1).await.unwrap(), 1);
    assert_eq!(t.increment("counter", 4).await.unwrap(), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn ttl_expires_the_key() {
    let (_app, t) = boot("t7:").await;
    // The template clamps sub-second TTLs to one second (Redis rejects EX 0),
    // so one second is the shortest expiry that can be observed at all.
    t.set("short-lived", "x", Some(Duration::from_millis(1))).await.unwrap();
    assert!(t.exists("short-lived").await.unwrap());
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(!t.exists("short-lived").await.unwrap(), "key should have expired");
}

#[tokio::test(flavor = "multi_thread")]
async fn key_prefix_isolates_namespaces() {
    let (_app_a, a) = boot("ns-a:").await;
    let (_app_b, b) = boot("ns-b:").await;
    a.set("same-name", "from-a", None).await.unwrap();
    b.set("same-name", "from-b", None).await.unwrap();
    assert_eq!(a.get("same-name").await.unwrap().as_deref(), Some("from-a"));
    assert_eq!(b.get("same-name").await.unwrap().as_deref(), Some("from-b"));
}

#[tokio::test(flavor = "multi_thread")]
async fn health_endpoint_reports_redis_up() {
    let url = oxide_test::containers::dragonfly().await.endpoint;
    let dir = tempfile::tempdir().expect("tempdir");
    // Port 0: the OS picks a free one, so this never collides with another
    // test binary running concurrently.
    let yml = format!(
        "oxide:\n  application:\n    name: redis-health-test\n  server:\n    port: 0\n  management:\n    port: 0\n    endpoints:\n      exposure: \"health\"\n      show-details: always\n  redis:\n    url: \"{url}\"\n"
    );
    std::fs::write(dir.path().join("application.yml"), yml).expect("write application.yml");

    // `TestApp` rather than `OxideApplication`: it is what exposes the bound
    // management address, which is not known ahead of time with port 0.
    let app = oxide_test::TestApp::builder().base_dir(dir.path()).start().await.expect("boot");

    let body: serde_json::Value = app
        .management_client()
        .get("/actuator/health")
        .send()
        .await
        .expect("health request")
        .json()
        .await
        .expect("health json");

    assert_eq!(body["status"], "UP", "body was: {body}");
    assert_eq!(body["components"]["redis"]["status"], "UP", "body was: {body}");

    app.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn get_many_is_positional_and_tolerates_gaps() {
    let (_app, t) = boot("t8:").await;
    t.set("a", "1", None).await.unwrap();
    t.set("c", "3", None).await.unwrap();
    // "b" is deliberately absent: the result must keep the caller's ordering
    // and mark the gap, not compact it away.
    let got = t.get_many(&["a", "b", "c"]).await.unwrap();
    assert_eq!(got, vec![Some("1".to_string()), None, Some("3".to_string())]);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_many_applies_the_key_prefix() {
    let (_app, t) = boot("t9:").await;
    t.set("prefixed", "yes", None).await.unwrap();
    assert_eq!(t.get_many(&["prefixed"]).await.unwrap(), vec![Some("yes".to_string())]);
    // Reading the same name through a differently prefixed template must miss.
    let (_app2, other) = boot("t9-other:").await;
    assert_eq!(other.get_many(&["prefixed"]).await.unwrap(), vec![None]);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_many_with_no_keys_does_not_hit_the_server() {
    let (_app, t) = boot("t10:").await;
    // MGET with no arguments is a protocol error, so this must be answered
    // locally rather than sent.
    assert_eq!(t.get_many(&[]).await.unwrap(), Vec::<Option<String>>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_applies_every_command() {
    let (_app, t) = boot("t11:").await;
    t.delete("counter").await.unwrap();
    // The closure gets redis-rs's builder, which does *not* apply the
    // template's prefix -- hence `t.key(...)` on each key.
    t.pipeline(|pipe| {
        pipe.cmd("SET").arg(t.key("one")).arg("1").ignore();
        pipe.cmd("INCRBY").arg(t.key("counter")).arg(5).ignore();
        pipe.cmd("INCRBY").arg(t.key("counter")).arg(2).ignore();
    })
    .await
    .unwrap();
    assert_eq!(t.get("one").await.unwrap().as_deref(), Some("1"));
    assert_eq!(t.get("counter").await.unwrap().as_deref(), Some("7"));
}
