//! The showcase's cache-aside policy, against real MongoDB and Redis.
//!
//! This example carried no tests at all while it was being benchmarked, and
//! the benchmark found two defects in it that a test would have caught in a
//! second: every response exposed `_id` instead of `id`, and the list
//! endpoint had no result cap. Both are pinned here.
//!
//! What is worth testing is the *policy*, not the plumbing: that a second
//! read is served from the cache, that a write evicts rather than updates,
//! that a Redis outage degrades latency instead of availability, and that
//! the cap holds. Those are the decisions someone could plausibly change by
//! accident.

use oxide_mongo_test::MongoTest;
use showcase::{NewNote, Note, NoteView};
use std::time::Duration;

/// Boots the showcase with both stores wired to their fixtures, each
/// isolated to this test, and returns the app plus its base URL.
async fn boot() -> (MongoTest, String) {
    let redis = oxide_redis_test::dragonfly().await.endpoint;
    let prefix = oxide_redis_test::unique_prefix();
    let kafka = oxide_kafka_test::redpanda().await.endpoint;

    // `MongoTest` supplies the isolated database; the other two starters are
    // layered on through its builder so all three share one application.
    let t = MongoTest::builder()
        .with_yaml(&format!(
            "  redis:\n    url: \"{redis}\"\n    key-prefix: \"{prefix}\"\n  kafka:\n    bootstrap-servers: \"{kafka}\"\n"
        ))
        .start()
        .await;

    let addr =
        t.app().context().get::<oxide_web::WebServer>().expect("web server").local_addr().expect("bound address");
    (t, format!("http://{addr}"))
}

async fn create(base: &str, title: &str, tag: &str) -> NoteView {
    reqwest::Client::new()
        .post(format!("{base}/api/notes"))
        .json(&NewNote { title: title.to_string(), body: "b".repeat(50), tag: tag.to_string() })
        .send()
        .await
        .expect("create request")
        .json()
        .await
        .expect("create response")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_api_exposes_id_not_the_storage_field_name() {
    let (t, base) = boot().await;
    let raw: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/notes"))
        .json(&NewNote { title: "t".into(), body: "b".into(), tag: "general".into() })
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // `Note` renames its id to `_id` for BSON. Serialising the document
    // straight to the client leaked that into the public contract, which is
    // what the first benchmark run tripped over.
    assert!(raw.get("id").is_some(), "response has no `id`: {raw}");
    assert!(raw.get("_id").is_none(), "the Mongo field name leaked into the API: {raw}");

    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_second_read_is_served_from_the_cache() {
    let (t, base) = boot().await;
    let note = create(&base, "cached", "general").await;
    let http = reqwest::Client::new();
    let url = format!("{base}/api/notes/{}", note.id);

    // First read misses and fills; second must hit.
    let first: NoteView = http.get(&url).send().await.unwrap().json().await.unwrap();
    assert_eq!(first.id, note.id);
    let _second: NoteView = http.get(&url).send().await.unwrap().json().await.unwrap();

    let stats: serde_json::Value = http.get(format!("{base}/api/stats")).send().await.unwrap().json().await.unwrap();
    // The counters are written off the request path, so give them a moment.
    let mut hits = stats["cache_hits"].as_i64().unwrap_or(0);
    for _ in 0..40 {
        if hits >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let s: serde_json::Value = http.get(format!("{base}/api/stats")).send().await.unwrap().json().await.unwrap();
        hits = s["cache_hits"].as_i64().unwrap_or(0);
    }
    assert!(hits >= 1, "no cache hit was recorded after reading the same note twice");

    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_still_works_when_redis_is_unreachable() {
    // Redis pointed at a closed port: the cache must degrade latency, not
    // availability, because Mongo still holds the authoritative copy.
    let kafka = oxide_kafka_test::redpanda().await.endpoint;
    let t = MongoTest::builder()
        .with_yaml(&format!(
            "  redis:\n    url: \"redis://127.0.0.1:1\"\n    command-timeout-ms: 300\n    ping-on-start: false\n  kafka:\n    bootstrap-servers: \"{kafka}\"\n"
        ))
        .start()
        .await;
    let addr = t.app().context().get::<oxide_web::WebServer>().unwrap().local_addr().unwrap();
    let base = format!("http://{addr}");

    let note = create(&base, "no-cache", "general").await;
    let read = reqwest::Client::new().get(format!("{base}/api/notes/{}", note.id)).send().await.unwrap();
    assert_eq!(read.status(), 200, "a read failed while Redis was down; it should fall through to Mongo");
    let body: NoteView = read.json().await.unwrap();
    assert_eq!(body.id, note.id);

    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_list_endpoint_is_capped() {
    let (t, base) = boot().await;

    // Insert past the cap directly, rather than through 60 HTTP requests.
    let notes = t.collection::<Note>("notes");
    let docs: Vec<Note> = (0..60)
        .map(|i| Note { id: format!("seed-{i:03}"), title: format!("n{i}"), body: "b".into(), tag: "general".into() })
        .collect();
    notes.insert_many(docs).await.expect("seed notes");

    let listed: Vec<NoteView> =
        reqwest::Client::new().get(format!("{base}/api/notes?tag=general")).send().await.unwrap().json().await.unwrap();

    // Uncapped, this returned every matching document -- against a 20k
    // corpus that was ~6,700 per request and dominated the benchmark.
    assert_eq!(listed.len(), 50, "the list endpoint returned {} notes; the cap is 50", listed.len());

    t.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_note_is_rfc_7807_problem_json() {
    let (t, base) = boot().await;
    let res = reqwest::Client::new().get(format!("{base}/api/notes/does-not-exist")).send().await.unwrap();

    assert_eq!(res.status(), 404);
    assert!(
        res.headers()["content-type"].to_str().unwrap().starts_with("application/problem+json"),
        "content-type was {:?}",
        res.headers()["content-type"]
    );
    let problem: serde_json::Value = res.json().await.unwrap();
    assert_eq!(problem["status"], 404, "problem was: {problem}");

    t.stop().await.unwrap();
}
