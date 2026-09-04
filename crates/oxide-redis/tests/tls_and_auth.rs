//! Connecting to a Redis that is not an unauthenticated localhost.
//!
//! Credentials and TLS are carried in the URL, exactly as `redis-cli` and
//! Spring's `spring.data.redis.url` do:
//!
//! | form | meaning |
//! |---|---|
//! | `redis://host:6379` | plaintext, no auth |
//! | `redis://:password@host` | password only (Redis before ACLs) |
//! | `redis://user:password@host` | ACL user (Redis 6+) |
//! | `rediss://user:password@host` | the above over TLS |
//!
//! Nothing here reaches a server: what is being pinned is that the driver
//! *accepts* these forms, which is what a wrong feature set would break --
//! `rediss://` fails to parse at all unless a TLS feature is enabled, and it
//! would fail at boot in production rather than in CI.

use oxide_redis::RedisProps;

fn props(url: &str) -> RedisProps {
    RedisProps { url: url.to_string(), ping_on_start: false, ..RedisProps::default() }
}

#[test]
fn every_url_form_is_accepted_by_the_driver() {
    for url in [
        "redis://127.0.0.1:6379",
        "redis://:secret@127.0.0.1:6379",
        "redis://appuser:secret@127.0.0.1:6379",
        "redis://127.0.0.1:6379/3",
        "rediss://127.0.0.1:6380",
        "rediss://appuser:secret@redis.example.com:6380",
        // Certificate verification off, for a self-signed server. Accepted
        // by the driver; whether it is wise is the operator's call.
        "rediss://127.0.0.1:6380/#insecure",
    ] {
        redis::Client::open(url).unwrap_or_else(|e| panic!("driver rejected {url}: {e}"));
    }
}

#[test]
fn credentials_never_reach_the_logs() {
    // `Debug` is hand-written precisely so a config dump cannot leak the
    // password embedded in the URL.
    let text = format!("{:?}", props("rediss://appuser:hunter2@redis.example.com:6380"));
    assert!(!text.contains("hunter2"), "the password appeared in Debug output: {text}");
    assert!(!text.contains("appuser"), "the username appeared in Debug output: {text}");
    assert!(text.contains("<redacted>"), "expected the url to be redacted: {text}");
    // The rest stays visible -- redaction must not make the type useless
    // for diagnosing a misconfiguration.
    assert!(text.contains("command_timeout_ms"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_password_surfaces_as_an_error_not_a_hang() {
    // Nothing listens here, so this exercises the failure path rather than
    // authentication itself: the point is that a bad connection is reported
    // rather than retried forever.
    let template = oxide_redis::RedisTemplate::from_props(&props("redis://user:wrong@127.0.0.1:1"));
    let err = template.get("k").await.expect_err("a closed port must not look like a cache miss");
    let text = err.to_string();
    assert!(!text.contains("wrong"), "the password leaked into the error: {text}");
}
