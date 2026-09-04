//! Serving over TLS.
//!
//! The certificate is generated in-process rather than checked in: a
//! committed test certificate expires, and the day it does the failure looks
//! like a TLS bug rather than a calendar problem.

use oxide_boot::prelude::*;
use std::collections::HashMap;

/// Writes a self-signed certificate and key for `localhost`, returning their
/// paths.
fn self_signed(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("generate certificate");
    let cert_path = dir.join("tls.crt");
    let key_path = dir.join("tls.key");
    std::fs::write(&cert_path, cert.cert.pem()).expect("write certificate");
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).expect("write key");
    (cert_path, key_path)
}

fn write_config(dir: &std::path::Path, extra: &str) {
    std::fs::write(
        dir.join("application.yml"),
        format!(
            "oxide:\n  application:\n    name: tls-test\n  server:\n    port: 0\n{extra}  management:\n    port: 0\n"
        ),
    )
    .expect("write application.yml");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_server_speaks_https_when_ssl_is_enabled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (cert, key) = self_signed(dir.path());
    write_config(
        dir.path(),
        &format!(
            "    ssl:\n      enabled: true\n      certificate: \"{}\"\n      private-key: \"{}\"\n",
            cert.display(),
            key.display()
        ),
    );

    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot");
    let addr = app.context().get::<oxide_web::WebServer>().expect("web server").local_addr().expect("addr");

    // Self-signed, so verification is off here -- what is being tested is
    // that a TLS handshake happens at all, not the trust chain.
    let client = reqwest::Client::builder().danger_accept_invalid_certs(true).build().expect("client");
    let res = client.get(format!("https://localhost:{}/actuator/health", addr.port())).send().await;

    // The main server has no /actuator route, so a 404 over HTTPS is a
    // successful handshake -- which is the assertion.
    match res {
        Ok(r) => assert!(r.status().is_client_error() || r.status().is_success(), "unexpected status {}", r.status()),
        Err(e) => panic!("HTTPS request failed, so TLS did not come up: {e}"),
    }

    // ...and plain HTTP against the same port must not work.
    let plain = reqwest::Client::new().get(format!("http://localhost:{}/", addr.port())).send().await;
    assert!(plain.is_err(), "the server answered plaintext on a TLS port");

    app.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn enabling_ssl_without_a_certificate_fails_the_boot() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_config(dir.path(), "    ssl:\n      enabled: true\n");

    let err = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
        .start()
        .await
        .expect_err("a server that quietly started in plaintext would be the worst outcome");
    let text = err.to_string();
    assert!(text.contains("certificate"), "the failure does not say what is missing: {text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_certificate_names_the_paths() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_config(
        dir.path(),
        "    ssl:\n      enabled: true\n      certificate: \"/nonexistent/tls.crt\"\n      private-key: \"/nonexistent/tls.key\"\n",
    );

    let err = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap_err();
    let text = err.to_string();
    // With several certificates mounted, "invalid certificate" alone is
    // unactionable.
    assert!(text.contains("/nonexistent/tls.crt"), "the certificate path is missing from: {text}");
    assert!(text.contains("/nonexistent/tls.key"), "the key path is missing from: {text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn ssl_is_off_by_default() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_config(dir.path(), "");
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot");
    let addr = app.context().get::<oxide_web::WebServer>().unwrap().local_addr().unwrap();
    // Terminating TLS at the pod is often the ingress's job, so plaintext
    // stays the default rather than self-signing something at boot.
    let res = reqwest::get(format!("http://localhost:{}/", addr.port())).await;
    assert!(res.is_ok(), "plaintext should work with no ssl configuration: {res:?}");
    app.stop().await.expect("stop");
}
