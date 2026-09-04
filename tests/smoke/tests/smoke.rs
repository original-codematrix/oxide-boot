//! The check that everything still fits together — fast, and without Docker.
//!
//! The rest of the suite is thorough but slow and container-bound: nine test
//! files need a running Docker daemon, which means a contributor without one
//! cannot tell whether their change broke anything. This file is what
//! `make smoke` runs: a full application boot with the web and observability
//! starters, every actuator endpoint, a real request through the router and
//! the middleware, the metrics that request produced, and a clean shutdown.
//!
//! It is deliberately one test rather than many. A smoke test answers one
//! question -- "is this build fundamentally sound?" -- and splitting it into
//! twenty assertions across twenty boots would trade the answer's speed for
//! granularity nobody needs at this stage.

use axum::extract::Json;
use oxide_boot::component;
use oxide_boot::prelude::*;
use oxide_web::{ApiError, controller};
use std::collections::HashMap;

// Linked in for its `inventory` registrations: the router layer, the
// actuator routes and the metrics recorder. Nothing below names an item from
// it, so without this import the crate is dropped and half of what this test
// checks would silently not exist.
#[allow(unused_imports)]
use oxide_observability as _;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct Echo {
    pub value: String,
}

#[component]
pub struct SmokeController {
    #[default]
    _marker: bool,
}

#[controller("/smoke")]
impl SmokeController {
    #[get("/echo/{value}")]
    async fn echo(&self, value: axum::extract::Path<String>) -> Json<Echo> {
        Json(Echo { value: value.0 })
    }

    #[post("/echo")]
    async fn echo_body(&self, body: Json<Echo>) -> Json<Echo> {
        Json(body.0)
    }

    #[get("/boom")]
    async fn boom(&self) -> Result<Json<Echo>, ApiError> {
        Err(ApiError::not_found())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_framework_boots_serves_measures_and_shuts_down() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        // Port 0 so this never collides with another test binary or a
        // running load-test stack. `prometheus` is opt-in by default and is
        // enabled here because the metrics assertion below needs it.
        "oxide:\n  application:\n    name: smoke\n  server:\n    port: 0\n  management:\n    port: 0\n    endpoints:\n      exposure: \"health,info,prometheus\"\n",
    )
    .expect("write application.yml");

    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
        .start()
        .await
        .expect("the application boots");

    let main = app.context().get::<oxide_web::WebServer>().expect("web server").local_addr().expect("http addr");
    let mgmt =
        app.context().get::<oxide_web::ManagementServer>().expect("management server").local_addr().expect("mgmt addr");
    let http = reqwest::Client::new();

    // --- the actuator surface a platform depends on -------------------
    let health: serde_json::Value =
        http.get(format!("http://{mgmt}/actuator/health")).send().await.unwrap().json().await.unwrap();
    assert_eq!(health["status"], "UP", "health was: {health}");

    for probe in ["health/liveness", "health/readiness", "info"] {
        let res = http.get(format!("http://{mgmt}/actuator/{probe}")).send().await.unwrap();
        assert_eq!(res.status(), 200, "/actuator/{probe} answered {}", res.status());
    }

    // --- a route, through the router and the observability middleware --
    let echoed: Echo = http.get(format!("http://{main}/smoke/echo/hello")).send().await.unwrap().json().await.unwrap();
    assert_eq!(echoed, Echo { value: "hello".into() });

    let posted: Echo = http
        .post(format!("http://{main}/smoke/echo"))
        .json(&Echo { value: "body".into() })
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(posted, Echo { value: "body".into() });

    // --- errors are RFC 7807, not a bare status ------------------------
    let err = http.get(format!("http://{main}/smoke/boom")).send().await.unwrap();
    assert_eq!(err.status(), 404);
    assert!(
        err.headers()["content-type"].to_str().unwrap().starts_with("application/problem+json"),
        "content-type was {:?}",
        err.headers()["content-type"]
    );
    let problem: serde_json::Value = err.json().await.unwrap();
    assert_eq!(problem["status"], 404, "problem was: {problem}");
    assert!(problem["title"].is_string(), "problem was: {problem}");

    // --- the requests above were measured, with route templates --------
    let scrape = http.get(format!("http://{mgmt}/actuator/prometheus")).send().await.unwrap().text().await.unwrap();
    assert!(scrape.contains("http_server_requests_seconds"), "no request histogram in scrape:\n{scrape}");
    assert!(
        scrape.contains(r#"uri="/smoke/echo/{value}""#),
        "the path parameter leaked into the metric label instead of the route template:\n{scrape}"
    );
    assert!(scrape.contains("process_resident_memory_bytes"), "no process metrics in scrape:\n{scrape}");

    // --- and it stops cleanly ------------------------------------------
    app.stop().await.expect("the application shuts down");
}
