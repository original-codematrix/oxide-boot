//! Proves the `#[oxide_test]` annotation (the `@SpringBootTest` equivalent):
//! the async test body receives a booted `&TestApplication` on ephemeral ports
//! and the harness shuts it down afterwards.

use oxide_boot::component;
use oxide_test::oxide_test;
use oxide_web::controller;

#[component]
struct Greeter {}

#[controller("/hello")]
impl Greeter {
    #[get("")]
    async fn hello(&self) -> String {
        "hello from oxide-test".to_string()
    }
}

#[oxide_test]
async fn annotation_boots_app_and_serves_controller(app: &oxide_test::TestApplication) {
    let resp = app.client().get("/hello").send().await.expect("GET /hello");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "hello from oxide-test");

    // Management server reachable too.
    let health = app.management_client().get("/actuator/health/liveness").send().await.expect("mgmt liveness");
    assert_eq!(health.status(), 200);
}
