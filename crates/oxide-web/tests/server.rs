use oxide_boot::prelude::*;
use oxide_web::{controller, ApiError, WebServer, RouteDescriptor, WebError};
use oxide_boot::__private::ComponentDescriptor;
use axum::extract::{Json, Path};
use std::collections::HashMap;

static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: web-test\n  server:\n    port: 0\n",
    )
    .unwrap();
    dir
}

#[tokio::test]
async fn boots_serves_health_and_shuts_down() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    let server = app.context().get::<WebServer>().unwrap();
    let addr = server.local_addr().expect("server started");
    let base = format!("http://{addr}");

    let live: serde_json::Value =
        reqwest::get(format!("{base}/actuator/health/liveness")).await.unwrap().json().await.unwrap();
    assert_eq!(live["status"], "UP");

    let ready = reqwest::get(format!("{base}/actuator/health")).await.unwrap();
    assert_eq!(ready.status(), 200);

    app.stop().await.unwrap();
    assert!(reqwest::get(format!("{base}/actuator/health")).await.is_err(), "server must be down after stop");
    assert!(server.local_addr().is_none(), "local_addr must be None after pre_destroy tears down the server");
}

// Hand-written route descriptor test (macro-generated version comes in Task 6)
struct EchoService { prefix: String }

inventory::submit! {
    ComponentDescriptor {
        type_name: "EchoService",
        type_id: || std::any::TypeId::of::<EchoService>(),
        deps: || vec![],
        construct: |b| { b.insert(EchoService { prefix: "echo:".into() }); Ok(()) },
        post_construct: None,
        pre_destroy: None,
    }
}

inventory::submit! {
    RouteDescriptor {
        method: "GET",
        path: "/echo/{msg}",
        register: |ctx, router| {
            let svc = ctx.get::<EchoService>().ok_or(WebError::ControllerNotRegistered {
                type_name: "EchoService",
                path: "/echo/{msg}",
            })?;
            Ok(router.route("/echo/{msg}", axum::routing::get(move |axum::extract::Path(msg): axum::extract::Path<String>| {
                let svc = svc.clone();
                async move { format!("{}{}", svc.prefix, msg) }
            })))
        },
    }
}

#[tokio::test]
async fn hand_written_route_descriptor_is_mounted_and_resolves_components() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();
    let body = reqwest::get(format!("http://{addr}/echo/hi")).await.unwrap().text().await.unwrap();
    assert_eq!(body, "echo:hi");
    app.stop().await.unwrap();
}

// #[controller] macro test (Task 6).
#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct Note {
    id: u32,
    text: String,
}

#[component]
struct NotesController {
    #[default]
    store: std::sync::Mutex<Vec<Note>>,
}

#[controller("/api/notes")]
impl NotesController {
    #[post("")]
    async fn create(&self, body: Json<Note>) -> Result<Json<Note>, ApiError> {
        let note = body.0.clone();
        self.store.lock().unwrap_or_else(|p| p.into_inner()).push(body.0);
        Ok(Json(note))
    }

    #[get("/{id}")]
    async fn get_by_id(&self, id: Path<u32>) -> Result<Json<Note>, ApiError> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .find(|n| n.id == *id)
            .cloned()
            .map(Json)
            .ok_or_else(ApiError::not_found)
    }
}

// Graceful-shutdown drain test (Task 7).
#[component]
struct SlowController {}

#[controller("/slow")]
impl SlowController {
    #[get("")]
    async fn slow(&self) -> &'static str {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        "done"
    }
}

#[tokio::test]
async fn graceful_shutdown_drains_in_flight_requests() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();

    let in_flight = tokio::spawn(reqwest::get(format!("http://{addr}/slow")));
    tokio::time::sleep(std::time::Duration::from_millis(100)).await; // request is on the wire
    app.stop().await.unwrap(); // must wait for the drain

    let response = in_flight.await.unwrap().unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "done");
}

#[tokio::test]
async fn controller_macro_mounts_routes_and_returns_problem_json_on_error() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();
    let base = format!("http://{addr}/api/notes");
    let client = reqwest::Client::new();

    let created: serde_json::Value = client
        .post(&base)
        .json(&serde_json::json!({ "id": 1, "text": "hello" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(created["text"], "hello");

    let fetched: serde_json::Value =
        client.get(format!("{base}/1")).send().await.unwrap().json().await.unwrap();
    assert_eq!(fetched["id"], 1);

    let missing = client.get(format!("{base}/999")).send().await.unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(missing.headers()["content-type"], "application/problem+json");
    let problem: serde_json::Value = missing.json().await.unwrap();
    assert_eq!(problem["title"], "Not Found");
    assert_eq!(problem["status"], 404);

    app.stop().await.unwrap();
}
