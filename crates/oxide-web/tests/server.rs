use oxide_boot::prelude::*;
use oxide_web::{WebServer, RouteDescriptor, WebError};
use oxide_boot::__private::ComponentDescriptor;
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
