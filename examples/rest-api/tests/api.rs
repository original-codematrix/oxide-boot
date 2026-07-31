use oxide_boot::prelude::*;
use oxide_web::WebServer;
use rest_api::Note;
use std::collections::HashMap;

#[tokio::test]
async fn create_fetch_and_missing_note_flow() {
    let env = HashMap::from([("OXIDE_SERVER_PORT".to_string(), "0".to_string())]); // env override, dogfooded
    let app = OxideApplication::builder().base_dir(env!("CARGO_MANIFEST_DIR")).env(env).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();
    let base = format!("http://{addr}/api/notes");
    let client = reqwest::Client::new();

    client.post(&base).json(&Note { id: 7, text: "first".into() }).send().await.unwrap();
    let fetched: Note = client.get(format!("{base}/7")).send().await.unwrap().json().await.unwrap();
    assert_eq!(fetched.text, "first");

    let missing = client.get(format!("{base}/8")).send().await.unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(missing.headers()["content-type"], "application/problem+json");

    app.stop().await.unwrap();
}
