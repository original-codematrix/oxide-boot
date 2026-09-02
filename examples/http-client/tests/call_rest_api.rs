//! End-to-end proof for `#[http_client]`: starts an in-process axum server
//! (the same routes rest-api exposes), then calls through the generated
//! `NotesClient` — GET list, GET by id, POST create, and a 404 via the typed
//! `ClientError`.

use axum::Router;
use axum::extract::{Json as AxumJson, Path as AxumPath};
use axum::http::StatusCode;
use axum::routing::get as aget;
use http_client::Note;
use oxide_client::{Json, Path, http_client};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Mutex;

const BASE: &str = "http://localhost:18080";

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct Stored {
    id: u32,
    text: String,
}

static STORE: Mutex<Vec<Stored>> = Mutex::new(Vec::new());

async fn list_notes() -> axum::Json<Vec<Stored>> {
    axum::Json(STORE.lock().unwrap().clone())
}
async fn get_note(id: AxumPath<u32>) -> std::result::Result<axum::Json<Stored>, (StatusCode, &'static str)> {
    STORE
        .lock()
        .unwrap()
        .iter()
        .find(|n| n.id == *id)
        .cloned()
        .map(axum::Json)
        .ok_or((StatusCode::NOT_FOUND, "not found"))
}
async fn create_note(AxumJson(body): AxumJson<Stored>) -> axum::Json<Stored> {
    STORE.lock().unwrap().push(body.clone());
    axum::Json(body)
}

#[http_client(base_url = "http://localhost:18080")]
trait NotesClient {
    #[get("/api/notes")]
    async fn list(&self) -> Result<Vec<Note>, oxide_client::ClientError>;

    #[get("/api/notes/{id}")]
    async fn get(&self, id: Path<u32>) -> Result<Note, oxide_client::ClientError>;

    #[post("/api/notes")]
    async fn create(&self, body: Json<Note>) -> Result<Note, oxide_client::ClientError>;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_client_hits_real_server() {
    STORE.lock().unwrap().clear();

    let app =
        Router::new().route("/api/notes", aget(list_notes).post(create_note)).route("/api/notes/{id}", aget(get_note));
    let addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });
    // Give the server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let client = NotesClient { __client: reqwest::Client::new(), __base_url: BASE.to_string() };

    // POST create.
    let created = client.create(Json(Note { id: 7, text: "first".into() })).await.expect("create");
    assert_eq!(created.text, "first");

    // GET list.
    let all = client.list().await.expect("list");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, 7);

    // GET by id.
    let fetched = client.get(Path(7)).await.expect("get");
    assert_eq!(fetched.text, "first");

    // NOT FOUND → typed ClientError.
    let missing = client.get(Path(999)).await.expect_err("expected a 404 ClientError");
    assert_eq!(missing.status().map(|s| s.as_u16()), Some(404));
}
