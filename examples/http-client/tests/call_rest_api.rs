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

// Only a placeholder for the `#[http_client]` attribute, which needs a literal
// at compile time. Both tests below bind port 0 and override `__base_url` with
// the address the OS actually handed out -- a fixed port here would collide
// with anything else on the machine (another test binary, the load-test stack
// in `deploy/`), and the failure mode is nasty: the bind happens inside a
// spawned task, so its panic is swallowed and the test silently talks to
// whatever else already owns the port.
const BASE: &str = "http://localhost:18080";

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct Stored {
    id: u32,
    text: String,
}

static STORE: Mutex<Vec<Stored>> = Mutex::new(Vec::new());
// A second, independent store/server pair for `path_injection_cannot_escape_the_route`
// — it runs concurrently with `http_client_hits_real_server` (both `#[tokio::test]`),
// so sharing `STORE` would race the two tests' assertions against each other.
static STORE2: Mutex<Vec<Stored>> = Mutex::new(Vec::new());

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

async fn list_notes2() -> axum::Json<Vec<Stored>> {
    axum::Json(STORE2.lock().unwrap().clone())
}
// `id` is extracted as a raw `String` (not `u32`) so a path-injected value
// that stays inside this single route segment (percent-encoded, per the
// `Path<T>` contract) is compared literally against stored ids instead of
// being rejected upstream by a numeric-parse failure — that would hide
// whether the value escaped the route, which is exactly what this handler
// (and `path_injection_cannot_escape_the_route` below) needs to observe.
async fn get_note2(id: AxumPath<String>) -> std::result::Result<axum::Json<Stored>, (StatusCode, &'static str)> {
    STORE2
        .lock()
        .unwrap()
        .iter()
        .find(|n| n.id.to_string() == *id)
        .cloned()
        .map(axum::Json)
        .ok_or((StatusCode::NOT_FOUND, "not found"))
}

#[http_client(base_url = "http://localhost:18080")]
trait NotesClient {
    #[get("/api/notes")]
    async fn list(&self) -> Result<Vec<Note>, oxide_client::ClientError>;

    #[get("/api/notes/{id}")]
    async fn get(&self, id: Path<u32>) -> Result<Note, oxide_client::ClientError>;

    // Same route as `get`, but `id` is a `String` — used to prove a value
    // containing `/`/`..` stays inside this one route segment instead of
    // being interpreted as extra path components.
    #[get("/api/notes/{id}")]
    async fn get_raw(&self, id: Path<String>) -> Result<Note, oxide_client::ClientError>;

    #[post("/api/notes")]
    async fn create(&self, body: Json<Note>) -> Result<Note, oxide_client::ClientError>;
}

#[tokio::test(flavor = "multi_thread")]
async fn http_client_hits_real_server() {
    STORE.lock().unwrap().clear();

    let app =
        Router::new().route("/api/notes", aget(list_notes).post(create_note)).route("/api/notes/{id}", aget(get_note));
    // Bind here rather than inside the task: this way a bind failure fails the
    // test, and the port is already listening before the first request.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind test server");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = NotesClient { __client: reqwest::Client::new(), __base_url: format!("http://{addr}") };
    let _ = BASE;

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

/// Without percent-encoding, `Path("../notes".to_string())` interpolated raw
/// into `/api/notes/{id}` produces the literal path `/api/notes/../notes`,
/// which normalizes to `/api/notes` — the *list* route, answered with 200 and
/// a JSON array (a different route entirely, not the one `get_raw` targets).
/// Percent-encoded, the value stays inside the single `{id}` segment, so the
/// server never finds a note named `../notes` and answers the target route's
/// own 404 — proving the path parameter cannot leave the declared route.
#[tokio::test(flavor = "multi_thread")]
async fn path_injection_cannot_escape_the_route() {
    STORE2.lock().unwrap().push(Stored { id: 7, text: "first".into() });

    let app = Router::new().route("/api/notes", aget(list_notes2)).route("/api/notes/{id}", aget(get_note2));
    // Same as above: bind here so a failure surfaces, and take the port the OS
    // assigns instead of claiming a fixed one.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind test server");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = NotesClient { __client: reqwest::Client::new(), __base_url: format!("http://{addr}") };

    let escaped = client.get_raw(Path("../notes".to_string())).await.expect_err("must not reach the list route");
    assert_eq!(escaped.status().map(|s| s.as_u16()), Some(404));
}
