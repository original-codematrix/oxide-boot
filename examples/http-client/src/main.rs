//! Runnable shell: the `#[http_client]` example's real behavior lives in
//! `tests/call_rest_api.rs` (which boots rest-api, then calls through the
//! `NotesClient`). This binary documents the wiring so `cargo build -p
//! http-client` compiles the client macro path.
fn main() {
    println!(
        "http-client example: run 'cargo run -p rest-api', then 'cargo test -p http-client' to exercise the #[http_client] client."
    );
}