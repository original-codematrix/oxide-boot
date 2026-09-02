//! `#[http_client]` example: a declarative FeignClient-style client for the
//! `rest-api` example (`GET/POST /api/notes`). The `#[http_client]` trait is
//! defined in `tests/call_rest_api.rs`, which boots an in-process server and
//! exercises every client method end-to-end.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Note {
    pub id: u32,
    pub text: String,
}
