//! Reference example: a minimal REST API on `oxide-web`. `NoteStore` is a
//! plain component holding in-memory state; `NotesController` mounts routes
//! via `#[controller]`/`#[get]`/`#[post]` and returns RFC 7807 problem+json
//! on error. Types live here (rather than directly in `main.rs`) so the
//! integration test in `tests/api.rs` can see them.

// oxide-observability's `LoggingInitializer`/`RouterLayer`/`ManagementRoute`
// registrations are `inventory::submit!` statics; like `rest_api`'s own
// components (see the forcing import in `main.rs`), they only survive into
// the final binary if something forces the crate to be linked in. Nothing
// else here names an `oxide_observability` item, so this import is that
// force — remove it and `/actuator/prometheus` 404s.
#[allow(unused_imports)]
use oxide_observability as _;

use oxide_boot::component;
use oxide_web::{controller, ApiError};
use axum::extract::{Json, Path};
use std::sync::Mutex;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Note {
    pub id: u32,
    pub text: String,
}

#[component]
pub struct NoteStore {
    #[default]
    notes: Mutex<Vec<Note>>,
}

impl NoteStore {
    pub fn add(&self, note: Note) -> Note {
        let mut notes = self.notes.lock().unwrap_or_else(|p| p.into_inner());
        notes.push(note.clone());
        note
    }

    pub fn find(&self, id: u32) -> Option<Note> {
        self.notes.lock().unwrap_or_else(|p| p.into_inner()).iter().find(|n| n.id == id).cloned()
    }

    pub fn all(&self) -> Vec<Note> {
        self.notes.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

#[component]
pub struct NotesController {
    store: NoteStore,
}

#[controller("/api/notes")]
impl NotesController {
    #[get("")]
    async fn list(&self) -> Json<Vec<Note>> {
        Json(self.store.all())
    }

    #[post("")]
    async fn create(&self, body: Json<Note>) -> Json<Note> {
        metrics::counter!("notes_created_total").increment(1);
        Json(self.store.add(body.0))
    }

    #[get("/{id}")]
    async fn get_by_id(&self, id: Path<u32>) -> Result<Json<Note>, ApiError> {
        self.store.find(*id).map(Json).ok_or_else(ApiError::not_found)
    }
}
