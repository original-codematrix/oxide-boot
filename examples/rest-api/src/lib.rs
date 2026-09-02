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
// Same principle for oxide-kafka: its listener descriptors and the kafka
// health indicator are `inventory::submit!` statics, linked in only by a
// reference like this.
#[allow(unused_imports)]
use oxide_kafka as _;

use oxide_boot::component;
use oxide_kafka::kafka_listeners;
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
    producer: oxide_kafka::KafkaProducer,
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
        let note = self.store.add(body.0);
        // Publish a note-created event (Kafka starter): the listener below
        // consumes it, exercising the producer→consumer pipeline end to end.
        if let Err(e) = self.producer.send("notes-events", &note.id.to_string(), &note).await {
            tracing::error!(error = %e, id = note.id, "failed to publish notes-events");
        }
        Json(note)
    }

    #[get("/{id}")]
    async fn get_by_id(&self, id: Path<u32>) -> Result<Json<Note>, ApiError> {
        self.store.find(*id).map(Json).ok_or_else(ApiError::not_found)
    }
}

/// Consumes `notes-events` (produced by `NotesController::create`) and logs
/// each created note — demonstrating the `#[kafka_listeners]` consumer side.
#[component]
pub struct NoteEventsListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl NoteEventsListener {
    #[kafka_listener(topic = "notes-events")]
    async fn on_note_created(&self, note: Note, key: oxide_kafka::MessageKey) -> Result<(), String> {
        tracing::info!(id = note.id, key = ?key.0, "note created event received");
        Ok(())
    }
}
