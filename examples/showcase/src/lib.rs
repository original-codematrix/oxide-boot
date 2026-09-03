//! Reference example exercising all four integrations at once: HTTP
//! (`oxide-web`), MongoDB (`oxide-mongo`), Redis/Dragonfly (`oxide-redis`)
//! and Kafka (`oxide-kafka`), with `oxide-observability` exporting metrics
//! and traces.
//!
//! It is deliberately shaped like a service worth load-testing rather than a
//! feature tour. The read path is cache-aside, the write path is
//! write-through-and-publish, and a Kafka listener folds the published events
//! back into a Redis counter — so a load test drives every component and the
//! resulting dashboard shows cache hit ratio, consumer lag and request
//! latency moving against each other.
//!
//! ```text
//!   GET  /api/notes/{id}  ->  Redis (hit)  ──────────────┐
//!                         ->  Redis (miss) -> Mongo -> Redis (fill)
//!   POST /api/notes       ->  Mongo -> Redis (evict) -> Kafka
//!   Kafka notes-events    ->  Redis (counter++)
//!   GET  /api/stats       ->  Redis
//! ```

// These crates' `inventory::submit!` registrations (starters, health
// indicators, the observability router layer) only survive into the final
// binary if something forces the crate to be linked in. Nothing below names
// an item from them, so these imports are that force -- remove one and its
// health indicator, routes or listeners silently disappear at boot.
#[allow(unused_imports)]
use oxide_observability as _;
#[allow(unused_imports)]
use oxide_redis as _;

use axum::extract::{Json, Path, Query};
use futures::TryStreamExt;
use oxide_boot::component;
use oxide_kafka::{KafkaProducer, kafka_listeners};
use oxide_mongo::{Document, MongoClient};
use oxide_redis::RedisTemplate;
use oxide_web::{ApiError, controller};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// How long a cached note stays warm. Short enough that a load test actually
/// exercises refills rather than reading one filled cache for the whole run.
const CACHE_TTL: Duration = Duration::from_secs(30);

/// Cap on `GET /api/notes`. Any list endpoint without one is a denial of
/// service against itself as soon as the collection grows.
const PAGE_SIZE: i64 = 50;

#[derive(Debug, Clone, Serialize, Deserialize, Document, PartialEq)]
#[collection("notes")]
pub struct Note {
    #[id]
    #[serde(rename = "_id")]
    pub id: String,
    pub title: String,
    pub body: String,
    pub tag: String,
}

/// What the API returns. Separate from `Note` on purpose: `Note` is the Mongo
/// document, and its `id` is renamed to `_id` for BSON -- serializing it
/// straight to the client would leak the storage field name into the public
/// contract. (The load test caught exactly that: every response carried `_id`
/// and no `id`.) This is the entity/DTO split Spring makes for the same reason.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NoteView {
    pub id: String,
    pub title: String,
    pub body: String,
    pub tag: String,
}

impl From<Note> for NoteView {
    fn from(n: Note) -> Self {
        Self { id: n.id, title: n.title, body: n.body, tag: n.tag }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewNote {
    pub title: String,
    pub body: String,
    #[serde(default = "default_tag")]
    pub tag: String,
}

fn default_tag() -> String {
    "general".to_string()
}

#[derive(Debug, Serialize)]
pub struct Stats {
    /// Notes the Kafka listener has observed being created.
    pub events_consumed: i64,
    pub cache_hits: i64,
    pub cache_misses: i64,
}

#[derive(Debug, Deserialize)]
pub struct ByTag {
    /// Absent `?tag=` deserializes to `None` rather than failing the request,
    /// so the same handler serves the filtered and unfiltered list.
    pub tag: Option<String>,
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

// `async_fn_in_trait`: the generated `NoteRepositoryImpl` is what gets
// injected and called, never a `dyn NoteRepository`, so the missing `Send`
// bound on the desugared future has no caller to constrain. `dead_code`:
// several methods exist to document the derived-query surface.
#[allow(dead_code, async_fn_in_trait)]
#[oxide_mongo::mongo_repository(document = Note)]
pub trait NoteRepository {
    async fn find_by_id(&self, id: String) -> oxide_mongo::Result<Option<Note>>;
    async fn find_all(&self) -> oxide_mongo::Result<Vec<Note>>;
    async fn save(&self, note: &Note) -> oxide_mongo::Result<()>;
    async fn count(&self) -> oxide_mongo::Result<u64>;
    async fn find_all_by_tag(&self, tag: String) -> oxide_mongo::Result<Vec<Note>>;
}

// ---------------------------------------------------------------------------
// Cache-aside service
// ---------------------------------------------------------------------------

/// The read/write path. Kept out of the controller so the caching policy is
/// testable without going through HTTP.
#[component]
pub struct NoteService {
    repo: NoteRepositoryImpl,
    cache: RedisTemplate,
    /// Used only by `list`, which needs a `limit` the derived-query surface
    /// cannot express -- see the comment there.
    mongo: MongoClient,
}

impl NoteService {
    fn cache_key(id: &str) -> String {
        format!("note:{id}")
    }

    /// Cache-aside read. A cache error is logged and treated as a miss: Redis
    /// being unavailable must degrade latency, not availability, since Mongo
    /// still holds the authoritative copy.
    pub async fn get(&self, id: &str) -> Result<Option<Note>, ApiError> {
        match self.cache.get_json::<Note>(&Self::cache_key(id)).await {
            Ok(Some(note)) => {
                metrics::counter!("showcase_cache_total", "result" => "hit").increment(1);
                let _ = self.cache.increment("stats:cache-hits", 1).await;
                return Ok(Some(note));
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, id, "cache read failed; falling through to mongo"),
        }
        metrics::counter!("showcase_cache_total", "result" => "miss").increment(1);
        let _ = self.cache.increment("stats:cache-misses", 1).await;

        let note = self.repo.find_by_id(id.to_string()).await.map_err(|e| {
            tracing::error!(error = %e, id, "mongo read failed");
            ApiError::internal_server_error().with_detail("failed to load the note")
        })?;

        if let Some(note) = &note
            && let Err(e) = self.cache.set_json(&Self::cache_key(id), note, Some(CACHE_TTL)).await
        {
            tracing::warn!(error = %e, id, "cache fill failed");
        }
        Ok(note)
    }

    /// Write-through: Mongo is the source of truth, so it is written first.
    /// The cache entry is *evicted* rather than updated -- an eviction that
    /// fails leaves a stale entry that expires within `CACHE_TTL`, whereas a
    /// failed update could leave a wrong one indefinitely.
    pub async fn create(&self, new: NewNote) -> Result<Note, ApiError> {
        let note = Note { id: format!("{:x}", uuid_like()), title: new.title, body: new.body, tag: new.tag };
        self.repo.save(&note).await.map_err(|e| {
            tracing::error!(error = %e, "mongo write failed");
            ApiError::internal_server_error().with_detail("failed to save the note")
        })?;
        if let Err(e) = self.cache.delete(&Self::cache_key(&note.id)).await {
            tracing::warn!(error = %e, id = note.id, "cache eviction failed");
        }
        Ok(note)
    }

    /// Lists at most `PAGE_SIZE` notes.
    ///
    /// This deliberately does *not* use `repo.find_all_by_tag`. Derived
    /// queries have no `limit`/`skip`/`sort` -- pagination is a known gap --
    /// so `find_all_by_tag` returns the entire matching set. The first
    /// benchmark run made the cost obvious: against a 20k-document corpus it
    /// serialised ~6.7k notes per request, and the run measured JSON encoding
    /// rather than anything about the framework. Until derived pagination
    /// exists, the driver escape hatch is the honest way to write this.
    pub async fn list(&self, tag: Option<String>) -> Result<Vec<Note>, ApiError> {
        let filter = match &tag {
            Some(tag) => mongodb::bson::doc! { "tag": tag },
            None => mongodb::bson::doc! {},
        };
        let collection = self.mongo.collection::<Note>("notes").map_err(|e| {
            tracing::error!(error = %e, "mongo collection handle failed");
            ApiError::internal_server_error().with_detail("failed to list notes")
        })?;
        let cursor = collection.find(filter).limit(PAGE_SIZE).await.map_err(|e| {
            tracing::error!(error = %e, "mongo list failed");
            ApiError::internal_server_error().with_detail("failed to list notes")
        })?;
        cursor.try_collect::<Vec<Note>>().await.map_err(|e| {
            tracing::error!(error = %e, "mongo cursor failed");
            ApiError::internal_server_error().with_detail("failed to list notes")
        })
    }
}

/// A random-enough id without pulling in a uuid dependency: the example only
/// needs collision resistance within one load-test run.
fn uuid_like() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let salt = std::thread::current().id();
    nanos ^ ((format!("{salt:?}").len() as u128) << 96) ^ (std::process::id() as u128) << 64
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

#[component]
pub struct NotesController {
    service: NoteService,
    producer: KafkaProducer,
}

#[controller("/api/notes")]
impl NotesController {
    #[get("")]
    async fn list(&self, query: Query<ByTag>) -> Result<Json<Vec<NoteView>>, ApiError> {
        let notes = self.service.list(query.0.tag).await?;
        Ok(Json(notes.into_iter().map(NoteView::from).collect()))
    }

    #[post("")]
    async fn create(&self, body: Json<NewNote>) -> Result<Json<NoteView>, ApiError> {
        let note = self.service.create(body.0).await?;
        metrics::counter!("showcase_notes_created_total").increment(1);
        // Publishing is best-effort: the note is already durable in Mongo, so
        // a broker outage must not fail the request the client already won.
        if let Err(e) = self.producer.send("notes-events", &note.id, &note).await {
            tracing::error!(error = %e, id = note.id, "failed to publish notes-events");
            metrics::counter!("showcase_publish_failures_total").increment(1);
        }
        Ok(Json(note.into()))
    }

    #[get("/{id}")]
    async fn get_by_id(&self, id: Path<String>) -> Result<Json<NoteView>, ApiError> {
        self.service.get(&id).await?.map(|n| Json(n.into())).ok_or_else(ApiError::not_found)
    }
}

#[component]
pub struct StatsController {
    cache: RedisTemplate,
}

#[controller("/api/stats")]
impl StatsController {
    #[get("")]
    async fn stats(&self) -> Result<Json<Stats>, ApiError> {
        // A missing counter means "nothing has happened yet", which is zero,
        // not an error -- so an unseeded Redis still answers this endpoint.
        let read = |key: &'static str| {
            let cache = self.cache.clone();
            async move { cache.get(key).await.ok().flatten().and_then(|v| v.parse::<i64>().ok()).unwrap_or(0) }
        };
        Ok(Json(Stats {
            events_consumed: read("stats:events-consumed").await,
            cache_hits: read("stats:cache-hits").await,
            cache_misses: read("stats:cache-misses").await,
        }))
    }
}

// ---------------------------------------------------------------------------
// Kafka consumer
// ---------------------------------------------------------------------------

/// Folds published note events back into a Redis counter, so `/api/stats`
/// shows the producer→broker→consumer round trip actually completing under
/// load rather than just the producer's own success count.
#[component]
pub struct NoteEventsListener {
    cache: RedisTemplate,
}

#[kafka_listeners]
impl NoteEventsListener {
    #[kafka_listener(topic = "notes-events", group_id = "showcase")]
    async fn on_note_created(&self, note: Note) -> Result<(), String> {
        // Returning `Err` re-delivers the message, which is what we want here:
        // a Redis blip should cost a retry, not a silently lost count.
        self.cache.increment("stats:events-consumed", 1).await.map_err(|e| format!("counter increment failed: {e}"))?;
        metrics::counter!("showcase_events_consumed_total").increment(1);
        tracing::debug!(id = note.id, "consumed note event");
        Ok(())
    }
}
