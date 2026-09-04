//! Named caches with pluggable backends — the `@Cacheable` / `CacheManager`
//! analogue.
//!
//! A service usually wants more than one cache and not the same storage for
//! each: rendered images belong in process memory where they cost a pointer
//! dereference, while anything several replicas must agree on belongs in
//! Redis. [`CacheManager`] resolves a cache *by name*, and configuration
//! decides which backend each name gets, so the choice is an operational one
//! rather than something compiled into a handler.
//!
//! ```yaml
//! oxide:
//!   cache:
//!     default-backend: in-memory
//!     caches:
//!       images:  { backend: in-memory, ttl-seconds: 300, max-entries: 500 }
//!       notes:   { backend: redis, ttl-seconds: 30 }
//! ```
//!
//! ```ignore
//! #[component]
//! pub struct NoteService {
//!     caches: CacheManager,
//!     repo: NoteRepositoryImpl,
//! }
//!
//! impl NoteService {
//!     #[cacheable(cache = "notes", key = "id")]
//!     pub async fn find(&self, id: &str) -> Result<Option<Note>, ApiError> {
//!         self.repo.find_by_id(id).await
//!     }
//! }
//! ```
//!
//! # What is deliberately not cached
//!
//! An `Err` is never stored. Caching a failure turns a transient outage into
//! a sticky one for the length of the TTL, and the caller cannot tell the
//! difference. A successful `None` *is* stored, because "this does not
//! exist" is a real answer worth not asking twice — and it is what stops a
//! lookup storm for a missing key from reaching the database on every
//! request.

pub mod memory;

pub use oxide_cache_macros::cacheable;

use oxide_core::{ComponentDescriptor, Context};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Errors a cache backend can return.
///
/// A cache is an optimisation, so callers are expected to treat these as a
/// miss rather than as a failure. `#[cacheable]` does exactly that.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cache backend error: {0}")]
    Backend(String),
    #[error("failed to serialize a value for the cache: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("failed to deserialize the value cached at '{key}': {source}")]
    Deserialize {
        key: String,
        #[source]
        source: serde_json::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// One named cache.
///
/// Values are stored as JSON strings rather than as typed objects, so a
/// single backend can serve every value type and an in-memory cache and a
/// Redis one behave identically. The cost is a serialise/deserialise even
/// for the in-memory case; the benefit is that switching a cache's backend
/// is a configuration change and nothing else.
#[allow(async_fn_in_trait)]
pub trait Cache: Send + Sync {
    /// The cache's name, as configured.
    fn name(&self) -> &str;

    /// Returns the raw entry, or `None` on a miss or an expired entry.
    fn get_raw(&self, key: &str) -> impl std::future::Future<Output = Result<Option<String>>> + Send;

    /// Stores an entry. `ttl` of `None` means the cache's configured
    /// default, which may itself be "no expiry".
    fn put_raw(
        &self,
        key: &str,
        value: String,
        ttl: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Removes one entry. Returns whether it was there.
    fn evict(&self, key: &str) -> impl std::future::Future<Output = Result<bool>> + Send;

    /// Removes every entry in this cache.
    fn clear(&self) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// Object-safe façade over [`Cache`], which is what [`CacheManager`] hands
/// out: `Cache` uses `async fn`, so it cannot be made into a trait object
/// directly, and boxing the futures here keeps the ergonomic form for
/// implementors while still allowing `Arc<dyn DynCache>`.
pub trait DynCache: Send + Sync {
    fn name(&self) -> &str;
    fn get_raw<'a>(&'a self, key: &'a str) -> futures_box::BoxFuture<'a, Result<Option<String>>>;
    fn put_raw<'a>(
        &'a self,
        key: &'a str,
        value: String,
        ttl: Option<Duration>,
    ) -> futures_box::BoxFuture<'a, Result<()>>;
    fn evict<'a>(&'a self, key: &'a str) -> futures_box::BoxFuture<'a, Result<bool>>;
    fn clear(&self) -> futures_box::BoxFuture<'_, Result<()>>;
}

/// A hand-rolled `BoxFuture`, so this crate does not depend on `futures`
/// for one type alias.
pub mod futures_box {
    pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
}

impl<C: Cache> DynCache for C {
    fn name(&self) -> &str {
        Cache::name(self)
    }
    fn get_raw<'a>(&'a self, key: &'a str) -> futures_box::BoxFuture<'a, Result<Option<String>>> {
        Box::pin(Cache::get_raw(self, key))
    }
    fn put_raw<'a>(
        &'a self,
        key: &'a str,
        value: String,
        ttl: Option<Duration>,
    ) -> futures_box::BoxFuture<'a, Result<()>> {
        Box::pin(Cache::put_raw(self, key, value, ttl))
    }
    fn evict<'a>(&'a self, key: &'a str) -> futures_box::BoxFuture<'a, Result<bool>> {
        Box::pin(Cache::evict(self, key))
    }
    fn clear(&self) -> futures_box::BoxFuture<'_, Result<()>> {
        Box::pin(Cache::clear(self))
    }
}

/// Typed helpers over a [`DynCache`]. Free functions rather than trait
/// methods, because generic methods would make the trait non-object-safe
/// again.
pub async fn get<T: serde::de::DeserializeOwned>(cache: &dyn DynCache, key: &str) -> Result<Option<T>> {
    match cache.get_raw(key).await? {
        None => Ok(None),
        Some(raw) => {
            serde_json::from_str(&raw).map(Some).map_err(|source| Error::Deserialize { key: key.to_string(), source })
        }
    }
}

pub async fn put<T: serde::Serialize>(cache: &dyn DynCache, key: &str, value: &T, ttl: Option<Duration>) -> Result<()> {
    let raw = serde_json::to_string(value).map_err(Error::Serialize)?;
    cache.put_raw(key, raw, ttl).await
}

/// Which storage a named cache uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Backend {
    /// Process memory. Fastest, and *not* shared between replicas — an
    /// entry evicted on one pod stays live on the others until it expires.
    InMemory,
    /// Redis, via the `oxide-redis` starter. Shared across replicas, at the
    /// cost of a round trip.
    Redis,
}

/// Configuration for one named cache.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CacheSpec {
    #[serde(default)]
    pub backend: Option<Backend>,
    /// Entries expire after this long. Absent means no expiry, which for an
    /// in-memory cache means entries live until evicted or the process ends.
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
    /// In-memory only: the entry count above which the oldest entries are
    /// dropped. Without it a cache is an unbounded memory leak with extra
    /// steps.
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,
}

fn default_max_entries() -> usize {
    10_000
}

impl Default for CacheSpec {
    fn default() -> Self {
        Self { backend: None, ttl_seconds: None, max_entries: default_max_entries() }
    }
}

impl CacheSpec {
    pub fn ttl(&self) -> Option<Duration> {
        self.ttl_seconds.map(Duration::from_secs)
    }
}

/// Configuration bound to `oxide.cache`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.cache", default)]
#[serde(rename_all = "kebab-case")]
pub struct CacheProps {
    /// Backend for any cache not listed under `caches`.
    #[serde(default = "default_backend")]
    pub default_backend: Backend,
    /// Per-cache settings, keyed by cache name.
    #[serde(default)]
    pub caches: HashMap<String, CacheSpec>,
}

fn default_backend() -> Backend {
    // In-memory: it needs no infrastructure, so an application that adds
    // `#[cacheable]` works before anyone configures anything. A cache that
    // must be shared is an explicit decision.
    Backend::InMemory
}

impl Default for CacheProps {
    fn default() -> Self {
        Self { default_backend: default_backend(), caches: HashMap::new() }
    }
}

/// Builds a cache for a name and spec. Registered via `inventory` so
/// `oxide-redis` can supply the Redis backend without `oxide-cache`
/// depending on it -- the two are independent starters, and an application
/// using only in-memory caches should not pull in a Redis client.
pub struct CacheBackendFactory {
    pub backend: Backend,
    pub build: fn(&Context, &str, &CacheSpec) -> Option<Arc<dyn DynCache>>,
}

inventory::collect!(CacheBackendFactory);

inventory::submit! {
    CacheBackendFactory {
        backend: Backend::InMemory,
        build: |_ctx, name, spec| Some(Arc::new(memory::InMemoryCache::new(name.to_string(), spec))),
    }
}

/// Resolves caches by name. Injectable.
#[derive(Clone)]
pub struct CacheManager {
    /// Filled once at `post_construct`, when every component the backends
    /// need already exists. A `OnceLock` rather than a plain map because the
    /// manager is constructed before those components are -- and rather than
    /// a `Mutex`, because after boot this is read-only and read on every
    /// cached call.
    caches: Arc<std::sync::OnceLock<HashMap<String, Arc<dyn DynCache>>>>,
    /// Kept so a name absent from configuration can still be served.
    fallback: Arc<std::sync::Mutex<HashMap<String, Arc<dyn DynCache>>>>,
    props: Arc<CacheProps>,
    ctx: Arc<std::sync::OnceLock<Context>>,
}

impl CacheManager {
    /// The named cache, creating it from the default backend if it was not
    /// configured.
    ///
    /// Returns `None` only when the configured backend has no factory
    /// registered -- in practice, `backend: redis` without the `oxide-redis`
    /// starter linked in. That is a configuration error, and callers are
    /// expected to surface it rather than silently run without a cache.
    pub fn get(&self, name: &str) -> Option<Arc<dyn DynCache>> {
        if let Some(found) = self.caches.get().and_then(|m| m.get(name)) {
            return Some(Arc::clone(found));
        }
        let mut fallback = self.fallback.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(found) = fallback.get(name) {
            return Some(Arc::clone(found));
        }
        let spec = CacheSpec { backend: Some(self.props.default_backend), ..CacheSpec::default() };
        let built = self.build(name, &spec)?;
        fallback.insert(name.to_string(), Arc::clone(&built));
        Some(built)
    }

    fn build(&self, name: &str, spec: &CacheSpec) -> Option<Arc<dyn DynCache>> {
        let backend = spec.backend.unwrap_or(self.props.default_backend);
        let ctx = self.ctx.get()?;
        inventory::iter::<CacheBackendFactory>
            .into_iter()
            .find(|f| f.backend == backend)
            .and_then(|f| (f.build)(ctx, name, spec))
    }

    /// The names configured up front. A cache created on demand for an
    /// unconfigured name is not listed here.
    pub fn configured_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> =
            self.caches.get().map(|m| m.keys().map(String::as_str).collect()).unwrap_or_default();
        names.sort_unstable();
        names
    }
}

inventory::submit! {
    ComponentDescriptor::builder(
        "CacheManager",
        std::any::TypeId::of::<CacheManager>,
        |b| {
            let props = (*b.resolve::<CacheProps>()?).clone();
            b.insert(CacheManager {
                caches: Arc::new(std::sync::OnceLock::new()),
                fallback: Arc::new(std::sync::Mutex::new(HashMap::new())),
                props: Arc::new(props),
                ctx: Arc::new(std::sync::OnceLock::new()),
            });
            Ok(())
        },
    )
    .deps(|| vec![(std::any::TypeId::of::<CacheProps>(), "CacheProps")])
    // Caches are built at post_construct, not construct: a Redis-backed
    // cache needs `RedisTemplate`, which only exists once every component
    // has been constructed.
    .post_construct(|ctx: &Context| {
        let manager = ctx.get::<CacheManager>().expect("CacheManager exists");
        let ctx = ctx.clone();
        Box::pin(async move {
            let _ = manager.ctx.set(ctx);
            let mut built: HashMap<String, Arc<dyn DynCache>> = HashMap::new();
            for (name, spec) in &manager.props.caches {
                match manager.build(name, spec) {
                    Some(cache) => {
                        built.insert(name.clone(), cache);
                    }
                    None => {
                        let backend = spec.backend.unwrap_or(manager.props.default_backend);
                        return Err(oxide_core::ContextError::Construct {
                            type_name: "CacheManager",
                            message: format!(
                                "cache '{name}' asks for backend {backend:?}, but no factory is registered for it \
                                 -- is the starter that provides it linked into the binary?"
                            ),
                        }
                        .into());
                    }
                }
            }
            if !built.is_empty() {
                tracing::debug!(caches = built.len(), "cache manager ready");
            }
            // Set once; a second boot in the same process would get a fresh
            // manager, so losing this race is not possible.
            let _ = manager.caches.set(built);
            Ok(())
        })
    })
    .build()
}
