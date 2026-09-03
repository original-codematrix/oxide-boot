//! Redis configuration and the `RedisTemplate` lifecycle component.

use crate::{Error, Result};
use oxide_core::{ComponentDescriptor, Context};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Configuration bound to the `oxide.redis` prefix. `url` is required
/// (integration starter: missing section ⇒ fail-fast).
#[derive(Clone, serde::Serialize, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.redis")]
#[serde(rename_all = "kebab-case")]
pub struct RedisProps {
    pub url: String,
    /// Per-command budget. Every call in `RedisTemplate` is wrapped in this,
    /// so a wedged server surfaces as `Error::Timeout` instead of hanging a
    /// request until the client gives up.
    #[serde(default = "default_command_timeout_ms")]
    pub command_timeout_ms: u64,
    /// Applied to the initial connect at `post_construct` and to the
    /// reconnects `ConnectionManager` performs afterwards.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Number of reconnect attempts before `ConnectionManager` reports the
    /// connection as failed. It keeps retrying after that on the next command.
    #[serde(default = "default_max_reconnect_attempts")]
    pub max_reconnect_attempts: usize,
    /// Prepended to every key, so several apps can share one server without
    /// colliding. Empty by default.
    #[serde(default)]
    pub key_prefix: String,
    /// PING the server at boot.
    #[serde(default = "default_true")]
    pub ping_on_start: bool,
}

// `url` can embed credentials (`redis://user:pass@host`), so `Debug` is
// hand-written to redact it rather than derived, which would print it into
// logs verbatim.
impl std::fmt::Debug for RedisProps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisProps")
            .field("url", &"<redacted>")
            .field("command_timeout_ms", &self.command_timeout_ms)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("max_reconnect_attempts", &self.max_reconnect_attempts)
            .field("key_prefix", &self.key_prefix)
            .field("ping_on_start", &self.ping_on_start)
            .finish()
    }
}

impl Default for RedisProps {
    fn default() -> Self {
        Self {
            url: String::new(),
            command_timeout_ms: default_command_timeout_ms(),
            connect_timeout_ms: default_connect_timeout_ms(),
            max_reconnect_attempts: default_max_reconnect_attempts(),
            key_prefix: String::new(),
            ping_on_start: default_true(),
        }
    }
}

fn default_command_timeout_ms() -> u64 {
    2_000
}
fn default_connect_timeout_ms() -> u64 {
    5_000
}
fn default_max_reconnect_attempts() -> usize {
    6
}
fn default_true() -> bool {
    true
}

/// Injectable handle to Redis. Cheap to clone: `ConnectionManager` is itself
/// an `Arc`-backed multiplexed connection that reconnects on its own, so
/// there is no pool to check out of and nothing to return.
///
/// The connection is established at `post_construct` because connecting is
/// async, mirroring `MongoClient` in the Mongo starter.
#[derive(Clone)]
pub struct RedisTemplate {
    url: String,
    key_prefix: String,
    command_timeout: Duration,
    connect_timeout: Duration,
    max_reconnect_attempts: usize,
    manager: Arc<OnceLock<ConnectionManager>>,
}

impl RedisTemplate {
    /// The raw connection, or an error if boot hasn't finished yet. Use this
    /// for commands the template doesn't wrap; note that it is *not* covered
    /// by `command_timeout_ms`.
    pub fn raw(&self) -> Result<ConnectionManager> {
        self.manager.get().cloned().ok_or(Error::NotStarted)
    }

    /// The configured key prefix (empty when unset).
    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    /// The fully qualified key for `key`, prefix applied.
    pub fn key(&self, key: &str) -> String {
        if self.key_prefix.is_empty() { key.to_string() } else { format!("{}{key}", self.key_prefix) }
    }

    /// Runs `f` under the configured per-command timeout.
    async fn timed<T, F>(&self, f: F) -> Result<T>
    where
        F: std::future::Future<Output = std::result::Result<T, redis::RedisError>>,
    {
        match tokio::time::timeout(self.command_timeout, f).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(Error::Redis(e)),
            Err(_) => Err(Error::Timeout(self.command_timeout)),
        }
    }

    /// `GET key`, as a string. `None` when the key does not exist.
    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.raw()?;
        let key = self.key(key);
        self.timed(async move { conn.get::<_, Option<String>>(&key).await }).await
    }

    /// `MGET key...`, in one round trip. `None` for each key that does not
    /// exist; the result is positionally aligned with `keys`.
    ///
    /// Worth reaching for whenever a handler reads several keys: three
    /// sequential `get` calls are three round trips, and on a laptop where
    /// Redis is a container that cost dominates the handler.
    pub async fn get_many(&self, keys: &[&str]) -> Result<Vec<Option<String>>> {
        if keys.is_empty() {
            // MGET with no arguments is a protocol error, and "nothing asked
            // for" has an obvious answer that does not need the server.
            return Ok(Vec::new());
        }
        let mut conn = self.raw()?;
        let keys: Vec<String> = keys.iter().map(|k| self.key(k)).collect();
        self.timed(async move { conn.mget::<_, Vec<Option<String>>>(&keys).await }).await
    }

    /// Runs several commands in one round trip and discards their replies.
    ///
    /// The closure receives redis-rs's own pipeline builder, so anything the
    /// driver can express is available; `RedisTemplate`'s key prefix is *not*
    /// applied to keys named in there, so use [`RedisTemplate::key`] for them.
    /// Use this for write batches -- counters, evictions, fan-out `SET`s --
    /// where the values are not needed back.
    pub async fn pipeline<F>(&self, build: F) -> Result<()>
    where
        F: FnOnce(&mut redis::Pipeline),
    {
        let mut conn = self.raw()?;
        let mut pipe = redis::pipe();
        build(&mut pipe);
        self.timed(async move { pipe.query_async::<()>(&mut conn).await }).await
    }

    /// `SET key value`, optionally with `EX ttl`.
    pub async fn set(&self, key: &str, value: &str, ttl: Option<Duration>) -> Result<()> {
        let mut conn = self.raw()?;
        let key = self.key(key);
        let value = value.to_string();
        self.timed(async move {
            match ttl {
                // `as_secs` truncates, so a sub-second TTL would become 0 —
                // which Redis rejects. Clamp to at least one second.
                Some(ttl) => conn.set_ex::<_, _, ()>(&key, value, ttl.as_secs().max(1)).await,
                None => conn.set::<_, _, ()>(&key, value).await,
            }
        })
        .await
    }

    /// `GET key`, deserialized from JSON. `None` when the key does not exist.
    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let Some(raw) = self.get(key).await? else { return Ok(None) };
        serde_json::from_str(&raw).map(Some).map_err(|source| Error::Deserialize { key: self.key(key), source })
    }

    /// `SET key <json>`, optionally with `EX ttl`.
    pub async fn set_json<T: serde::Serialize>(&self, key: &str, value: &T, ttl: Option<Duration>) -> Result<()> {
        let raw = serde_json::to_string(value).map_err(Error::Serialize)?;
        self.set(key, &raw, ttl).await
    }

    /// `DEL key`. Returns whether the key existed.
    pub async fn delete(&self, key: &str) -> Result<bool> {
        let mut conn = self.raw()?;
        let key = self.key(key);
        let removed: u64 = self.timed(async move { conn.del::<_, u64>(&key).await }).await?;
        Ok(removed > 0)
    }

    /// `EXISTS key`.
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let mut conn = self.raw()?;
        let key = self.key(key);
        self.timed(async move { conn.exists::<_, bool>(&key).await }).await
    }

    /// `INCRBY key delta`, returning the new value.
    pub async fn increment(&self, key: &str, delta: i64) -> Result<i64> {
        let mut conn = self.raw()?;
        let key = self.key(key);
        self.timed(async move { conn.incr::<_, _, i64>(&key, delta).await }).await
    }

    /// `EXPIRE key ttl`. Returns whether the key existed.
    pub async fn expire(&self, key: &str, ttl: Duration) -> Result<bool> {
        let mut conn = self.raw()?;
        let key = self.key(key);
        self.timed(async move { conn.expire::<_, bool>(&key, ttl.as_secs().max(1) as i64).await }).await
    }

    /// `PING` (used by the health indicator).
    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.raw()?;
        self.timed(async move { redis::cmd("PING").query_async::<()>(&mut conn).await }).await
    }

    fn new(props: &RedisProps) -> Self {
        Self {
            url: props.url.clone(),
            key_prefix: props.key_prefix.clone(),
            command_timeout: Duration::from_millis(props.command_timeout_ms),
            connect_timeout: Duration::from_millis(props.connect_timeout_ms),
            max_reconnect_attempts: props.max_reconnect_attempts,
            manager: Arc::new(OnceLock::new()),
        }
    }

    async fn connect(&self) -> Result<()> {
        if self.manager.get().is_some() {
            return Ok(());
        }
        let client = redis::Client::open(self.url.as_str())?;
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_connection_timeout(Some(self.connect_timeout))
            .set_response_timeout(Some(self.command_timeout))
            .set_number_of_retries(self.max_reconnect_attempts);
        let manager = ConnectionManager::new_with_config(client, config).await?;
        let _ = self.manager.set(manager);
        Ok(())
    }
}

inventory::submit! {
    ComponentDescriptor::builder(
        "RedisTemplate",
        std::any::TypeId::of::<RedisTemplate>,
        |b| {
            // `?` rather than `expect`: `resolve` failing here would mean the
            // `deps` declaration below and `assemble`'s dependency check
            // disagree, which belongs in this closure's `ContextError`, not
            // in a panic.
            let props = b.resolve::<RedisProps>()?;
            b.insert(RedisTemplate::new(&props));
            Ok(())
        },
    )
    .deps(|| vec![(std::any::TypeId::of::<RedisProps>(), "RedisProps")])
    // Safe by construction: `assemble` runs every `construct` closure before
    // any `post_construct` hook, so both entries are in the context here.
    .post_construct(|ctx: &Context| {
        let template = ctx.get::<RedisTemplate>().expect("RedisTemplate exists");
        let props = ctx.get::<RedisProps>().expect("RedisProps exists").clone();
        Box::pin(async move {
            template.connect().await.map_err(|e| {
                // The driver's error text can echo the URL back, credentials
                // included, so it goes to the log and the propagated message
                // stays generic.
                tracing::error!(error = %e, "redis connection failed");
                oxide_core::ContextError::Construct {
                    type_name: "RedisTemplate",
                    message: "failed to connect to Redis".to_string(),
                }
            })?;
            // A failed boot PING is logged, not fatal: the connection manager
            // reconnects on its own, and a broker-style hard failure at boot
            // would crashloop the pod for what the health indicator already
            // reports. Same policy as the Mongo starter.
            if props.ping_on_start
                && let Err(e) = template.ping().await
            {
                tracing::error!(error = %e, "redis ping at boot failed");
            }
            Ok(())
        })
    })
    .build()
}
