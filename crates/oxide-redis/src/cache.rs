//! Redis as a backend for `oxide-cache`.
//!
//! Registered through `oxide-cache`'s factory seam, so `oxide-cache` never
//! depends on a Redis client: an application whose caches are all in-memory
//! does not pull one in. The wiring only exists because this crate is linked.
//!
//! Keys are namespaced per cache (`cache:<name>:<key>`) on top of whatever
//! `oxide.redis.key-prefix` already applies, so two caches cannot collide
//! and neither can a cache and ordinary application data.

use crate::RedisTemplate;
use oxide_cache::{Backend, Cache, CacheBackendFactory, CacheSpec};
use std::time::Duration;

pub struct RedisCache {
    name: String,
    template: RedisTemplate,
    default_ttl: Option<Duration>,
}

impl RedisCache {
    pub fn new(name: String, template: RedisTemplate, spec: &CacheSpec) -> Self {
        Self { name, template, default_ttl: spec.ttl() }
    }

    fn key(&self, key: &str) -> String {
        format!("cache:{}:{key}", self.name)
    }

    /// The set holding this cache's keys, so `clear` can remove exactly its
    /// own entries. Redis has no "delete by prefix"; the alternatives are
    /// `KEYS`, which blocks the server, or `SCAN`, which is O(all keys).
    fn index(&self) -> String {
        format!("cache:{}:__keys", self.name)
    }
}

impl Cache for RedisCache {
    fn name(&self) -> &str {
        &self.name
    }

    async fn get_raw(&self, key: &str) -> oxide_cache::Result<Option<String>> {
        self.template.get(&self.key(key)).await.map_err(|e| oxide_cache::Error::Backend(e.to_string()))
    }

    async fn put_raw(&self, key: &str, value: String, ttl: Option<Duration>) -> oxide_cache::Result<()> {
        let ttl = ttl.or(self.default_ttl);
        let full = self.template.key(&self.key(key));
        let index = self.template.key(&self.index());
        let member = key.to_string();
        // Value and index entry in one round trip: an entry missing from the
        // index would survive `clear`.
        self.template
            .pipeline(|pipe| {
                match ttl {
                    // `as_secs` truncates, so a sub-second TTL would become
                    // 0, which Redis rejects.
                    Some(d) => {
                        pipe.cmd("SET").arg(&full).arg(&value).arg("EX").arg(d.as_secs().max(1)).ignore();
                    }
                    None => {
                        pipe.cmd("SET").arg(&full).arg(&value).ignore();
                    }
                }
                pipe.cmd("SADD").arg(&index).arg(&member).ignore();
            })
            .await
            .map_err(|e| oxide_cache::Error::Backend(e.to_string()))
    }

    async fn evict(&self, key: &str) -> oxide_cache::Result<bool> {
        let existed =
            self.template.delete(&self.key(key)).await.map_err(|e| oxide_cache::Error::Backend(e.to_string()))?;
        let _ = self.template.srem(&self.index(), &[key]).await;
        Ok(existed)
    }

    async fn clear(&self) -> oxide_cache::Result<()> {
        let keys =
            self.template.smembers(&self.index()).await.map_err(|e| oxide_cache::Error::Backend(e.to_string()))?;
        if keys.is_empty() {
            return Ok(());
        }
        let full: Vec<String> = keys.iter().map(|k| self.template.key(&self.key(k))).collect();
        let index = self.template.key(&self.index());
        self.template
            .pipeline(|pipe| {
                let mut del = redis::cmd("DEL");
                for k in &full {
                    del.arg(k);
                }
                pipe.add_command(del).ignore();
                pipe.cmd("DEL").arg(&index).ignore();
            })
            .await
            .map_err(|e| oxide_cache::Error::Backend(e.to_string()))
    }
}

inventory::submit! {
    CacheBackendFactory {
        backend: Backend::Redis,
        // `None` when `RedisTemplate` is absent, which `CacheManager` turns
        // into a boot error naming the cache -- better than a cache that
        // silently never hits.
        build: |ctx, name, spec| {
            let template = (*ctx.get::<RedisTemplate>()?).clone();
            Some(std::sync::Arc::new(RedisCache::new(name.to_string(), template, spec)))
        },
    }
}
