//! In-process cache backend.
//!
//! Sharded rather than one big map: a cache sits on the hot path of every
//! cached call, and a single lock across all keys would serialise those
//! calls across every worker thread — the same mistake that cost 5% in the
//! observability middleware. Sharding by key hash means threads touching
//! different keys do not contend at all.
//!
//! Eviction is approximate-LRU by insertion order within a shard, triggered
//! only when a shard exceeds its share of `max-entries`. Exact LRU needs a
//! write on every *read* to move an entry to the front, which turns reads
//! into contended writes; for a cache that is a bad trade.

use crate::{Cache, CacheSpec, Result};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Number of independent shards. A power of two so the index is a mask
/// rather than a division, and comfortably above the core count of a typical
/// node so threads rarely land on the same shard.
const SHARDS: usize = 32;

struct Entry {
    value: String,
    /// Absent means no expiry.
    expires_at: Option<Instant>,
    /// Monotonic per shard; the lowest is evicted first.
    inserted_seq: u64,
}

struct Shard {
    entries: HashMap<String, Entry>,
    next_seq: u64,
}

pub struct InMemoryCache {
    name: String,
    shards: Vec<RwLock<Shard>>,
    default_ttl: Option<Duration>,
    /// Per shard, derived from the configured total.
    max_per_shard: usize,
}

impl InMemoryCache {
    pub fn new(name: String, spec: &CacheSpec) -> Self {
        Self {
            name,
            shards: (0..SHARDS).map(|_| RwLock::new(Shard { entries: HashMap::new(), next_seq: 0 })).collect(),
            default_ttl: spec.ttl(),
            // At least one per shard: a `max-entries` below the shard count
            // would otherwise round down to zero and evict everything
            // immediately.
            max_per_shard: (spec.max_entries / SHARDS).max(1),
        }
    }

    fn shard_for(&self, key: &str) -> &RwLock<Shard> {
        // `DefaultHasher::new()` is unseeded, so the same key hashes to the
        // same shard for the life of the process -- which is what a shard
        // index needs. `RandomState` would re-seed per instance and send a
        // key to a different shard on every call, so every read would miss.
        let mut hasher = std::hash::DefaultHasher::new();
        std::hash::Hash::hash(key, &mut hasher);
        let index = (std::hash::Hasher::finish(&hasher) as usize) & (SHARDS - 1);
        &self.shards[index]
    }

    /// Total live entries, expired ones included until they are touched.
    /// Test and diagnostics only -- it locks every shard.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().unwrap_or_else(|p| p.into_inner()).entries.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Cache for InMemoryCache {
    fn name(&self) -> &str {
        &self.name
    }

    async fn get_raw(&self, key: &str) -> Result<Option<String>> {
        let shard = self.shard_for(key);
        // Read lock first: the overwhelmingly common case is a live hit or a
        // clean miss, neither of which needs to mutate anything.
        {
            let guard = shard.read().unwrap_or_else(|p| p.into_inner());
            match guard.entries.get(key) {
                None => return Ok(None),
                Some(entry) => {
                    if entry.expires_at.is_none_or(|at| at > Instant::now()) {
                        return Ok(Some(entry.value.clone()));
                    }
                }
            }
        }
        // Expired: drop it so the memory is reclaimed rather than waiting
        // for eviction pressure.
        let mut guard = shard.write().unwrap_or_else(|p| p.into_inner());
        guard.entries.remove(key);
        Ok(None)
    }

    async fn put_raw(&self, key: &str, value: String, ttl: Option<Duration>) -> Result<()> {
        let ttl = ttl.or(self.default_ttl);
        let shard = self.shard_for(key);
        let mut guard = shard.write().unwrap_or_else(|p| p.into_inner());

        let seq = guard.next_seq;
        guard.next_seq += 1;
        guard
            .entries
            .insert(key.to_string(), Entry { value, expires_at: ttl.map(|d| Instant::now() + d), inserted_seq: seq });

        while guard.entries.len() > self.max_per_shard {
            // Evict the oldest insertion in this shard. Linear in shard
            // size, but it runs only when a shard is over its limit, and a
            // shard holds `max_entries / 32`.
            let Some(oldest) = guard.entries.iter().min_by_key(|(_, e)| e.inserted_seq).map(|(k, _)| k.clone()) else {
                break;
            };
            guard.entries.remove(&oldest);
        }
        Ok(())
    }

    async fn evict(&self, key: &str) -> Result<bool> {
        let shard = self.shard_for(key);
        let mut guard = shard.write().unwrap_or_else(|p| p.into_inner());
        Ok(guard.entries.remove(key).is_some())
    }

    async fn clear(&self) -> Result<()> {
        for shard in &self.shards {
            shard.write().unwrap_or_else(|p| p.into_inner()).entries.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(spec: CacheSpec) -> InMemoryCache {
        InMemoryCache::new("test".into(), &spec)
    }

    #[tokio::test]
    async fn stores_and_reads_back() {
        let c = cache(CacheSpec::default());
        c.put_raw("k", "v".into(), None).await.unwrap();
        assert_eq!(c.get_raw("k").await.unwrap().as_deref(), Some("v"));
    }

    #[tokio::test]
    async fn a_missing_key_is_none() {
        let c = cache(CacheSpec::default());
        assert_eq!(c.get_raw("never").await.unwrap(), None);
    }

    #[tokio::test]
    async fn an_expired_entry_reads_as_a_miss_and_is_dropped() {
        let c = cache(CacheSpec::default());
        c.put_raw("k", "v".into(), Some(Duration::from_millis(20))).await.unwrap();
        assert_eq!(c.len(), 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(c.get_raw("k").await.unwrap(), None);
        // Not merely reported as missing: the memory is released, otherwise
        // an expiring cache grows forever under a changing key set.
        assert_eq!(c.len(), 0, "the expired entry was left in the map");
    }

    #[tokio::test]
    async fn a_per_entry_ttl_overrides_the_cache_default() {
        let c = cache(CacheSpec { ttl_seconds: Some(3600), ..CacheSpec::default() });
        c.put_raw("short", "v".into(), Some(Duration::from_millis(20))).await.unwrap();
        c.put_raw("long", "v".into(), None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(c.get_raw("short").await.unwrap(), None);
        assert_eq!(c.get_raw("long").await.unwrap().as_deref(), Some("v"), "the default TTL was not applied");
    }

    #[tokio::test]
    async fn eviction_bounds_the_cache() {
        // One entry per shard, so 32 in total.
        let c = cache(CacheSpec { max_entries: SHARDS, ..CacheSpec::default() });
        for i in 0..500 {
            c.put_raw(&format!("k{i}"), "v".into(), None).await.unwrap();
        }
        // Eviction is per shard and hashing is uneven, so the bound is
        // "does not grow without limit", not an exact count.
        assert!(c.len() <= SHARDS, "cache held {} entries with a limit of {SHARDS}", c.len());
        assert!(!c.is_empty(), "eviction emptied the cache entirely");
    }

    #[tokio::test]
    async fn max_entries_below_the_shard_count_still_stores_something() {
        // 4 / 32 rounds to zero; without the floor of one per shard this
        // cache would evict every entry the moment it was written.
        let c = cache(CacheSpec { max_entries: 4, ..CacheSpec::default() });
        c.put_raw("k", "v".into(), None).await.unwrap();
        assert_eq!(c.get_raw("k").await.unwrap().as_deref(), Some("v"));
    }

    #[tokio::test]
    async fn evict_and_clear_remove_entries() {
        let c = cache(CacheSpec::default());
        c.put_raw("a", "1".into(), None).await.unwrap();
        c.put_raw("b", "2".into(), None).await.unwrap();
        assert!(c.evict("a").await.unwrap());
        assert!(!c.evict("a").await.unwrap(), "evicting twice reports false");
        assert_eq!(c.len(), 1);
        c.clear().await.unwrap();
        assert!(c.is_empty());
    }

    #[tokio::test]
    async fn the_same_key_always_lands_on_the_same_shard() {
        let c = cache(CacheSpec::default());
        // If the shard index were not stable, a write and the read after it
        // would look in different shards and every lookup would miss.
        for i in 0..200 {
            let key = format!("key-{i}");
            c.put_raw(&key, format!("v{i}"), None).await.unwrap();
            assert_eq!(c.get_raw(&key).await.unwrap(), Some(format!("v{i}")), "key {key} moved shard");
        }
    }
}
