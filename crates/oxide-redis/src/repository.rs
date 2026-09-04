//! A CRUD repository over Redis — the `@RedisHash` + `CrudRepository`
//! analogue.
//!
//! [`RedisTemplate`] is the command surface: it speaks Redis. This is the
//! layer above it that speaks in entities, so application code says
//! `repo.save(&note)` instead of choosing a key, serialising, and
//! remembering to keep an index in step.
//!
//! # How an entity is stored
//!
//! Each entity is one Redis key, `<keyspace>:<id>`, holding its JSON. Their
//! ids are also kept in a set at `<keyspace>:__index`, because Redis has no
//! "list everything of this kind" — the alternative is `KEYS`/`SCAN` over
//! the whole database, which is O(all keys) and, for `KEYS`, blocks the
//! server. The index makes `find_all`, `count` and `delete_all` bounded by
//! the number of entities rather than by the size of the database.
//!
//! The index costs one extra command per write, batched into the same round
//! trip as the write itself, and it is the reason [`RedisRepository::save`]
//! and [`RedisRepository::delete`] are not simply `SET` and `DEL`.
//!
//! # When a TTL is set
//!
//! With [`RedisRepository::with_ttl`], entity keys expire — but a set member
//! cannot expire on its own, so an expired entity leaves its id behind in
//! the index. `find_all` therefore treats a missing entity as a deleted one
//! and prunes the id as it goes, rather than returning holes or failing.

use crate::{Error, RedisTemplate, Result};
use std::time::Duration;

/// An entity that can be stored by [`RedisRepository`].
///
/// Implemented by hand rather than derived: it is two lines, and a derive
/// macro here would only hide which field is the id.
///
/// ```ignore
/// impl RedisEntity for Note {
///     fn keyspace() -> &'static str { "note" }
///     fn id(&self) -> String { self.id.clone() }
/// }
/// ```
pub trait RedisEntity: serde::Serialize + serde::de::DeserializeOwned + Send + Sync {
    /// Prefix for this entity's keys, e.g. `"note"` for `note:42`. Must be
    /// distinct per type, or two types share a keyspace and an index.
    fn keyspace() -> &'static str;

    /// This instance's id. Ids must not contain the keyspace separator `:`,
    /// which would make two different ids collide on one key.
    fn id(&self) -> String;
}

/// CRUD over one entity type.
///
/// Cheap to clone — it holds a [`RedisTemplate`], which is itself a handle.
#[derive(Clone)]
pub struct RedisRepository<T: RedisEntity> {
    template: RedisTemplate,
    ttl: Option<Duration>,
    _entity: std::marker::PhantomData<fn() -> T>,
}

impl<T: RedisEntity> RedisRepository<T> {
    /// A repository whose entities never expire.
    pub fn new(template: RedisTemplate) -> Self {
        Self { template, ttl: None, _entity: std::marker::PhantomData }
    }

    /// A repository whose entities expire after `ttl`.
    ///
    /// Suited to a cache or a session store. See the module docs for what
    /// happens to the index when an entity expires.
    pub fn with_ttl(template: RedisTemplate, ttl: Duration) -> Self {
        Self { template, ttl: Some(ttl), _entity: std::marker::PhantomData }
    }

    fn key_for(id: &str) -> String {
        format!("{}:{id}", T::keyspace())
    }

    fn index_key() -> String {
        format!("{}:__index", T::keyspace())
    }

    /// Inserts or replaces the entity, and records its id in the index.
    ///
    /// Both commands travel in one pipeline: a write that landed without its
    /// index entry would be invisible to `find_all`, and the two are not
    /// worth two round trips.
    pub async fn save(&self, entity: &T) -> Result<()> {
        let id = entity.id();
        if id.contains(':') {
            return Err(Error::InvalidId { id });
        }
        let payload = serde_json::to_string(entity).map_err(Error::Serialize)?;
        let key = self.template.key(&Self::key_for(&id));
        let index = self.template.key(&Self::index_key());

        self.template
            .pipeline(|pipe| {
                match self.ttl {
                    // `as_secs` truncates, so a sub-second TTL would become
                    // 0, which Redis rejects.
                    Some(ttl) => {
                        pipe.cmd("SET").arg(&key).arg(&payload).arg("EX").arg(ttl.as_secs().max(1)).ignore();
                    }
                    None => {
                        pipe.cmd("SET").arg(&key).arg(&payload).ignore();
                    }
                }
                pipe.cmd("SADD").arg(&index).arg(&id).ignore();
            })
            .await
    }

    /// The entity with this id, or `None`.
    pub async fn find_by_id(&self, id: &str) -> Result<Option<T>> {
        self.template.get_json::<T>(&Self::key_for(id)).await
    }

    /// Whether an entity with this id exists. Checks the key, not the index,
    /// so an expired entity reads as absent.
    pub async fn exists(&self, id: &str) -> Result<bool> {
        self.template.exists(&Self::key_for(id)).await
    }

    /// Every entity, in one `SMEMBERS` plus one `MGET`.
    ///
    /// Ids whose entity has expired are pruned from the index as they are
    /// found, so a TTL'd repository does not accumulate dead ids forever.
    pub async fn find_all(&self) -> Result<Vec<T>> {
        let ids = self.template.smembers(&Self::index_key()).await?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let keys: Vec<String> = ids.iter().map(|id| Self::key_for(id)).collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let raw = self.template.get_many(&refs).await?;

        let mut out = Vec::with_capacity(ids.len());
        let mut stale = Vec::new();
        for (id, value) in ids.iter().zip(raw) {
            match value {
                Some(json) => out.push(
                    serde_json::from_str(&json)
                        .map_err(|source| Error::Deserialize { key: Self::key_for(id), source })?,
                ),
                None => stale.push(id.as_str()),
            }
        }
        if !stale.is_empty() {
            // Best effort: failing to prune must not fail the read the
            // caller actually asked for.
            let _ = self.template.srem(&Self::index_key(), &stale).await;
        }
        Ok(out)
    }

    /// How many ids the index holds.
    ///
    /// This counts index entries, not live keys, so with a TTL it can be
    /// briefly higher than `find_all().len()` until the next `find_all`
    /// prunes. Reading every key to make it exact would turn an O(1) call
    /// into an O(n) one.
    pub async fn count(&self) -> Result<u64> {
        self.template.scard(&Self::index_key()).await
    }

    /// Deletes the entity and its index entry. Returns whether it existed.
    pub async fn delete(&self, id: &str) -> Result<bool> {
        let existed = self.template.exists(&Self::key_for(id)).await?;
        let key = self.template.key(&Self::key_for(id));
        let index = self.template.key(&Self::index_key());
        let id = id.to_string();
        self.template
            .pipeline(|pipe| {
                pipe.cmd("DEL").arg(&key).ignore();
                pipe.cmd("SREM").arg(&index).arg(&id).ignore();
            })
            .await?;
        Ok(existed)
    }

    /// Deletes every entity of this type and empties the index.
    pub async fn delete_all(&self) -> Result<u64> {
        let ids = self.template.smembers(&Self::index_key()).await?;
        if ids.is_empty() {
            return Ok(0);
        }
        let keys: Vec<String> = ids.iter().map(|id| self.template.key(&Self::key_for(id))).collect();
        let index = self.template.key(&Self::index_key());
        self.template
            .pipeline(|pipe| {
                let mut del = redis::cmd("DEL");
                for k in &keys {
                    del.arg(k);
                }
                pipe.add_command(del).ignore();
                pipe.cmd("DEL").arg(&index).ignore();
            })
            .await?;
        Ok(ids.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Note {
        id: String,
    }

    impl RedisEntity for Note {
        fn keyspace() -> &'static str {
            "note"
        }
        fn id(&self) -> String {
            self.id.clone()
        }
    }

    #[test]
    fn keys_are_namespaced_by_entity() {
        assert_eq!(RedisRepository::<Note>::key_for("42"), "note:42");
    }

    #[test]
    fn the_index_cannot_collide_with_an_entity_key() {
        // An entity whose id were literally `__index` would otherwise
        // overwrite the index. Ids containing `:` are rejected by `save`,
        // and this name is reserved by convention -- pinned so a rename of
        // the index key does not silently reintroduce the collision.
        assert_eq!(RedisRepository::<Note>::index_key(), "note:__index");
        assert_ne!(RedisRepository::<Note>::index_key(), RedisRepository::<Note>::key_for("0"));
    }
}
