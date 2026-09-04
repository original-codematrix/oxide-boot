//! Test support for the MongoDB starter — the `@DataMongoTest` analogue.
//!
//! # Isolation
//!
//! One container is shared across the whole run, because starting a MongoDB
//! per test would dominate the suite. The isolation therefore has to happen
//! inside it, and it happens at the **database** level: every
//! [`MongoTest::start`] gets a database named uniquely for that test.
//!
//! Dropping a shared collection between tests — the obvious alternative —
//! deletes documents a concurrently running test just inserted, and produces
//! failures that only appear under `cargo test`'s default parallelism and
//! vanish under `--test-threads=1`. A database per test cannot do that.
//!
//! The database is dropped on [`MongoTest::stop`]; a test that just drops
//! the value leaves it behind, which costs nothing but a name in `show dbs`
//! until the container is removed.
//!
//! ```ignore
//! #[tokio::test(flavor = "multi_thread")]
//! async fn saves_and_reads_back() {
//!     let t = MongoTest::start().await;
//!     let notes = t.collection::<Note>("notes");
//!     notes.insert_one(Note { id: 1 }).await.unwrap();
//!     assert_eq!(notes.count_documents(doc! {}).await.unwrap(), 1);
//!     t.stop().await.unwrap();
//! }
//! ```

use mongodb::bson::doc;
use oxide_boot::{Application, OxideApplication};
use oxide_mongo::MongoClient;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub use oxide_test::containers::ContainerHandle;

/// Starts (or attaches to) the shared MongoDB container and returns its URI.
pub async fn mongo() -> ContainerHandle {
    oxide_test::containers::mongo().await
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A database name unique to this test, e.g. `oxide-test-91423-7`. Public so
/// a test that boots its own application can still get an isolated database.
pub fn unique_database() -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("oxide-test-{}-{n}", std::process::id())
}

/// A booted application wired to the shared MongoDB fixture, in its own
/// database.
pub struct MongoTest {
    app: Application,
    client: MongoClient,
    database: String,
    _dir: tempfile::TempDir,
}

impl MongoTest {
    /// Boots an application whose only configured starter is Mongo, on
    /// ephemeral HTTP ports, against a database unique to this test.
    pub async fn start() -> Self {
        Self::builder().start().await
    }

    pub fn builder() -> MongoTestBuilder {
        MongoTestBuilder { extra_yaml: String::new(), env: HashMap::new() }
    }

    /// The injected client, already pointed at this test's database.
    pub fn client(&self) -> &MongoClient {
        &self.client
    }

    /// A typed collection handle in this test's database.
    pub fn collection<T: Send + Sync>(&self, name: &str) -> mongodb::Collection<T> {
        self.client.collection(name).expect("mongo client is connected after boot")
    }

    /// This test's database name — needed only when connecting with a raw
    /// driver rather than through the injected client.
    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn app(&self) -> &Application {
        &self.app
    }

    /// Waits until the collection holds `expected` documents, or fails with
    /// what it actually found.
    ///
    /// Writes that a handler performs are not visible the instant its HTTP
    /// response returns — a test that asserts immediately is the classic
    /// flaky one. Polling to a deadline is the fix; a fixed sleep is either
    /// too short on a loaded machine or wasted time on an idle one.
    pub async fn await_count<T: Send + Sync>(&self, collection: &str, expected: u64, within: std::time::Duration) {
        let handle = self.collection::<T>(collection);
        let deadline = std::time::Instant::now() + within;
        let mut last = u64::MAX;
        loop {
            last = handle.count_documents(doc! {}).await.unwrap_or(last);
            if last == expected {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "collection '{collection}' held {last} documents, expected {expected}, after {within:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Shuts the application down and drops this test's database.
    pub async fn stop(self) -> Result<(), oxide_boot::BootError> {
        // Best effort: a failed drop leaves a stray database, which costs
        // nothing, and must not mask the assertion failure that may be the
        // reason this test is unwinding.
        if let Ok(raw) = self.client.raw() {
            let _ = raw.database(&self.database).drop().await;
        }
        self.app.stop().await
    }
}

/// Configures a [`MongoTest`] before it boots.
pub struct MongoTestBuilder {
    extra_yaml: String,
    env: HashMap<String, String>,
}

impl MongoTestBuilder {
    /// Appends YAML under the `oxide:` key. Indentation is the caller's.
    pub fn with_yaml(mut self, yaml: &str) -> Self {
        self.extra_yaml.push_str(yaml);
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub async fn start(self) -> MongoTest {
        let uri = mongo().await.endpoint;
        let database = unique_database();
        let dir = tempfile::tempdir().expect("tempdir for application.yml");

        let yaml = format!(
            "oxide:\n  application:\n    name: mongo-test\n  server:\n    port: 0\n  management:\n    port: 0\n  mongo:\n    uri: \"{uri}\"\n    database: \"{database}\"\n{}",
            self.extra_yaml
        );
        std::fs::write(dir.path().join("application.yml"), yaml).expect("write application.yml");

        let app = OxideApplication::builder()
            .base_dir(dir.path())
            .env(self.env)
            .start()
            .await
            .expect("boot an application with the mongo starter");

        let client = (*app
            .context()
            .get::<MongoClient>()
            .expect("MongoClient is registered — is `oxide-mongo` linked into the test binary?"))
        .clone();

        MongoTest { app, client, database, _dir: dir }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_names_are_unique_within_a_process() {
        assert_ne!(unique_database(), unique_database());
    }

    #[test]
    fn database_names_are_valid_mongo_identifiers() {
        let name = unique_database();
        // Mongo rejects /\. "$*<>:|? and names over 63 bytes.
        assert!(name.len() < 64, "too long: {name}");
        assert!(!name.contains(['/', '\\', '.', ' ', '"', '$', '*', '<', '>', ':', '|', '?']), "invalid: {name}");
    }
}
