//! Test support for the Redis starter — the `@DataRedisTest` analogue.
//!
//! [`oxide_test`] boots an application; this adds the part that is specific
//! to testing against Redis, and the part that is easy to get wrong.
//!
//! # Isolation
//!
//! The container is shared across every test in the run — starting one
//! Dragonfly per test would dominate the suite's runtime. Shared state is
//! then the hazard: two tests writing `note:1` interfere, and a `FLUSHALL`
//! for cleanliness wipes whatever a concurrently running test just wrote.
//!
//! [`RedisTest::start`] answers that with a **unique key prefix per test**
//! rather than a shared namespace and a flush. Every test gets its own
//! keyspace inside one server, so tests never collide and never need to
//! clean up after each other. `RedisTemplate` applies the prefix itself, so
//! test code writes plain key names and is unaware of it.
//!
//! ```ignore
//! #[tokio::test(flavor = "multi_thread")]
//! async fn caches_the_note() {
//!     let t = RedisTest::start().await;
//!     t.template().set("note:1", "cached", None).await.unwrap();
//!     assert_eq!(t.template().get("note:1").await.unwrap().as_deref(), Some("cached"));
//! }
//! ```

use oxide_boot::{Application, OxideApplication};
use oxide_redis::RedisTemplate;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub use oxide_test::containers::ContainerHandle;

/// Starts (or attaches to) the shared Dragonfly container and returns its
/// connection URL. Dragonfly speaks the Redis wire protocol; it is what the
/// starter is tested and benchmarked against.
pub async fn dragonfly() -> ContainerHandle {
    oxide_test::containers::dragonfly().await
}

/// Monotonic within a process; combined with the process id below so two
/// test binaries running concurrently cannot land on the same prefix.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique key prefix, e.g. `t-91423-7:`. Public so a test that boots its
/// own application (rather than using [`RedisTest`]) can still get an
/// isolated keyspace.
pub fn unique_prefix() -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("t-{}-{n}:", std::process::id())
}

/// A booted application wired to the shared Redis fixture, in its own
/// keyspace.
pub struct RedisTest {
    app: Application,
    template: RedisTemplate,
    prefix: String,
    /// Kept so the generated `application.yml` outlives the application that
    /// reads it.
    _dir: tempfile::TempDir,
}

impl RedisTest {
    /// Boots an application whose only configured starter is Redis, on
    /// ephemeral HTTP ports, with a key prefix unique to this test.
    pub async fn start() -> Self {
        Self::builder().start().await
    }

    /// For tests that need extra configuration — another starter, a
    /// different command timeout — while keeping the fixture and the
    /// isolated prefix.
    pub fn builder() -> RedisTestBuilder {
        RedisTestBuilder { extra_yaml: String::new(), env: HashMap::new() }
    }

    /// The injected template, already prefixed for this test.
    pub fn template(&self) -> &RedisTemplate {
        &self.template
    }

    /// The key prefix this test's template applies. Only needed when
    /// bypassing the template — a raw driver connection, say — since every
    /// `RedisTemplate` call prefixes on its own.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The running application, for resolving other components.
    pub fn app(&self) -> &Application {
        &self.app
    }

    /// Shuts the application down, running `pre_destroy` hooks. Optional:
    /// dropping the value is enough for a test, but calling this exercises
    /// the shutdown path, which is worth doing at least once per suite.
    pub async fn stop(self) -> Result<(), oxide_boot::BootError> {
        self.app.stop().await
    }
}

/// Configures a [`RedisTest`] before it boots.
pub struct RedisTestBuilder {
    extra_yaml: String,
    env: HashMap<String, String>,
}

impl RedisTestBuilder {
    /// Appends YAML under the `oxide:` key, e.g.
    /// `"  mongo:\n    uri: \"...\"\n"`. Indentation is the caller's, so a
    /// misindented block fails at boot with a parse error naming the file.
    pub fn with_yaml(mut self, yaml: &str) -> Self {
        self.extra_yaml.push_str(yaml);
        self
    }

    /// An environment override, applied on top of the YAML exactly as it
    /// would be in production.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub async fn start(self) -> RedisTest {
        let url = dragonfly().await.endpoint;
        let prefix = unique_prefix();
        let dir = tempfile::tempdir().expect("tempdir for application.yml");

        // Port 0 on both servers: test binaries run concurrently, and the
        // documented defaults (8080/8081) would collide with each other and
        // with a running load-test stack.
        let yaml = format!(
            "oxide:\n  application:\n    name: redis-test\n  server:\n    port: 0\n  management:\n    port: 0\n  redis:\n    url: \"{url}\"\n    key-prefix: \"{prefix}\"\n{}",
            self.extra_yaml
        );
        std::fs::write(dir.path().join("application.yml"), yaml).expect("write application.yml");

        let app = OxideApplication::builder()
            .base_dir(dir.path())
            .env(self.env)
            .start()
            .await
            .expect("boot an application with the redis starter");

        let template = (*app
            .context()
            .get::<RedisTemplate>()
            .expect("RedisTemplate is registered — is `oxide-redis` linked into the test binary?"))
        .clone();

        RedisTest { app, template, prefix, _dir: dir }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_are_unique_within_a_process() {
        let a = unique_prefix();
        let b = unique_prefix();
        assert_ne!(a, b);
        // The process id is in there so two binaries running at once cannot
        // collide either.
        assert!(a.contains(&std::process::id().to_string()), "prefix was {a}");
    }
}
