//! `oxide-test`: spring-boot-test-style harness for Oxide Boot applications.
//!
//! Boots the application under test on ephemeral ports (so tests don't collide)
//! and exposes a preconfigured HTTP client for the main + management servers.
//! Component overriding (`replace::<T>(mock)`) and testcontainers fixtures are
//! the roadmap; the V1 core covers the squeeze: boot + port-0 + test client.

use oxide_boot::{Application, BootError, OxideApplication};
use oxide_core::Context;
use oxide_web::{ManagementServer, WebServer};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub use oxide_test_macros::oxide_test;

#[cfg(feature = "containers")]
pub mod containers;

/// Entry point for booting an application in tests.
pub struct TestApp;

impl TestApp {
    pub fn builder() -> TestAppBuilder {
        TestAppBuilder { base_dir: PathBuf::from("."), env: HashMap::new(), overrides: Vec::new() }
    }
}

/// Configures and starts an application under test.
pub struct TestAppBuilder {
    base_dir: PathBuf,
    env: HashMap<String, String>,
    /// Applied to the underlying `AppBuilder` in registration order. Boxed
    /// because each closure captures a different concrete type.
    overrides: Vec<Box<dyn FnOnce(oxide_boot::AppBuilder) -> oxide_boot::AppBuilder + Send>>,
}

impl TestAppBuilder {
    /// The working directory containing `application.yml` (default `.`).
    pub fn base_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.base_dir = dir.into();
        self
    }

    /// Adds an env override (defaults to `OXIDE_SERVER_PORT=0` and
    /// `OXIDE_MANAGEMENT_PORT=0` so tests use ephemeral ports).
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    /// Activates the `test` profile (`OXIDE_PROFILES_ACTIVE=test`), so
    /// `application-test.yml` is layered on top of `application.yml` — the
    /// `@SpringBootTest(testProfile)` analogue. Used by `#[oxide_test]`.
    pub fn test_profile(mut self) -> Self {
        self.env.insert("OXIDE_PROFILES_ACTIVE".to_string(), "test".to_string());
        self
    }

    /// Supplies `instance` in place of the component the container would
    /// have built for `T` — the `@MockBean` equivalent.
    ///
    /// The real component is not constructed at all, so a replaced Kafka
    /// listener does not subscribe and a replaced client opens no
    /// connection. Everything that depends on `T` gets the replacement.
    ///
    /// ```ignore
    /// let app = TestApp::builder().replace(FakeClock::at(NOON)).start().await?;
    /// ```
    pub fn replace<T: Send + Sync + 'static>(mut self, instance: T) -> Self {
        self.overrides.push(Box::new(move |b: oxide_boot::AppBuilder| b.replace(instance)));
        self
    }

    /// Starts the application with the configured overrides.
    pub async fn start(self) -> Result<TestApplication, BootError> {
        let mut env = HashMap::from([
            ("OXIDE_SERVER_PORT".to_string(), "0".to_string()),
            ("OXIDE_MANAGEMENT_PORT".to_string(), "0".to_string()),
        ]);
        env.extend(self.env);
        let mut builder = OxideApplication::builder().base_dir(&self.base_dir).env(env);
        for apply in self.overrides {
            builder = apply(builder);
        }
        let app = builder.start().await?;
        Ok(TestApplication { app })
    }
}

/// A running application under test, with a preconfigured HTTP client.
pub struct TestApplication {
    app: Application,
}

impl TestApplication {
    /// The underlying DI context (grab injections like `WebServer`, repos...).
    pub fn context(&self) -> &Context {
        self.app.context()
    }

    /// The bound local address of the main HTTP server.
    pub fn main_addr(&self) -> Option<std::net::SocketAddr> {
        self.app.context().get::<WebServer>().and_then(|w| w.local_addr())
    }

    /// The bound local address of the management server.
    pub fn management_addr(&self) -> Option<std::net::SocketAddr> {
        self.app.context().get::<ManagementServer>().and_then(|m| m.local_addr())
    }

    /// A reqwest client preconfigured for the main server.
    pub fn client(&self) -> TestWebClient {
        TestWebClient {
            client: reqwest::Client::new(),
            base: self.main_addr().map(|a| format!("http://{a}")).unwrap_or_default(),
        }
    }

    /// A reqwest client preconfigured for the management server.
    pub fn management_client(&self) -> TestWebClient {
        TestWebClient {
            client: reqwest::Client::new(),
            base: self.management_addr().map(|a| format!("http://{a}")).unwrap_or_default(),
        }
    }

    /// Stops the application (drains servers, flushes online hooks).
    pub async fn stop(self) -> Result<(), BootError> {
        self.app.stop().await
    }
}

/// A thin wrapper over `reqwest::Client`, address-bound to a server under test.
pub struct TestWebClient {
    client: reqwest::Client,
    base: String,
}

impl TestWebClient {
    pub fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.client.get(format!("{}{path}", self.base))
    }
    pub fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.client.post(format!("{}{path}", self.base))
    }
    pub fn put(&self, path: &str) -> reqwest::RequestBuilder {
        self.client.put(format!("{}{path}", self.base))
    }
    pub fn delete(&self, path: &str) -> reqwest::RequestBuilder {
        self.client.delete(format!("{}{path}", self.base))
    }
}

/// Convenience: boot the app from the test's crate root (`CARGO_MANIFEST_DIR`).
pub async fn start_from_manifest() -> Result<TestApplication, BootError> {
    let dir: &Path = Path::new(env!("CARGO_MANIFEST_DIR"));
    TestApp::builder().base_dir(dir).start().await
}
