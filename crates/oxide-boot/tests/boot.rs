use oxide_boot::component;
use oxide_boot::prelude::*;
use oxide_boot::__private::{ComponentDescriptor, Context};
use oxide_boot::ConfigProperties;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

// `inventory` registration is process-global: every `#[tokio::test]` in this
// binary that calls `.start()` constructs ALL components registered anywhere
// in this file, not just the ones declared for that particular test. Cargo
// runs tests in parallel by default, so two boots can race on these shared
// statics. We therefore (a) serialize the boot/assert/stop lifecycle across
// tests with BOOT_LOCK, and (b) track hook invocations as counters and
// assert on the delta caused by *this* test's own boot, rather than on
// absolute state — the absolute state can already reflect another test's
// boot that ran earlier (in either order; ordering is not guaranteed once
// tests race for BOOT_LOCK).
static STARTED_COUNT: AtomicUsize = AtomicUsize::new(0);
static STOPPED_COUNT: AtomicUsize = AtomicUsize::new(0);
static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Shared config fixture: every component registered anywhere in this file is
/// constructed on every boot in this binary (see the `inventory` note above),
/// so this must contain the union of all config keys needed by ALL registered
/// test components — currently `oxide.application.name` (Probe) and `billing`
/// (BillingProps).
fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: probe-app\nbilling:\n  url: \"http://localhost:8081\"\n",
    )
    .unwrap();
    dir
}

struct Probe;

inventory::submit! {
    ComponentDescriptor::builder(
        "Probe",
        std::any::TypeId::of::<Probe>,
        |b| {
            b.resolve::<AppConfig>()?;
            b.insert(Probe);
            Ok(())
        },
    )
    .deps(|| vec![(std::any::TypeId::of::<AppConfig>(), "AppConfig")])
    .post_construct(|_ctx: &Context| Box::pin(async {
        STARTED_COUNT.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }))
    .pre_destroy(|_ctx: &Context| Box::pin(async {
        STOPPED_COUNT.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }))
    .build()
}

#[tokio::test]
async fn boots_reads_config_runs_hooks_and_stops() {
    let _guard = BOOT_LOCK.lock().await;
    let started_before = STARTED_COUNT.load(Ordering::SeqCst);
    let stopped_before = STOPPED_COUNT.load(Ordering::SeqCst);

    let dir = test_config_dir();

    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
        .start()
        .await
        .unwrap();

    let cfg = app.context().get::<AppConfig>().unwrap();
    assert_eq!(cfg.get::<String>("oxide.application.name").unwrap(), "probe-app");
    assert!(app.context().get::<Probe>().is_some());
    assert_eq!(STARTED_COUNT.load(Ordering::SeqCst) - started_before, 1);
    assert_eq!(STOPPED_COUNT.load(Ordering::SeqCst) - stopped_before, 0);

    app.stop().await.unwrap();
    assert_eq!(STOPPED_COUNT.load(Ordering::SeqCst) - stopped_before, 1);
}

static GREETER_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);

#[component]
struct Clock {
    #[default]
    ticks: std::sync::atomic::AtomicUsize,
}

#[component(post_construct = init)]
struct Greeter {
    clock: Clock,
}

impl Greeter {
    async fn init(&self) -> ComponentResult<()> {
        GREETER_INIT_COUNT.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn greet(&self) -> String {
        self.clock.ticks.fetch_add(1, Ordering::SeqCst);
        "hello".to_string()
    }
}

#[tokio::test]
async fn component_macro_wires_dependencies_and_runs_post_construct() {
    let _guard = BOOT_LOCK.lock().await;
    // Even serialized against the other test, this app's own boot also
    // constructs Greeter, which is process-global via `inventory`. Snapshot
    // the counter before this boot and assert on the delta rather than an
    // absolute value, so this test doesn't depend on whether it runs before
    // or after `boots_reads_config_runs_hooks_and_stops`.
    let before = GREETER_INIT_COUNT.load(Ordering::SeqCst);

    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    let greeter = app.context().get::<Greeter>().unwrap();
    assert_eq!(greeter.greet(), "hello");
    assert_eq!(GREETER_INIT_COUNT.load(Ordering::SeqCst) - before, 1);
    app.stop().await.unwrap();
}

static OPEN_COUNT: AtomicUsize = AtomicUsize::new(0);
static CLOSE_COUNT: AtomicUsize = AtomicUsize::new(0);

#[component(post_construct = open, pre_destroy = close)]
struct Connection {
    #[default]
    _marker: (),
}

impl Connection {
    async fn open(&self) -> ComponentResult<()> {
        OPEN_COUNT.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&self) -> ComponentResult<()> {
        CLOSE_COUNT.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn component_macro_wires_combined_post_construct_and_pre_destroy() {
    let _guard = BOOT_LOCK.lock().await;
    // Same rationale as the other tests: `Connection` is process-global via
    // `inventory`, so every boot in this binary constructs it. Snapshot both
    // counters and assert on the delta caused by this test's own boot/stop.
    let open_before = OPEN_COUNT.load(Ordering::SeqCst);
    let close_before = CLOSE_COUNT.load(Ordering::SeqCst);

    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    assert!(app.context().get::<Connection>().is_some());
    assert_eq!(OPEN_COUNT.load(Ordering::SeqCst) - open_before, 1);
    assert_eq!(CLOSE_COUNT.load(Ordering::SeqCst) - close_before, 0);

    app.stop().await.unwrap();
    assert_eq!(CLOSE_COUNT.load(Ordering::SeqCst) - close_before, 1);
}

#[derive(ConfigProperties, serde::Deserialize)]
#[config(prefix = "billing")]
struct BillingProps {
    url: String,
    #[serde(default)]
    retries: u32,
}

#[component]
struct BillingGateway {
    props: BillingProps,
}

#[tokio::test]
async fn config_properties_are_bound_and_injectable() {
    let _guard = BOOT_LOCK.lock().await;

    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    let gw = app.context().get::<BillingGateway>().unwrap();
    assert_eq!(gw.props.url, "http://localhost:8081");
    assert_eq!(gw.props.retries, 0);
    app.stop().await.unwrap();
}

#[derive(ConfigProperties, serde::Deserialize, Default)]
#[config(prefix = "optional-section", default)]
struct OptionalProps {
    #[serde(default)]
    flag: bool,
}

#[component]
struct OptionalConsumer {
    props: OptionalProps,
}

#[tokio::test]
async fn config_properties_with_default_tolerate_missing_section() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir(); // contains NO `optional-section:` key — that is the point
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let consumer = app.context().get::<OptionalConsumer>().unwrap();
    assert!(!consumer.props.flag);
    app.stop().await.unwrap();
}

static CUSTOM_LOGGING_RAN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

inventory::submit! {
    oxide_boot::LoggingInitializer {
        priority: 100,
        init: |_config| {
            CUSTOM_LOGGING_RAN.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Deliberately no subscriber install: the built-in fallback must NOT run
            // as well, so log output setup stays with whoever wins.
            Ok(())
        },
    }
}

#[tokio::test]
async fn highest_priority_logging_initializer_wins() {
    let _guard = BOOT_LOCK.lock().await;
    let before = CUSTOM_LOGGING_RAN.load(std::sync::atomic::Ordering::SeqCst);
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    assert_eq!(CUSTOM_LOGGING_RAN.load(std::sync::atomic::Ordering::SeqCst), before + 1);
    app.stop().await.unwrap();
}
