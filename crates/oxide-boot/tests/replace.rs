//! Replacing a component before boot — the `@MockBean` equivalent.
//!
//! The documentation promised `replace::<T>(mock)` long before it existed,
//! which is worse than not having it. These tests pin what it actually does,
//! and in particular the part that separates it from "construct, then
//! overwrite": the replaced component is never constructed, so its side
//! effects never happen.

use oxide_boot::component;
use oxide_boot::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Counts lifecycle events, so a test can assert that a replaced component
/// did not merely get overwritten afterwards.
static STARTED: AtomicUsize = AtomicUsize::new(0);
static STOPPED: AtomicUsize = AtomicUsize::new(0);

/// These tests read a process-wide counter across a boot, so they cannot
/// overlap: a sibling test booting the *real* component would be counted
/// here and read as "the replacement ran its lifecycle after all".
static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Stands in for something with real side effects: a client that opens a
/// connection, a listener that subscribes.
#[component(post_construct = connect, pre_destroy = disconnect)]
pub struct Upstream {
    #[default]
    pub label: String,
}

impl Upstream {
    pub fn describe(&self) -> String {
        if self.label.is_empty() { "real".to_string() } else { self.label.clone() }
    }

    async fn connect(&self) -> ComponentResult<()> {
        STARTED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn disconnect(&self) -> ComponentResult<()> {
        STOPPED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Depends on `Upstream`, to prove the replacement is what dependents see.
#[component]
pub struct Consumer {
    upstream: Upstream,
}

impl Consumer {
    pub fn ask(&self) -> String {
        self.upstream.describe()
    }
}

async fn boot(replacement: Option<Upstream>) -> oxide_boot::Application {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: replace-test\n  server:\n    port: 0\n  management:\n    port: 0\n",
    )
    .expect("write application.yml");
    let builder = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new());
    let builder = match replacement {
        Some(instance) => builder.replace(instance),
        None => builder,
    };
    builder.start().await.expect("boot")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replacement_is_what_dependents_receive() {
    let _guard = BOOT_LOCK.lock().await;
    let app = boot(Some(Upstream { label: "fake".into() })).await;

    assert_eq!(app.context().get::<Upstream>().expect("Upstream").describe(), "fake");
    // The dependent was still built by the container, and wired to the
    // replacement rather than to a real instance.
    assert_eq!(app.context().get::<Consumer>().expect("Consumer").ask(), "fake");

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_replaced_component_never_runs_its_lifecycle() {
    let _guard = BOOT_LOCK.lock().await;
    let started_before = STARTED.load(Ordering::SeqCst);
    let stopped_before = STOPPED.load(Ordering::SeqCst);

    let app = boot(Some(Upstream { label: "fake".into() })).await;
    app.stop().await.unwrap();

    // This is the whole point. Constructing the real component and then
    // overwriting it would have opened its connection and subscribed its
    // listener first -- which is exactly what a test replaces it to avoid.
    assert_eq!(STARTED.load(Ordering::SeqCst), started_before, "post_construct ran on a replaced component");
    assert_eq!(STOPPED.load(Ordering::SeqCst), stopped_before, "pre_destroy ran on a replaced component");
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_replacement_the_real_component_is_used() {
    let _guard = BOOT_LOCK.lock().await;
    let started_before = STARTED.load(Ordering::SeqCst);

    let app = boot(None).await;
    assert_eq!(app.context().get::<Upstream>().expect("Upstream").describe(), "real");
    assert_eq!(STARTED.load(Ordering::SeqCst), started_before + 1, "the real component's post_construct did not run");

    app.stop().await.unwrap();
}
