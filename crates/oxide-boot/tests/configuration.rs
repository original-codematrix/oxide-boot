//! `#[configuration]` / `#[bean]`, and injection by name.
//!
//! `#[component]` can only build a type the application owns, and only one
//! instance of it. Two HTTP clients pointed at different services, a primary
//! and a replica connection, a handle from a crate that knows nothing about
//! this framework — none of those fit, and they are ordinary requirements.
//!
//! What is pinned here is the resolution rules, because getting them wrong
//! is silent: an unqualified lookup with two candidates must not pick one.

use oxide_boot::prelude::*;
use oxide_boot::{component, configuration};
use std::collections::HashMap;

/// Stands in for a type the application does not own — no `#[component]`,
/// no derive, constructed by hand.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceClient {
    pub url: String,
}

impl ServiceClient {
    fn new(url: &str) -> Self {
        Self { url: url.to_string() }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Lonely(pub u32);

pub struct Clients;

#[configuration]
impl Clients {
    /// Two beans of one type: this is the case `#[component]` cannot express
    /// at all.
    #[bean(name = "orders", primary)]
    fn orders() -> ServiceClient {
        ServiceClient::new("https://orders.internal")
    }

    #[bean(name = "billing")]
    fn billing() -> ServiceClient {
        ServiceClient::new("https://billing.internal")
    }

    /// A single unnamed bean, which behaves exactly like a `#[component]`.
    #[bean]
    fn lonely() -> Lonely {
        Lonely(7)
    }
}

/// Proves a bean's parameters are resolved from the container rather than
/// being a plain function call. Deliberately *not* `Clone`, so the
/// `Inject<T>` parameter form is exercised.
#[component]
pub struct Marker {
    #[default]
    pub value: bool,
}

pub struct Derived;

#[configuration]
impl Derived {
    #[bean(name = "derived")]
    fn from_marker(marker: oxide_boot::Inject<Marker>) -> ServiceClient {
        ServiceClient::new(if marker.value { "https://on" } else { "https://off" })
    }
}

async fn boot() -> oxide_boot::Application {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: configuration-test\n  server:\n    port: 0\n  management:\n    port: 0\n",
    )
    .expect("write application.yml");
    OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot")
}

#[tokio::test(flavor = "multi_thread")]
async fn beans_resolve_by_name() {
    let app = boot().await;
    let ctx = app.context();

    let orders = ctx.get_named::<ServiceClient>("orders").expect("orders client");
    let billing = ctx.get_named::<ServiceClient>("billing").expect("billing client");
    assert_eq!(orders.url, "https://orders.internal");
    assert_eq!(billing.url, "https://billing.internal");

    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unqualified_lookup_picks_the_primary() {
    let app = boot().await;
    // Three `ServiceClient` instances exist; without `primary` this would
    // have to be None.
    let client = app.context().get::<ServiceClient>().expect("a primary is declared");
    assert_eq!(client.url, "https://orders.internal");
    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sole_bean_needs_no_name() {
    let app = boot().await;
    // One instance, so the unqualified path is unambiguous -- everything
    // written before qualifiers existed keeps working.
    assert_eq!(app.context().get::<Lonely>().expect("Lonely").0, 7);
    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bean_receives_its_parameters_from_the_container() {
    let app = boot().await;
    let derived = app.context().get_named::<ServiceClient>("derived").expect("derived client");
    // `Marker::default()` is `false`, and it came from the container rather
    // than from a literal in the factory.
    assert_eq!(derived.url, "https://off");
    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_name_is_none_rather_than_a_fallback() {
    let app = boot().await;
    // Falling back to the primary here would turn a typo in a qualifier into
    // a service silently talking to the wrong endpoint.
    assert!(app.context().get_named::<ServiceClient>("nope").is_none());
    app.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn every_instance_is_enumerable() {
    let app = boot().await;
    let mut all: Vec<String> = app
        .context()
        .get_all_named::<ServiceClient>()
        .into_iter()
        .map(|(name, client)| format!("{}={}", name.unwrap_or("<unnamed>"), client.url))
        .collect();
    all.sort();
    assert_eq!(
        all,
        vec![
            "billing=https://billing.internal".to_string(),
            "derived=https://off".to_string(),
            "orders=https://orders.internal".to_string(),
        ]
    );
    app.stop().await.unwrap();
}
