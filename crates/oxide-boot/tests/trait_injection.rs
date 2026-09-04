//! Two components implementing one trait, resolved together.
//!
//! Before `provides(...)`, the container was keyed by concrete `TypeId`
//! alone: two implementations of a trait were two unrelated entries and
//! there was no way to ask for all of them. These tests pin the behaviour
//! that replaces that -- and the parts that deliberately did *not* change,
//! since the concrete path has to keep working for everything already built
//! on it.

use oxide_boot::OxideApplication;
use oxide_boot::component;
use std::collections::HashMap;
use std::sync::Arc;

/// `Send + Sync` are required, not incidental: the container stores handles
/// across threads, and without them the coercion to `Arc<dyn Notifier>`
/// inside an `Arc<dyn Any + Send + Sync>` does not compile.
pub trait Notifier: Send + Sync {
    fn channel(&self) -> &'static str;
    fn notify(&self, message: &str) -> String;
}

#[component(provides(Notifier))]
pub struct EmailNotifier {
    #[default]
    _marker: bool,
}

impl Notifier for EmailNotifier {
    fn channel(&self) -> &'static str {
        "email"
    }
    fn notify(&self, message: &str) -> String {
        format!("email: {message}")
    }
}

#[component(provides(Notifier))]
pub struct SmsNotifier {
    #[default]
    _marker: bool,
}

impl Notifier for SmsNotifier {
    fn channel(&self) -> &'static str {
        "sms"
    }
    fn notify(&self, message: &str) -> String {
        format!("sms: {message}")
    }
}

/// A second trait on one of the same components: a component may be
/// registered under several traits at once.
pub trait Auditable: Send + Sync {
    fn audit_name(&self) -> &'static str;
}

#[component(provides(Notifier, Auditable))]
pub struct PushNotifier {
    #[default]
    _marker: bool,
}

impl Notifier for PushNotifier {
    fn channel(&self) -> &'static str {
        "push"
    }
    fn notify(&self, message: &str) -> String {
        format!("push: {message}")
    }
}

impl Auditable for PushNotifier {
    fn audit_name(&self) -> &'static str {
        "push-notifier"
    }
}

/// A trait nothing implements: an extension point with no plugins yet.
pub trait Unimplemented: Send + Sync {
    fn never(&self);
}

async fn boot() -> oxide_boot::Application {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: trait-injection-test\n  server:\n    port: 0\n  management:\n    port: 0\n",
    )
    .expect("write application.yml");
    OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot")
}

#[tokio::test(flavor = "multi_thread")]
async fn all_implementations_of_a_trait_are_collected() {
    let app = boot().await;
    let notifiers: Vec<Arc<dyn Notifier>> = app.context().get_all::<dyn Notifier>();

    assert_eq!(notifiers.len(), 3, "expected every #[component(provides(Notifier))] to be collected");

    let mut channels: Vec<&str> = notifiers.iter().map(|n| n.channel()).collect();
    channels.sort_unstable();
    assert_eq!(channels, ["email", "push", "sms"]);

    // The handles are the real components, not fresh instances.
    let out: Vec<String> = notifiers.iter().map(|n| n.notify("hi")).collect();
    assert!(out.contains(&"email: hi".to_string()), "was: {out:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn ordering_is_stable_rather_than_link_order() {
    let app = boot().await;
    // Sorted by component type name, so a chain built from this does not
    // silently reorder itself when an unrelated crate is added to the build.
    let first: Vec<&str> = app.context().get_all::<dyn Notifier>().iter().map(|n| n.channel()).collect();
    let second: Vec<&str> = app.context().get_all::<dyn Notifier>().iter().map(|n| n.channel()).collect();
    assert_eq!(first, second);
    assert_eq!(first, ["email", "push", "sms"], "expected order by component type name");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_component_can_provide_several_traits() {
    let app = boot().await;
    let auditables: Vec<Arc<dyn Auditable>> = app.context().get_all::<dyn Auditable>();
    assert_eq!(auditables.len(), 1);
    assert_eq!(auditables[0].audit_name(), "push-notifier");
    // ...and still appears under the other trait it declares.
    assert_eq!(app.context().count_of::<dyn Notifier>(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_one_refuses_to_choose_between_several() {
    let app = boot().await;
    // Three implementations: picking one silently is how a wrong dependency
    // reaches production unnoticed, so this is None rather than arbitrary.
    assert!(app.context().get_one::<dyn Notifier>().is_none());
    // Exactly one implementation resolves.
    let audit = app.context().get_one::<dyn Auditable>().expect("exactly one Auditable");
    assert_eq!(audit.audit_name(), "push-notifier");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unimplemented_trait_is_empty_not_an_error() {
    let app = boot().await;
    assert!(app.context().get_all::<dyn Unimplemented>().is_empty());
    assert_eq!(app.context().count_of::<dyn Unimplemented>(), 0);
    assert!(app.context().get_one::<dyn Unimplemented>().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_concrete_type_still_resolves() {
    let app = boot().await;
    // The whole point of a binding rather than a replacement: everything
    // already written against the concrete type keeps working.
    let email = app.context().get::<EmailNotifier>().expect("EmailNotifier by concrete type");
    assert_eq!(email.channel(), "email");
}
