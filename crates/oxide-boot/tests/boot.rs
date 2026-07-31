use oxide_boot::prelude::*;
use oxide_boot::__private::{ComponentDescriptor, Context};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

static STARTED: AtomicBool = AtomicBool::new(false);
static STOPPED: AtomicBool = AtomicBool::new(false);

struct Probe;

inventory::submit! {
    ComponentDescriptor {
        type_name: "Probe",
        type_id: || std::any::TypeId::of::<Probe>(),
        deps: || vec![(std::any::TypeId::of::<AppConfig>(), "AppConfig")],
        construct: |b| {
            b.resolve::<AppConfig>()?;
            b.insert(Probe);
            Ok(())
        },
        post_construct: Some(|_ctx: &Context| Box::pin(async {
            STARTED.store(true, Ordering::SeqCst);
            Ok(())
        })),
        pre_destroy: Some(|_ctx: &Context| Box::pin(async {
            STOPPED.store(true, Ordering::SeqCst);
            Ok(())
        })),
    }
}

#[tokio::test]
async fn boots_reads_config_runs_hooks_and_stops() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), "oxide:\n  application:\n    name: probe-app\n").unwrap();

    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
        .start()
        .await
        .unwrap();

    let cfg = app.context().get::<AppConfig>().unwrap();
    assert_eq!(cfg.get::<String>("oxide.application.name").unwrap(), "probe-app");
    assert!(app.context().get::<Probe>().is_some());
    assert!(STARTED.load(Ordering::SeqCst));
    assert!(!STOPPED.load(Ordering::SeqCst));

    app.stop().await.unwrap();
    assert!(STOPPED.load(Ordering::SeqCst));
}
