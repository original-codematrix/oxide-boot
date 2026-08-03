// Separate test binary (see boot_failure.rs for the rationale): `inventory`
// is process-global per binary, so a component whose `pre_destroy` always
// fails would poison every other test in `tests/boot.rs` that boots and
// stops an application in that binary.
use oxide_boot::component;
use oxide_boot::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTING_STOPPED: AtomicUsize = AtomicUsize::new(0);

// Independent components (no `deps`) are constructed in ascending
// `type_name` order and torn down in the reverse of that order (see
// `oxide_core::container::assemble`), so the alphabetically-*last* type name
// destructs *first*. `ZFailingOnStop` sorts after `ACountingOnStop`, so its
// `pre_destroy` fails before `ACountingOnStop`'s runs. If `Application::stop`
// aborted on the first failing hook (the bug this test guards against),
// `ACountingOnStop::record` would never execute and `COUNTING_STOPPED` would
// stay at 0.
#[component(pre_destroy = explode)]
struct ZFailingOnStop {}

impl ZFailingOnStop {
    async fn explode(&self) -> ComponentResult<()> {
        Err("intentional stop failure".into())
    }
}

#[component(pre_destroy = record)]
struct ACountingOnStop {}

impl ACountingOnStop {
    async fn record(&self) -> ComponentResult<()> {
        COUNTING_STOPPED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn stop_runs_all_pre_destroy_hooks_despite_earlier_failure() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), "oxide: {}\n").unwrap();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    let before = COUNTING_STOPPED.load(Ordering::SeqCst);
    let err = app.stop().await.unwrap_err();

    match err {
        BootError::PreDestroy { component, message } => {
            assert_eq!(component, "ZFailingOnStop");
            assert!(message.contains("intentional stop failure"), "was: {message}");
        }
        other => panic!("expected PreDestroy error, got: {other}"),
    }
    assert_eq!(
        COUNTING_STOPPED.load(Ordering::SeqCst) - before,
        1,
        "ACountingOnStop::record must still run after ZFailingOnStop::explode fails"
    );
}
