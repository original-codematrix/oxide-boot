// Separate test binary (see boot_failure.rs / stop_failure.rs for the
// rationale): `inventory` is process-global per binary, so a component whose
// `post_construct` always fails must not leak into `tests/boot.rs`,
// `tests/boot_failure.rs`, `tests/stop_failure.rs`, or `tests/stop_ordering.rs`.
//
// This test proves Task 2 of Phase 3a: when a *later* component's
// `post_construct` fails during `start()`, the components whose
// `post_construct` already succeeded must be rolled back (their
// `pre_destroy` hooks run) before the `BootError` is returned — otherwise
// they keep running for the rest of the process lifetime (empirically: a
// started management server lingers until process exit).
//
// Order reasoning: independent components (no `deps`) are constructed in
// ascending `type_name` order (`oxide_core::container::assemble`), so
// "AServer" constructs (and its `post_construct` runs) strictly before
// "ZFailing". By the time `ZFailing::explode` fails, `AServer::start`'s
// `post_construct` has already succeeded and incremented `STARTED`. The fix
// must then run `AServer`'s `pre_destroy` as part of rollback, incrementing
// `STOPPED`, before `start()` returns the `PostConstruct` error for
// "ZFailing".
use oxide_boot::component;
use oxide_boot::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

static STARTED: AtomicUsize = AtomicUsize::new(0);
static STOPPED: AtomicUsize = AtomicUsize::new(0);

#[component(post_construct = start, pre_destroy = stop)]
struct AServer {}

impl AServer {
    async fn start(&self) -> ComponentResult<()> {
        STARTED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn stop(&self) -> ComponentResult<()> {
        STOPPED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[component(post_construct = explode)]
struct ZFailing {}

impl ZFailing {
    async fn explode(&self) -> ComponentResult<()> {
        Err("intentional startup failure".into())
    }
}

#[tokio::test]
async fn failing_post_construct_rolls_back_already_started_components() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), "oxide: {}\n").unwrap();

    let err = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap_err();

    match err {
        BootError::PostConstruct { component, message } => {
            assert_eq!(component, "ZFailing");
            assert!(message.contains("intentional startup failure"), "was: {message}");
        }
        other => panic!("expected PostConstruct error, got: {other}"),
    }

    assert_eq!(STARTED.load(Ordering::SeqCst), 1, "AServer::start must have run before ZFailing failed");
    assert_eq!(
        STOPPED.load(Ordering::SeqCst),
        1,
        "AServer::stop must be rolled back after ZFailing's post_construct fails"
    );
}
