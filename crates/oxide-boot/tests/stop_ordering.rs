// Separate test binary (see stop_failure.rs for the rationale): `inventory`
// is process-global per binary, so components registered here must not leak
// into `tests/boot.rs`'s or `tests/stop_failure.rs`'s inventory.
//
// This test proves `destroy_order` (Task 1 of Phase 3a): `pre_destroy` hooks
// run sorted by `(destroy_order asc, then reverse construction order)`,
// implemented in `Application::stop` as
// `self.order.iter().rev()` followed by a *stable* `sort_by_key(|d|
// d.destroy_order)`.
//
// `AlphaStop` and `MidStop` are handwritten descriptors at the default
// `destroy_order` (0) — no `.destroy_order(...)` call, same as every
// existing component. `LateStop` explicitly sets `.destroy_order(100)`, the
// framework's reserved telemetry-flush slot, to prove any component (not
// just `OtelLifecycle`) can opt into running last. `EarlyStop` is registered
// via `#[component(destroy_order = -100)]` — the macro-driven path — to
// prove the macro's `destroy_order` argument produces a descriptor that
// actually stops in the right slot, not just one that compiles.
//
// `AlphaStop`, `MidStop` and `LateStop` are handwritten `ComponentDescriptor`s
// (not `#[component]`): they exist to pin the pre-existing ordering
// contract itself (independent of how a descriptor gets registered), same
// pattern as `oxide-core::container`'s own unit tests and
// `oxide-observability`'s `OtelLifecycle`.
//
// Order reasoning, spelled out:
//   - Independent components (no deps) construct in ascending `type_name`
//     order (`oxide_core::container::assemble`): "AlphaStop" < "EarlyStop" <
//     "LateStop" < "MidStop", so construction order is Alpha, Early, Late,
//     Mid — regardless of the registration order below (Alpha is registered
//     before Mid, but that only matters for tie-breaking within
//     `assemble`'s own `sort_by_key(type_name)`, which is alphabetical
//     anyway).
//   - `Application::stop` takes `self.order.iter().rev()`: reverse of
//     [Alpha, Early, Late, Mid] is [Mid, Late, Early, Alpha].
//   - It then stably sorts that reversed list by `destroy_order`. Keys are
//     [Mid: 0, Late: 100, Early: -100, Alpha: 0]. A stable sort groups equal
//     keys while preserving their relative order from the input: the two
//     order-0 entries, Mid and Alpha, keep their relative order (Mid before
//     Alpha, as in the reversed list); Early (-100) sorts before both; Late
//     (100) sorts after both. Result: [Early, Mid, Alpha, Late].
//
// Hence the expected recorded order is
// ["EarlyStop", "MidStop", "AlphaStop", "LateStop"].
use oxide_boot::prelude::*;
use oxide_core::{ComponentDescriptor, Context};
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Mutex;

static RECORDED: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

struct AlphaStop {}
struct MidStop {}
struct LateStop {}

#[component(destroy_order = -100, pre_destroy = record)]
struct EarlyStop {}

impl EarlyStop {
    async fn record(&self) -> ComponentResult<()> {
        RECORDED.lock().unwrap().push("EarlyStop");
        Ok(())
    }
}

// Hooks record directly in the closure passed to `.pre_destroy` rather than
// via a named `async fn(&Context)`: `HookFn` requires the returned future to
// be `'static` for *any* input lifetime, so the future must not borrow the
// `&Context` argument it's called with (the macro-generated hooks satisfy
// this by cloning an owned `Inject<T>` out of the context before moving it
// into the `async move` block; these hooks don't need to read the context at
// all, so they simply ignore the parameter).
//
// Registered Alpha before Mid, on purpose — the task brief calls this out
// explicitly; registration order must not matter, only `type_name` does
// (see the header comment).
//
// `inventory::submit!` takes the descriptor expression directly (as the
// `#[component]` macro's own codegen does) rather than referencing a
// pre-declared `static`: `ComponentDescriptor` isn't `Copy`, so
// `submit!(SOME_STATIC)` would try to move out of a shared reference.
inventory::submit! {
    ComponentDescriptor::builder(
        "AlphaStop",
        TypeId::of::<AlphaStop>,
        |builder| {
            builder.insert(AlphaStop {});
            Ok(())
        },
    )
    .pre_destroy(|_ctx: &Context| {
        Box::pin(async move {
            RECORDED.lock().unwrap().push("AlphaStop");
            Ok(())
        })
    })
    .build()
}

inventory::submit! {
    ComponentDescriptor::builder(
        "MidStop",
        TypeId::of::<MidStop>,
        |builder| {
            builder.insert(MidStop {});
            Ok(())
        },
    )
    .pre_destroy(|_ctx: &Context| {
        Box::pin(async move {
            RECORDED.lock().unwrap().push("MidStop");
            Ok(())
        })
    })
    .build()
}

inventory::submit! {
    ComponentDescriptor::builder(
        "LateStop",
        TypeId::of::<LateStop>,
        |builder| {
            builder.insert(LateStop {});
            Ok(())
        },
    )
    .pre_destroy(|_ctx: &Context| {
        Box::pin(async move {
            RECORDED.lock().unwrap().push("LateStop");
            Ok(())
        })
    })
    .destroy_order(100)
    .build()
}

#[tokio::test]
async fn pre_destroy_hooks_run_by_destroy_order_then_reverse_construction_order() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), "oxide: {}\n").unwrap();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    app.stop().await.unwrap();

    let recorded = RECORDED.lock().unwrap();
    assert_eq!(*recorded, vec!["EarlyStop", "MidStop", "AlphaStop", "LateStop"]);
}
