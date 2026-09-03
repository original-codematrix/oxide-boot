// This import is not used by name, but it is what forces the `showcase`
// library crate to be linked into this binary. Without it the components,
// controllers and Kafka listeners registered by `inventory::submit!` in
// `src/lib.rs` never reach the final executable, and the app boots with zero
// components and an empty router.
use oxide_boot::prelude::*;
#[allow(unused_imports)]
use showcase::*;

#[oxide_boot::main]
async fn main() {
    if let Err(e) = OxideApplication::run().await {
        eprintln!("boot failed: {e}");
        std::process::exit(1);
    }
}
