// This import is not used by name, but it is what forces the `hello_world`
// library crate to be linked into this binary. Without it, `Greeter`'s
// `inventory::submit!` registration (emitted by `#[component]` in
// `src/lib.rs`) never makes it into the final executable, so the assembler
// finds zero components at boot. Verified by removing this import: the boot
// log then reports `components=0` instead of the expected non-zero count.
#[allow(unused_imports)]
use hello_world::*;
use oxide_boot::prelude::*;

#[oxide_boot::main]
async fn main() {
    if let Err(e) = OxideApplication::run().await {
        eprintln!("boot failed: {e}");
        std::process::exit(1);
    }
}
