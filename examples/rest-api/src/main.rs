// This import is not used by name, but it is what forces the `rest_api`
// library crate to be linked into this binary. Without it, `NoteStore`'s and
// `NotesController`'s `inventory::submit!` registrations (emitted by
// `#[component]`/`#[controller]` in `src/lib.rs`) never make it into the
// final executable, so the assembler finds zero components and the router
// mounts zero routes at boot. Verified by removing this import: the boot
// log then reports `components=0` instead of the expected non-zero count.
use oxide_boot::prelude::*;
#[allow(unused_imports)]
use rest_api::*;

#[oxide_boot::main]
async fn main() {
    if let Err(e) = OxideApplication::run().await {
        eprintln!("boot failed: {e}");
        std::process::exit(1);
    }
}
