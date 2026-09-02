//! Runnable kafka example. Start Redpanda on :9092 first, then
//! `cargo run -p kafka`. On boot `OrderProducer` publishes an `Order` to the
//! `orders` topic and `OrderListener` consumes it, printing a log per message.
use oxide_boot::prelude::*;

#[allow(unused_imports)]
use kafka as _;

#[oxide_boot::main]
async fn main() {
    if let Err(e) = OxideApplication::run().await {
        eprintln!("boot failed: {e}");
        std::process::exit(1);
    }
}
