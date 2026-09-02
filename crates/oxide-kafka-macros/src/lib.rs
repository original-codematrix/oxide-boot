//! Procedural macros for the Oxide Boot Kafka starter. Generated code only
//! references paths under `::oxide_kafka::__private`.

mod listeners;

use proc_macro::TokenStream;

/// Turns an impl block into Kafka listener registrations: methods annotated
/// with `#[kafka_listener(topic = "...")]` are mounted as consumers by the
/// KafkaListenerManager on boot.
#[proc_macro_attribute]
pub fn kafka_listeners(args: TokenStream, item: TokenStream) -> TokenStream {
    listeners::expand(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}