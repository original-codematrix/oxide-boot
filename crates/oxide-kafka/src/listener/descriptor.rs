//! Listener descriptor and message types: what `#[kafka_listeners]` expands
//! into (collected by `inventory`) and the payload handed to a handler.

use crate::KafkaError;
use oxide_core::Context;
use rdkafka::Message;
use rdkafka::message::{BorrowedMessage, Headers};
use std::future::Future;
use std::pin::Pin;

/// A consumed record passed to a listener handler (after JSON decode).
#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub payload: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl ReceivedMessage {
    /// Builds an owned `ReceivedMessage` from a `rdkafka` borrowed record —
    /// the routing hop from `supervised_loop`'s `consumer.recv()` to a
    /// per-partition worker's channel needs an owned value.
    pub(super) fn from_borrowed(m: &BorrowedMessage<'_>) -> Self {
        let headers = m
            .headers()
            .map(|h| h.iter().map(|h| (h.key.to_string(), h.value.map(|v| v.to_vec()).unwrap_or_default())).collect())
            .unwrap_or_default();
        Self {
            payload: m.payload().unwrap_or_default().to_vec(),
            key: m.key().map(|k| k.to_vec()),
            headers,
            topic: m.topic().to_string(),
            partition: m.partition(),
            offset: m.offset(),
        }
    }
}

/// Optional first extra handler parameter: the message key.
#[derive(Debug, Clone)]
pub struct MessageKey(pub Option<String>);

/// Optional first extra handler parameter: the raw message headers.
#[derive(Debug, Clone)]
pub struct MessageHeaders(pub Vec<(String, Vec<u8>)>);

/// A boxed listener handler closure, resolved once for a component handle.
/// Its error type is [`crate::retry::HandlerError`] rather than a plain
/// `String`: this is how macro-generated code around JSON deserialization
/// signals a poison pill to `run_with_retry` — as a distinct enum variant,
/// never as a magic string the user handler's own error message could
/// (however implausibly) collide with.
pub type PreparedListener = Box<
    dyn Fn(ReceivedMessage) -> Pin<Box<dyn Future<Output = Result<(), crate::retry::HandlerError>> + Send>>
        + Send
        + Sync,
>;

/// Registers zero or more listener handlers, collected by `inventory`.
#[derive(Clone, Copy)]
pub struct ListenerDescriptor {
    pub topic: &'static str,
    pub group_id: Option<&'static str>,
    /// `"Type::method"` — used for logs and diagnostics.
    pub listener_name: &'static str,
    /// Resolves the component handle once and returns a boxed handler.
    pub start: fn(&Context) -> Result<PreparedListener, KafkaError>,
}

inventory::collect!(ListenerDescriptor);
