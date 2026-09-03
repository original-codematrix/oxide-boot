//! Kafka listener descriptors and the `KafkaListenerManager` that drives
//! per-partition consumers with ordered, at-least-once processing.
//!
//! Guarantees:
//! - One `StreamConsumer` per `ListenerDescriptor` (one consumer group per
//!   listener), so a topic partition is assigned to exactly one process in a
//!   group and messages within it are consumed in offset order.
//! - Within a partition processing is strictly sequential (a dedicated worker
//!   task per `(topic, partition)`, created on demand).
//! - Across partitions up to `consumer.concurrency` handlers run in parallel
//!   (a global semaphore bounds concurrent handler executions).
//! - Offsets are committed **after** handler success (at-least-once) via an
//!   async offset commit. On shutdown, supervisors stop polling, in-flight
//!   handlers finish, and last commits are flushed before teardown.
//! - Consumer-group rebalances are handled via a `ListenerContext`
//!   (`ConsumerContext::pre_rebalance`): a revoked partition's worker is
//!   cancelled (any buffered-but-not-yet-started record is dropped; a
//!   record already in flight is allowed to finish) and whatever it
//!   finished is (re-)committed before the rebalance completes, bounded by
//!   a single deadline across the whole revoke. A newly (re)assigned
//!   partition needs no extra handling — the existing lazy per-partition
//!   worker creation starts one as soon as a record for it arrives.
//!
//! The module is split by responsibility: [`descriptor`] holds the
//! `ListenerDescriptor`/message types that `#[kafka_listeners]` expands
//! into, [`worker`] holds the per-partition worker (retry, DLT hand-off,
//! commit), and [`manager`] holds consumer construction, the supervisor,
//! rebalance handling and the `KafkaListenerManager` DI lifecycle.

mod descriptor;
mod manager;
mod worker;

pub use descriptor::{ListenerDescriptor, MessageHeaders, MessageKey, ReceivedMessage};
