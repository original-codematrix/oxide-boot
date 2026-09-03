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
//!   finished is committed synchronously before the rebalance completes.
//!   A newly (re)assigned partition needs no extra handling — the existing
//!   lazy per-partition worker creation starts one as soon as a record for
//!   it arrives.

use crate::KafkaError;
use crate::config::KafkaProps;
use crate::producer::KafkaProducer;
use crate::retry::{backoff_sequence, publish_to_dlt};
use oxide_core::{AppConfig, ComponentDescriptor, Context, Inject};
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Bundled values resolved from the DI context synchronously at post_construct.
type ListenerPrelude = (
    Inject<KafkaListenerManager>,
    Inject<KafkaProducer>,
    KafkaProps,
    Vec<(&'static ListenerDescriptor, PreparedListener, String)>,
);

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

/// Optional first extra handler parameter: the message key.
#[derive(Debug, Clone)]
pub struct MessageKey(pub Option<String>);

/// Optional first extra handler parameter: the raw message headers.
#[derive(Debug, Clone)]
pub struct MessageHeaders(pub Vec<(String, Vec<u8>)>);

/// A boxed listener handler closure, resolved once for a component handle.
pub type PreparedListener =
    Box<dyn Fn(ReceivedMessage) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

/// Shared runtime infrastructure handed to a listener's `start` fn.
pub struct ListenerInfra {}

/// Registers zero or more listener handlers, collected by `inventory`.
#[derive(Clone, Copy)]
pub struct ListenerDescriptor {
    pub topic: &'static str,
    pub group_id: Option<&'static str>,
    /// `"Type::method"` — used for logs and diagnostics.
    pub listener_name: &'static str,
    /// Resolves the component handle once and returns a boxed handler.
    pub start: fn(&Context, &ListenerInfra) -> Result<PreparedListener, KafkaError>,
}

inventory::collect!(ListenerDescriptor);

/// A lightweight owned record routed from a supervisor to a partition worker.
#[derive(Debug)]
struct RouteMessage {
    payload: Vec<u8>,
    key: Option<Vec<u8>>,
    headers: Vec<(String, Vec<u8>)>,
    topic: String,
    partition: i32,
    offset: i64,
}

impl RouteMessage {
    fn from_borrowed(m: &BorrowedMessage<'_>) -> Self {
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

    fn into_received(self) -> ReceivedMessage {
        ReceivedMessage {
            payload: self.payload,
            key: self.key,
            headers: self.headers,
            topic: self.topic,
            partition: self.partition,
            offset: self.offset,
        }
    }
}

/// A live per-partition worker, as tracked for rebalance handling.
struct PartitionHandle {
    /// Routes records to the worker. Dropped (during ordinary shutdown) to
    /// let the worker drain whatever is already buffered before it exits.
    tx: mpsc::Sender<RouteMessage>,
    /// Cancelled on a rebalance revoke to stop the worker from picking up
    /// any further buffered-but-not-yet-started record — unlike ordinary
    /// shutdown, a revoked partition must not have its backlog processed at
    /// all (see `ListenerContext::handle_revoke`).
    revoked: CancellationToken,
    /// Signalled (by dropping) when the worker task ends, so
    /// `handle_revoke` — running synchronously, outside any async context —
    /// can wait for it with a plain, bounded blocking receive.
    done_rx: std::sync::mpsc::Receiver<()>,
}

/// Per-listener registry of live partition workers, shared between
/// `supervised_loop` (which spawns/routes to them) and `ListenerContext`
/// (which stops the affected ones on a rebalance revoke).
type ActiveWorkers = Arc<std::sync::Mutex<HashMap<(String, i32), PartitionHandle>>>;

/// A consumer `ClientContext`/`ConsumerContext`: turns rdkafka's periodic
/// statistics into the `kafka_consumer_lag` gauge (as `StatsContext` used
/// to), and handles consumer-group rebalances by stopping the workers for
/// any revoked partition and committing what they had actually finished —
/// see `handle_revoke` for the mechanism and why it cannot deadlock.
struct ListenerContext {
    group: String,
    listener_name: &'static str,
    active: ActiveWorkers,
    commit_state: Arc<std::sync::Mutex<HashMap<(String, i32), i64>>>,
    /// Bound on how long `handle_revoke` blocks waiting for a revoked
    /// partition's worker to stop. Reuses
    /// `oxide.kafka.consumer.shutdown-timeout-seconds` — the same "how long
    /// are we willing to wait for in-flight work to end" budget Task 12
    /// already introduced for final shutdown.
    shutdown_timeout: Duration,
}

impl rdkafka::client::ClientContext for ListenerContext {
    fn stats(&self, statistics: rdkafka::statistics::Statistics) {
        for (topic, topic_stats) in &statistics.topics {
            for (partition, partition_stats) in &topic_stats.partitions {
                metrics::gauge!(
                    "kafka_consumer_lag",
                    "topic" => topic.clone(),
                    "partition" => partition.to_string(),
                    "group" => self.group.clone(),
                )
                .set(partition_stats.consumer_lag as f64);
            }
        }
    }
}

impl rdkafka::consumer::ConsumerContext for ListenerContext {
    fn pre_rebalance(
        &self,
        base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        // `Assign` needs no handling here: `supervised_loop` spawns a
        // worker for a `(topic, partition)` lazily, the first time a record
        // for it arrives — which already covers a partition gained (or
        // regained) by this rebalance. `Error` is logged by rdkafka itself.
        if let rdkafka::consumer::Rebalance::Revoke(tpl) = rebalance {
            self.handle_revoke(base_consumer, tpl);
        }
    }
}

impl ListenerContext {
    /// Stops every worker for a partition being revoked and issues a final
    /// synchronous commit for what it had actually finished, before this
    /// method returns — the default `rebalance()` driver (which calls
    /// `pre_rebalance`) unassigns the partition immediately afterwards, and
    /// a commit issued after that point would target a partition this
    /// consumer no longer owns.
    ///
    /// # Why the blocking wait below cannot deadlock
    ///
    /// `StreamConsumer` has no dedicated poll thread: its `recv()` future's
    /// `poll` calls straight into `rd_kafka_queue_poll`, which is what
    /// dispatches `pre_rebalance`/`post_rebalance` — synchronously, on
    /// whatever task is `.await`ing `consumer.recv()`. That's
    /// `supervised_loop`'s own tokio task, so this method runs on a tokio
    /// **worker thread**, not a foreign librdkafka thread. `Handle::block_on`
    /// would panic/deadlock here (a runtime cannot be re-entered from inside
    /// its own task poll).
    ///
    /// Instead we block on a plain `std::sync::mpsc::Receiver::recv_timeout`,
    /// bounded by `shutdown_timeout` — but wrapped in
    /// `tokio::task::block_in_place`. That wrapper is load-bearing, not
    /// decorative: the partition worker we're waiting for is a separate
    /// tokio task, but tokio's work-stealing scheduler is free to run a
    /// freshly-woken task on the SAME worker thread that woke it (the
    /// "LIFO slot" fast path) — including this very thread, right before it
    /// reaches this blocking call. Blocking a *plain* OS thread here would
    /// then be a genuine, reproducible deadlock (confirmed empirically while
    /// building this: one of the two workers consistently failed to be
    /// polled at all until the timeout elapsed). `block_in_place` tells the
    /// runtime "this worker thread is about to block" first, which hands
    /// off any task the scheduler had queued onto this thread (including a
    /// LIFO-slotted partition worker) to another worker thread before we
    /// block — so the worker we're waiting on is guaranteed to keep making
    /// progress elsewhere. This requires (and asserts, via a panic) the
    /// multi-thread runtime — true for every `#[oxide_boot::main]` app (see
    /// `oxide-macros::main_macro`) and for this crate's own
    /// `#[tokio::test(flavor = "multi_thread")]` integration tests. If the
    /// worker still hasn't stopped within the budget we log and proceed
    /// anyway, since the rebalance cannot be held up indefinitely.
    fn handle_revoke(&self, base_consumer: &rdkafka::consumer::BaseConsumer<Self>, tpl: &rdkafka::TopicPartitionList) {
        let mut stopped = Vec::new();
        {
            let mut active = self.active.lock().unwrap_or_else(|p| p.into_inner());
            for elem in tpl.elements() {
                let key = (elem.topic().to_string(), elem.partition());
                if let Some(handle) = active.remove(&key) {
                    handle.revoked.cancel();
                    stopped.push((key, handle.done_rx));
                }
            }
        }
        for ((topic, partition), done_rx) in stopped {
            // `Disconnected` (the worker task ended, dropping `done_tx`,
            // without ever sending on it — see `get_or_spawn_worker`) is
            // the EXPECTED, successful outcome, returned as soon as that
            // drop happens rather than only after the full timeout — only
            // `Timeout` is a genuine "still didn't stop in time".
            let timed_out = tokio::task::block_in_place(|| {
                matches!(done_rx.recv_timeout(self.shutdown_timeout), Err(std::sync::mpsc::RecvTimeoutError::Timeout))
            });
            if timed_out {
                tracing::warn!(
                    listener = self.listener_name,
                    topic = %topic,
                    partition,
                    timeout_secs = self.shutdown_timeout.as_secs(),
                    "rebalance revoke: partition worker did not stop within the shutdown-timeout budget; proceeding with the rebalance anyway"
                );
            }
            let Some(next_offset) =
                self.commit_state.lock().unwrap_or_else(|p| p.into_inner()).remove(&(topic.clone(), partition))
            else {
                continue;
            };
            let mut commit_tpl = rdkafka::TopicPartitionList::new();
            if let Err(e) = commit_tpl.add_partition(&topic, partition).set_offset(rdkafka::Offset::Offset(next_offset))
            {
                tracing::error!(error = %e, topic = %topic, partition, "set_offset failed during rebalance revoke commit");
                continue;
            }
            if let Err(e) = base_consumer.commit(&commit_tpl, rdkafka::consumer::CommitMode::Sync) {
                tracing::error!(error = %e, listener = self.listener_name, topic = %topic, partition, "offset commit failed during rebalance revoke");
            } else {
                tracing::debug!(
                    listener = self.listener_name,
                    topic = %topic,
                    partition,
                    offset = next_offset,
                    "committed offset for a revoked partition before the rebalance completed"
                );
            }
        }
    }
}

/// Concrete per-listener runtime state owned by the manager.
struct ListenerRun {
    _consumer: Arc<StreamConsumer<ListenerContext>>,
    supervisor: tokio::task::JoinHandle<()>,
    /// Signals the consumer supervisor to stop polling and drain.
    cancellation: CancellationToken,
    name: &'static str,
}

/// Bounds concurrent handler executions across all partitions of a listener.
/// Constructed automatically from `KafkaProps`; `post_construct` spawns one
/// supervisor per registered listener, `pre_destroy` drains and shuts them
/// down before the producer (and OTEL flush) tear down.
pub struct KafkaListenerManager {
    // Runs are populated during post_construct and drained during
    // pre_destroy, from the (immutable) `Inject<KafkaListenerManager>` handle,
    // so interior mutability is required.
    runs: std::sync::Mutex<Vec<ListenerRun>>,
}

impl KafkaListenerManager {
    fn new() -> Self {
        Self { runs: std::sync::Mutex::new(Vec::new()) }
    }
}

/// Resolves the group id for a listener: the descriptor's explicit `group_id`
/// wins; otherwise fall back to the application name (`oxide.application.name`).
fn resolve_group_id(ctx: &Context, descriptor: &ListenerDescriptor) -> String {
    if let Some(g) = descriptor.group_id {
        return g.to_string();
    }
    ctx.get::<AppConfig>()
        .and_then(|c| c.get_opt::<String>("oxide.application.name").ok().flatten())
        .unwrap_or_else(|| "oxide-app".to_string())
}

/// Builds a consumer for a single listener descriptor and subscribes it.
/// The consumer carries a `ListenerContext` (consumer-lag gauge + rebalance
/// handling) and is configured with a periodic statistics interval.
#[allow(clippy::too_many_arguments)]
fn build_consumer(
    props: &KafkaProps,
    group: &str,
    offset_reset: &str,
    listener_name: &'static str,
    active: ActiveWorkers,
    commit_state: Arc<std::sync::Mutex<HashMap<(String, i32), i64>>>,
    shutdown_timeout: Duration,
) -> Result<Arc<StreamConsumer<ListenerContext>>, KafkaError> {
    let config = props.client_config(&[
        ("group.id", group.to_string()),
        ("enable.auto.commit", "false".to_string()),
        ("auto.offset.reset", offset_reset.to_string()),
        ("statistics.interval.ms", "2000".to_string()),
    ]);
    let context = ListenerContext { group: group.to_string(), listener_name, active, commit_state, shutdown_timeout };
    let consumer: StreamConsumer<ListenerContext> =
        config.create_with_context(context).map_err(|source| KafkaError::Client { source })?;
    Ok(Arc::new(consumer))
}

/// Runs one listener: polls the consumer and routes messages to on-demand
/// per-partition worker tasks; on cancel, stops feeding new records, then
/// awaits every worker (bounded by `shutdown_timeout`) before returning so
/// their offset commits are issued (and flushed) before the consumer drops.
#[allow(clippy::too_many_arguments)]
async fn supervised_loop(
    consumer: Arc<StreamConsumer<ListenerContext>>,
    handler: PreparedListener,
    group: String,
    concurrency: usize,
    retry: crate::config::RetryProps,
    producer: Inject<KafkaProducer>,
    cancellation: CancellationToken,
    listener_name: &'static str,
    shutdown_timeout: Duration,
    active: ActiveWorkers,
    commit_state: Arc<std::sync::Mutex<HashMap<(String, i32), i64>>>,
) {
    let handler = Arc::new(handler);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut workers: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();

    // Looks up the live worker for `key`, spawning one on demand — this is
    // also what lazily (re)starts a worker for a partition (re)assigned by
    // a rebalance, so `ListenerContext::pre_rebalance` needs no `Assign`
    // handling of its own. Registered in `active` (not just a local map) so
    // a rebalance revoke can find and stop it.
    let mut get_or_spawn_worker = |key: (String, i32)| -> mpsc::Sender<RouteMessage> {
        if let Some(tx) = active.lock().unwrap_or_else(|p| p.into_inner()).get(&key).map(|h| h.tx.clone()) {
            return tx;
        }
        let (tx, rx) = mpsc::channel::<RouteMessage>(4);
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let revoked = CancellationToken::new();
        let consumer = consumer.clone();
        let handler = handler.clone();
        let semaphore = semaphore.clone();
        let group = group.clone();
        let retry = retry.clone();
        let producer = producer.clone();
        let commit_state = commit_state.clone();
        let worker_revoked = revoked.clone();
        workers.spawn(async move {
            partition_worker(rx, consumer, handler, semaphore, group, retry, producer, commit_state, worker_revoked)
                .await;
            drop(done_tx);
        });
        active
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key, PartitionHandle { tx: tx.clone(), revoked, done_rx });
        tx
    };

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            recv = consumer.recv() => {
                match recv {
                    Ok(m) => {
                        let route = RouteMessage::from_borrowed(&m);
                        let key = (route.topic.clone(), route.partition);
                        let sender = get_or_spawn_worker(key.clone());
                        // Bounded send: awaiting capacity here provides
                        // backpressure BEFORE the next recv(), so a busy
                        // partition can't make us buffer unboundedly.
                        if let Err(rejected) = sender.send(route).await {
                            // The registered worker exited concurrently — a
                            // rebalance revoke raced with this message (the
                            // revoke removes the registry entry and cancels
                            // the worker, which then closes its channel).
                            // Drop the stale entry and spawn a fresh worker
                            // so this partition keeps making progress
                            // instead of stalling, or (as before this fix)
                            // taking the whole listener down.
                            active.lock().unwrap_or_else(|p| p.into_inner()).remove(&key);
                            let sender = get_or_spawn_worker(key.clone());
                            if sender.send(rejected.0).await.is_err() {
                                tracing::error!(
                                    listener = listener_name,
                                    topic = %key.0,
                                    partition = key.1,
                                    "failed to route a record to a freshly spawned partition worker; dropping it"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        // Transient errors (e.g. the topic being auto-created
                        // slightly later) should not stop the listener. Log and
                        // keep polling; the loop ends on cancellation.
                        tracing::warn!(error = %e, "consumer recv transient error; continuing");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }
    }
    // Release the closure's borrow of `workers` before touching it directly
    // below.
    let _ = get_or_spawn_worker;

    // Stop feeding new records, then close every partition channel: each
    // worker finishes its current record (if any) plus whatever else is
    // already buffered (unlike a rebalance revoke, an ordinary shutdown
    // deliberately drains the backlog rather than discarding it), sees its
    // channel close, and exits. Awaiting the JoinSet (bounded by
    // `shutdown_timeout`) below is what actually waits for that in-flight
    // work instead of abandoning it, which is the fix for Task 12.
    active.lock().unwrap_or_else(|p| p.into_inner()).clear();

    let total_workers = workers.len();
    if tokio::time::timeout(shutdown_timeout, async { while workers.join_next().await.is_some() {} }).await.is_err() {
        let remaining = workers.len();
        tracing::warn!(
            listener = listener_name,
            remaining,
            total = total_workers,
            timeout_secs = shutdown_timeout.as_secs(),
            "kafka shutdown timeout expired; some workers were still running, continuing shutdown"
        );
    }

    // Flush the last offsets the (now-finished) workers committed: enqueued
    // async commits are not guaranteed to have reached the broker yet, and
    // the consumer is about to be dropped.
    let final_offsets = commit_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
    if !final_offsets.is_empty() {
        let mut tpl = rdkafka::TopicPartitionList::new();
        for ((topic, partition), offset) in &final_offsets {
            if let Err(e) = tpl.add_partition(topic, *partition).set_offset(rdkafka::Offset::Offset(*offset)) {
                tracing::error!(error = %e, topic = %topic, partition, "set_offset failed during shutdown flush");
            }
        }
        if let Err(e) = consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync) {
            tracing::error!(error = %e, listener = listener_name, "final offset commit failed during shutdown");
        }
    }

    tracing::debug!(listener = listener_name, "listener supervisor drained");
}

/// Processes one partition's messages sequentially; commits after handler
/// success (or after DLT-publish / log-and-commit for exhausted retries).
///
/// `revoked` is only set by a rebalance revoke (see `ListenerContext`), not
/// by ordinary shutdown (which instead just closes `rx`'s sender and lets
/// this loop drain whatever is already buffered). It is checked with
/// priority over `rx.recv()` so that once revoked, no further
/// buffered-but-not-yet-started record is processed — only a record already
/// being handled (past this `select!`) is allowed to finish.
#[allow(clippy::too_many_arguments)]
async fn partition_worker(
    mut rx: mpsc::Receiver<RouteMessage>,
    consumer: Arc<StreamConsumer<ListenerContext>>,
    handler: Arc<PreparedListener>,
    semaphore: Arc<tokio::sync::Semaphore>,
    group: String,
    retry: crate::config::RetryProps,
    producer: Inject<KafkaProducer>,
    commit_state: Arc<std::sync::Mutex<HashMap<(String, i32), i64>>>,
    revoked: CancellationToken,
) {
    loop {
        let next = tokio::select! {
            biased;
            _ = revoked.cancelled() => break,
            m = rx.recv() => m,
        };
        let Some(msg) = next else { break };
        // `Semaphore::acquire` only errors once `close()` has been called on
        // it; this semaphore is created fresh per supervisor
        // (`supervised_loop`) and nothing in this crate ever closes it, so
        // the `Err` arm is unreachable in practice.
        let _permit = semaphore.acquire().await.expect("semaphore not closed");
        let received = msg.into_received();
        let topic = received.topic.clone();
        let partition = received.partition;
        let offset = received.offset;

        // Consumer span: parented on the W3C `traceparent` the producer
        // injected (end-to-end trace), carrying the routing fields.
        let span = tracing::info_span!(
            "kafka.process",
            topic = %received.topic,
            partition = received.partition,
            offset = received.offset,
            group = %group,
            result = tracing::field::Empty,
        );
        let parent = crate::observe::extract_traceparent(&received.headers);
        span.set_parent(parent);

        let (result, last_error) =
            run_with_retry(handler.as_ref(), &received, &group, &topic, &retry).instrument(span.clone()).await;
        span.record("result", if result.is_ok() { "success" } else { "error" });

        let mut commit = result.is_ok();
        if let Err(e) = &result {
            if retry.dlt_enabled {
                match publish_to_dlt(&producer, &received, e).await {
                    Ok(()) => {
                        metrics::counter!("kafka_listener_dlt_total", "topic" => topic.clone(), "group" => group.clone()).increment(1);
                        // DLT published → commit the original offset (at-least-once
                        // without redelivery storm).
                        commit = true;
                    }
                    Err(dlt_err) => {
                        // DLT itself failed → do NOT commit so the record is
                        // redelivered rather than lost.
                        tracing::error!(error = %dlt_err, topic=%topic, "DLT publish failed; not committing (record will be redelivered)");
                        commit = false;
                    }
                }
            } else {
                // dlt_disabled: log the terminal error and commit.
                tracing::error!(error = %e, topic=%topic, partition=partition, offset=offset, "listener handler failed; committing (dlt disabled)");
                commit = true;
            }
        }
        let _ = (last_error, partition);

        if commit {
            commit_after_success(&consumer, &topic, partition, offset, &commit_state);
        }
    }
}

/// Runs `handler` up to `max_attempts` times with exponential backoff between
/// attempts, blocking the partition (sequential) while doing so. Returns the
/// terminal outcome plus the last error string (empty if success).
async fn run_with_retry(
    handler: &PreparedListener,
    received: &ReceivedMessage,
    group: &str,
    topic: &str,
    retry: &crate::config::RetryProps,
) -> (Result<(), String>, String) {
    let mut attempts: u32 = 0;
    let mut outcome: Result<(), String> = Err("handler never completed".to_string());
    for sleep in std::iter::once(Duration::ZERO).chain(backoff_sequence(retry)) {
        attempts += 1;
        if attempts > retry.max_attempts {
            break;
        }
        if attempts > 1 {
            tokio::time::sleep(sleep).await;
        }
        let started = Instant::now();
        let attempt_result = (handler)(received.clone()).await;
        let attempt_label = if attempt_result.is_ok() { "success" } else { "error" };
        metrics::histogram!("kafka_listener_seconds", "topic" => topic.to_string(), "group" => group.to_string(), "result" => attempt_label.to_string())
            .record(started.elapsed().as_secs_f64());
        match attempt_result {
            Ok(()) => {
                outcome = Ok(());
                break;
            }
            Err(e) => {
                if attempts > 1 {
                    metrics::counter!("kafka_listener_retries_total", "topic" => topic.to_string(), "group" => group.to_string()).increment(1);
                }
                outcome = Err(e.clone());
            }
        }
    }
    let last_error = outcome.clone().err().unwrap_or_default();
    (outcome, last_error)
}

/// Commits the offset AFTER a given message's handler succeeded. `CommitMode::Async`
/// enqueues the offset commit; librdkafka sends it asynchronously. Also
/// records the committed offset in `commit_state` so a final synchronous
/// commit can flush it on shutdown (see `supervised_loop`).
fn commit_after_success(
    consumer: &Arc<StreamConsumer<ListenerContext>>,
    topic: &str,
    partition: i32,
    offset: i64,
    commit_state: &std::sync::Mutex<HashMap<(String, i32), i64>>,
) {
    let next_offset = offset + 1;
    let mut tpl = rdkafka::TopicPartitionList::new();
    let result = tpl.add_partition(topic, partition).set_offset(rdkafka::Offset::Offset(next_offset));
    if let Err(e) = result {
        tracing::error!(error = %e, topic=%topic, partition=partition, offset=offset, "set_offset failed");
        return;
    }
    // Record the offset BEFORE issuing the actual (broker-facing) commit
    // call below: this is what `ListenerContext::handle_revoke` reads once
    // a revoked partition's worker has stopped, so a rebalance revoke never
    // has to wait on the FFI call itself — only on this cheap, in-memory
    // update.
    commit_state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert((topic.to_string(), partition), next_offset);
    // `rd_kafka_commit` (even in `Async` mode, which only enqueues the
    // request rather than waiting for the broker) still briefly needs an
    // internal librdkafka lock that a concurrently-dispatching rebalance
    // callback on the polling thread also holds for its whole duration —
    // so a call here CAN stall for a while whenever a rebalance happens to
    // be in flight. Running it via `spawn_blocking`, off this worker's own
    // task, keeps that possible stall from ever delaying this worker
    // noticing a cancellation and exiting (which is what the bounded wait
    // in `handle_revoke` depends on) — matching the "fire-and-forget" async
    // semantics this already had.
    let consumer = consumer.clone();
    let topic_owned = topic.to_string();
    tokio::task::spawn_blocking(move || {
        if let Err(e) = consumer.commit(&tpl, rdkafka::consumer::CommitMode::Async) {
            tracing::error!(error = %e, topic = %topic_owned, partition, offset, "offset commit failed");
        }
    });
}

inventory::submit! {
    ComponentDescriptor::builder(
        "KafkaListenerManager",
        std::any::TypeId::of::<KafkaListenerManager>,
        |b| {
            b.insert(KafkaListenerManager::new());
            Ok(())
        },
    )
    .deps(|| vec![
        (std::any::TypeId::of::<KafkaProps>(), "KafkaProps"),
        (std::any::TypeId::of::<KafkaProducer>(), "KafkaProducer"),
        (std::any::TypeId::of::<AppConfig>(), "AppConfig"),
    ])
    .post_construct(|ctx: &Context| {
        // Resolve everything we need from the context synchronously (the
        // context is not 'static, so it cannot be captured by the spawned
        // supervisor futures). Errors are surfaced as a boot-time component
        // failure.
        let prelude = (|| -> Result<ListenerPrelude, Box<dyn std::error::Error + Send + Sync>> {
            let me = ctx.get::<KafkaListenerManager>().ok_or_else(|| "KafkaListenerManager missing".to_string())?;
            let producer = ctx.get::<KafkaProducer>().ok_or_else(|| "KafkaProducer missing".to_string())?;
            let props = (*ctx.get::<KafkaProps>().ok_or_else(|| "KafkaProps missing".to_string())?).clone();
            let infra = ListenerInfra {};
            let listeners: Vec<(&'static ListenerDescriptor, PreparedListener, String)> = inventory::iter::<ListenerDescriptor>()
                .map(|descriptor| {
                    let handler = (descriptor.start)(ctx, &infra).map_err(|e| {
                        oxide_core::ContextError::Construct {
                            type_name: "KafkaListenerManager",
                            message: format!("listener '{}' failed to resolve: {e}", descriptor.listener_name),
                        }
                    })?;
                    let group = resolve_group_id(ctx, descriptor);
                    Ok::<_, oxide_core::ContextError>((descriptor, handler, group))
                })
                .collect::<Result<_, _>>()?;
            Ok((me, producer, props, listeners))
        })();

        match prelude {
            Err(e) => Box::pin(async move { Err(e) }),
            Ok((me, producer, props, listeners)) => Box::pin(async move {
                let mut runs = Vec::new();
                let retry = props.consumer.retry.clone();
                for (descriptor, handler, group) in listeners {
                    // A consumer config problem here (e.g. an invalid
                    // librdkafka option) is a boot-time failure, not a
                    // recoverable per-listener condition, so it propagates
                    // out of this `post_construct` hook (via `?` on the
                    // `KafkaError` -> boxed-error conversion) as a
                    // `BootError::PostConstruct` — NOT `ContextError`, since
                    // `construct` has already finished by the time
                    // `post_construct` hooks run (see `oxide_core::assemble`)
                    // — rather than being logged-and-skipped like a subscribe
                    // failure. Note this does NOT fire for a merely
                    // unreachable broker: `create_with_context` connects
                    // lazily, same as `KafkaProducer::new` (see its doc
                    // comment in `producer.rs`).
                    let shutdown_timeout = props.consumer.shutdown_timeout();
                    let active: ActiveWorkers = Arc::new(std::sync::Mutex::new(HashMap::new()));
                    // Tracks the last successfully-processed offset per
                    // (topic, partition), so a final synchronous commit can
                    // be issued once a worker has drained — the per-message
                    // commits during normal operation are async
                    // (fire-and-forget), so without this a shutdown or a
                    // rebalance revoke could not guarantee the very last
                    // commit actually reached the broker.
                    let commit_state: Arc<std::sync::Mutex<HashMap<(String, i32), i64>>> =
                        Arc::new(std::sync::Mutex::new(HashMap::new()));
                    let consumer = build_consumer(
                        &props,
                        &group,
                        &props.consumer.auto_offset_reset,
                        descriptor.listener_name,
                        active.clone(),
                        commit_state.clone(),
                        shutdown_timeout,
                    )?;
                    // `rd_kafka_subscribe` (what `subscribe` wraps) only
                    // validates the topic list locally — it registers
                    // interest with the consumer-group balancer without any
                    // network round-trip, so a genuine `Err` here means a
                    // malformed topic (e.g. an invalid regex when using
                    // pattern subscriptions) or similar local validation
                    // failure, never "broker unreachable" (that's discovered
                    // lazily on poll, same as `build_consumer` above /
                    // `KafkaProducer::new`). So — unlike a transient `recv`
                    // error in `supervised_loop` — this aborts the boot.
                    consumer.subscribe(&[descriptor.topic]).map_err(|source| KafkaError::Client { source })?;
                    let cancellation = CancellationToken::new();
                    let consumer_arc = consumer.clone();
                    let handler_clone = handler;
                    let concurrency = props.consumer.concurrency;
                    let cancel_run = cancellation.clone();
                    let group_run = group.clone();
                    let retry_run = retry.clone();
                    let producer_run = producer.clone();
                    let listener_name = descriptor.listener_name;
                    let active_run = active;
                    let commit_state_run = commit_state;
                    let supervisor = tokio::spawn(async move {
                        supervised_loop(
                            consumer_arc,
                            handler_clone,
                            group_run,
                            concurrency,
                            retry_run,
                            producer_run,
                            cancel_run,
                            listener_name,
                            shutdown_timeout,
                            active_run,
                            commit_state_run,
                        )
                        .await;
                    });
                    tracing::info!(listener = descriptor.listener_name, topic = descriptor.topic, group = %group, "kafka listener started");
                    runs.push(ListenerRun {
                        _consumer: consumer,
                        supervisor,
                        cancellation,
                        name: descriptor.listener_name,
                    });
                }
                // Recover from a poisoned lock rather than panicking a second
                // time on top of whatever earlier panic poisoned it — the
                // Mutex only ever guards a plain `Vec`, so there is no
                // invariant to lose by taking the (possibly stale) inner
                // value.
                *me.runs.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = runs;
                Ok(())
            }),
        }
    })
    // Safe by construction: `assemble` runs every component's `construct`
    // closure before any `pre_destroy` hook runs, so `KafkaListenerManager`
    // is unconditionally already in the context by the time this fires.
    .pre_destroy(|ctx: &Context| {
        let me = ctx.get::<KafkaListenerManager>().expect("component exists during shutdown");
        let runs = std::mem::take(&mut *me.runs.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
        Box::pin(async move {
            for run in runs {
                run.cancellation.cancel();
                // Wait for the supervisor (and, transitively, its in-flight
                // partition workers) to finish draining.
                let _ = run.supervisor.await;
                tracing::info!(listener = run.name, "kafka listener stopped");
            }
            Ok(())
        })
    })
    .build()
}
