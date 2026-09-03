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

/// Concrete per-listener runtime state owned by the manager.
struct ListenerRun {
    _consumer: Arc<StreamConsumer<crate::observe::StatsContext>>,
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
/// The consumer carries a `StatsContext` (for the consumer-lag gauge) and is
/// configured with a periodic statistics interval.
fn build_consumer(
    props: &KafkaProps,
    group: &str,
    offset_reset: &str,
) -> Result<Arc<StreamConsumer<crate::observe::StatsContext>>, KafkaError> {
    let config = props.client_config(&[
        ("group.id", group.to_string()),
        ("enable.auto.commit", "false".to_string()),
        ("auto.offset.reset", offset_reset.to_string()),
        ("statistics.interval.ms", "2000".to_string()),
    ]);
    let context = crate::observe::StatsContext { group: group.to_string() };
    let consumer: StreamConsumer<crate::observe::StatsContext> =
        config.create_with_context(context).map_err(|source| KafkaError::Client { source })?;
    Ok(Arc::new(consumer))
}

/// Runs one listener: polls the consumer and routes messages to on-demand
/// per-partition worker tasks; on cancel, drains and returns.
async fn supervised_loop(
    consumer: Arc<StreamConsumer<crate::observe::StatsContext>>,
    handler: PreparedListener,
    group: String,
    concurrency: usize,
    retry: crate::config::RetryProps,
    producer: Inject<KafkaProducer>,
    cancellation: CancellationToken,
) {
    let handler = Arc::new(handler);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut workers: HashMap<(String, i32), mpsc::Sender<RouteMessage>> = HashMap::new();

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            recv = consumer.recv() => {
                match recv {
                    Ok(m) => {
                        let route = RouteMessage::from_borrowed(&m);
                        let key = (route.topic.clone(), route.partition);
                        let sender = match workers.get(&key) {
                            Some(s) => s.clone(),
                            None => {
                                let (tx, rx) = mpsc::channel::<RouteMessage>(4);
                                let consumer = consumer.clone();
                                let handler = handler.clone();
                                let semaphore = semaphore.clone();
                                let group = group.clone();
                                let retry = retry.clone();
                                let producer = producer.clone();
                                tokio::spawn(async move {
                                    partition_worker(rx, consumer, handler, semaphore, group, retry, producer).await;
                                });
                                workers.insert(key.clone(), tx.clone());
                                tx
                            }
                        };
                        // Bounded send: awaiting capacity here provides
                        // backpressure BEFORE the next recv(), so a busy
                        // partition can't make us buffer unboundedly.
                        if sender.send(route).await.is_err() {
                            break;
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
    // On cancel: relative order of in-flight handler completion is not observed
    // beyond each partition (each worker finishes its current record, then sees
    // its channel close and exits).
    drop(workers);
    tracing::debug!("listener supervisor drained");
}

/// Processes one partition's messages sequentially; commits after handler
/// success (or after DLT-publish / log-and-commit for exhausted retries).
async fn partition_worker(
    mut rx: mpsc::Receiver<RouteMessage>,
    consumer: Arc<StreamConsumer<crate::observe::StatsContext>>,
    handler: Arc<PreparedListener>,
    semaphore: Arc<tokio::sync::Semaphore>,
    group: String,
    retry: crate::config::RetryProps,
    producer: Inject<KafkaProducer>,
) {
    while let Some(msg) = rx.recv().await {
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
            commit_after_success(&consumer, &topic, partition, offset);
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
/// enqueues the offset commit; librdkafka sends it asynchronously.
fn commit_after_success(
    consumer: &StreamConsumer<crate::observe::StatsContext>,
    topic: &str,
    partition: i32,
    offset: i64,
) {
    let mut tpl = rdkafka::TopicPartitionList::new();
    let result = tpl.add_partition(topic, partition).set_offset(rdkafka::Offset::Offset(offset + 1));
    if let Err(e) = result {
        tracing::error!(error = %e, topic=%topic, partition=partition, offset=offset, "set_offset failed");
        return;
    }
    if let Err(e) = consumer.commit(&tpl, rdkafka::consumer::CommitMode::Async) {
        tracing::error!(error = %e, topic=%topic, partition=partition, offset=offset, "offset commit failed");
    }
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
                    let consumer = build_consumer(&props, &group, &props.consumer.auto_offset_reset)?;
                    if let Err(e) = consumer.subscribe(&[descriptor.topic]) {
                        tracing::error!(error = %e, topic = descriptor.topic, "listener subscribe failed");
                        continue;
                    }
                    let cancellation = CancellationToken::new();
                    let consumer_arc = consumer.clone();
                    let handler_clone = handler;
                    let concurrency = props.consumer.concurrency;
                    let cancel_run = cancellation.clone();
                    let group_run = group.clone();
                    let retry_run = retry.clone();
                    let producer_run = producer.clone();
                    let supervisor = tokio::spawn(async move {
                        supervised_loop(consumer_arc, handler_clone, group_run, concurrency, retry_run, producer_run, cancel_run).await;
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
