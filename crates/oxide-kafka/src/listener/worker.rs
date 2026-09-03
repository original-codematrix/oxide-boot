//! The per-partition worker: sequential processing of one `(topic,
//! partition)`'s records, with the retry loop, the DLT hand-off for
//! exhausted/poison-pill failures, and the offset commit that follows.

use super::descriptor::{PreparedListener, ReceivedMessage};
use super::manager::ListenerContext;
use crate::producer::KafkaProducer;
use crate::retry::{backoff_sequence, publish_to_dlt};
use oxide_core::Inject;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
pub(super) async fn partition_worker(
    mut rx: mpsc::Receiver<ReceivedMessage>,
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
        let Some(received) = next else { break };
        // `Semaphore::acquire` only errors once `close()` has been called on
        // it; this semaphore is created fresh per supervisor
        // (`supervised_loop`) and nothing in this crate ever closes it, so
        // the `Err` arm is unreachable in practice.
        let _permit = semaphore.acquire().await.expect("semaphore not closed");
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
            // A poison pill (payload the handler could never deserialize)
            // never succeeds on retry — it's pure latency and log noise to
            // retry it like a transient failure. Skip straight to the
            // terminal outcome (→ DLT) after this single attempt. This is a
            // distinct enum variant, not a string pattern, so a handler's own
            // error can never be misclassified as a poison pill.
            Err(crate::retry::HandlerError::PoisonPill(message)) => {
                outcome = Err(message);
                break;
            }
            Err(crate::retry::HandlerError::Handler(message)) => {
                if attempts > 1 {
                    metrics::counter!("kafka_listener_retries_total", "topic" => topic.to_string(), "group" => group.to_string()).increment(1);
                }
                outcome = Err(message);
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
    consumer: &StreamConsumer<ListenerContext>,
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
    // Issued inline, synchronously (this only enqueues the op —
    // `CommitMode::Async` does not wait for the broker). Deliberately NOT
    // moved onto `spawn_blocking`: independent blocking tasks give no
    // ordering guarantee relative to each other or to
    // `ListenerContext::handle_revoke`'s own final commit, so a
    // late-running fan-out task for an EARLIER offset could commit after
    // (and thus undo) a revoke's final commit for a LATER offset, moving
    // the committed offset backwards and causing the new partition owner
    // to reprocess records — the exact bug this file exists to prevent.
    // `rd_kafka_commit(async=1)` only enqueues the op onto librdkafka's
    // internal queue, it does not contend with a concurrently-dispatching
    // rebalance callback (which does not hold a lock for its duration), so
    // there is no stall here to avoid in the first place.
    if let Err(e) = consumer.commit(&tpl, rdkafka::consumer::CommitMode::Async) {
        tracing::error!(error = %e, topic = %topic, partition, offset, "offset commit failed");
    }
}
