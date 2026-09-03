//! Seeds the load-test stack before the service starts.
//!
//! Two independent jobs, both of which change what the benchmark measures:
//!
//! * **Kafka backlog** — produces N events into `notes-events` *before* the
//!   app boots. Because the consumer group starts at `earliest`, the listener
//!   then has a real backlog to drain, so the run measures sustained consumer
//!   throughput rather than a consumer idling behind the HTTP load.
//! * **Mongo corpus** — inserts M documents so reads and `find_all_by_tag`
//!   run against a populated collection instead of an empty one, where Mongo
//!   would answer every query from nothing.
//!
//! Deliberately bypasses the framework and talks to the drivers directly:
//! this is test scaffolding, and going through the DI container would mean
//! booting the very app whose startup we are trying to seed ahead of.
//!
//! Usage: seed --kafka 100000 --mongo 20000 [--brokers ...] [--mongo-uri ...]

use mongodb::bson::doc;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::{Duration, Instant};

#[derive(serde::Serialize, serde::Deserialize)]
struct Note {
    #[serde(rename = "_id")]
    id: String,
    title: String,
    body: String,
    tag: String,
}

fn note(i: usize) -> Note {
    Note {
        id: format!("seed-{i:09}"),
        title: format!("seeded note {i}"),
        // ~200 bytes, matching what the k6 scenario writes, so message and
        // document sizes are comparable between the seeded and live load.
        body: "x".repeat(200),
        tag: ["general", "urgent", "archive"][i % 3].to_string(),
    }
}

struct Args {
    kafka: usize,
    mongo: usize,
    brokers: String,
    mongo_uri: String,
    topic: String,
    database: String,
}

fn parse_args() -> Args {
    let mut a = Args {
        kafka: 0,
        mongo: 0,
        brokers: "localhost:19092".to_string(),
        mongo_uri: "mongodb://localhost:37017".to_string(),
        topic: "notes-events".to_string(),
        database: "showcase".to_string(),
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let value = || argv.get(i + 1).cloned().unwrap_or_else(|| panic!("{} needs a value", argv[i]));
        match argv[i].as_str() {
            "--kafka" => a.kafka = value().parse().expect("--kafka takes a number"),
            "--mongo" => a.mongo = value().parse().expect("--mongo takes a number"),
            "--brokers" => a.brokers = value(),
            "--mongo-uri" => a.mongo_uri = value(),
            "--topic" => a.topic = value(),
            "--database" => a.database = value(),
            other => panic!("unknown argument: {other}"),
        }
        i += 2;
    }
    a
}

async fn seed_kafka(a: &Args) {
    if a.kafka == 0 {
        return;
    }
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &a.brokers)
        // Batch aggressively: this is a bulk load, not a latency-sensitive
        // path, and the default per-message flush would dominate the runtime.
        .set("linger.ms", "50")
        .set("batch.size", "1048576")
        .set("compression.type", "lz4")
        .set("acks", "1")
        // The in-flight window below is bounded by this, not the other way
        // round; raising it is what lets `send_result` keep accepting.
        .set("queue.buffering.max.messages", "200000")
        .create()
        .expect("kafka producer");

    let started = Instant::now();
    // `send_result` enqueues and hands back a delivery future immediately,
    // unlike `send`, which awaits the round trip per message and would
    // serialise the entire load. Futures are awaited in windows so a failed
    // delivery is still noticed, without holding all N of them at once.
    let mut inflight = Vec::with_capacity(WINDOW);
    for i in 0..a.kafka {
        let n = note(i);
        let payload = serde_json::to_string(&n).expect("serialize");

        let queued = loop {
            let record: FutureRecord<String, String> = FutureRecord::to(&a.topic).key(&n.id).payload(&payload);
            match producer.send_result(record) {
                Ok(fut) => break fut,
                // The local queue is full: the broker is the bottleneck, so
                // give it a moment rather than dropping the message.
                Err((_e, _record)) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        // `send_result` hands back an owned future, so `n` and `payload` are
        // free to drop at the end of this iteration.
        inflight.push(queued);

        if inflight.len() == WINDOW {
            await_window(&mut inflight).await;
            eprintln!("[seed] kafka {}/{} ({:.1}s)", i + 1, a.kafka, started.elapsed().as_secs_f64());
        }
    }
    await_window(&mut inflight).await;

    let elapsed = started.elapsed().as_secs_f64();
    eprintln!("[seed] kafka: {} messages in {elapsed:.1}s ({:.0} msg/s)", a.kafka, a.kafka as f64 / elapsed);
}

/// How many deliveries are awaited at once.
const WINDOW: usize = 10_000;

/// Awaits every queued delivery, failing loudly: a benchmark that silently
/// produced fewer messages than it claims would misreport the backlog.
async fn await_window(inflight: &mut Vec<rdkafka::producer::future_producer::DeliveryFuture>) {
    for fut in inflight.drain(..) {
        fut.await.expect("delivery future cancelled").expect("message delivered");
    }
}

async fn seed_mongo(a: &Args) {
    if a.mongo == 0 {
        return;
    }
    let client = mongodb::Client::with_uri_str(&a.mongo_uri).await.expect("mongo connect");
    let collection = client.database(&a.database).collection::<Note>("notes");

    let started = Instant::now();
    // One `insert_many` per 5k documents: a single call with hundreds of
    // thousands would exceed the driver's batch limits, and one call per
    // document would spend the whole run in round trips.
    let mut batch = Vec::with_capacity(5_000);
    for i in 0..a.mongo {
        batch.push(note(i));
        if batch.len() == 5_000 {
            collection.insert_many(std::mem::take(&mut batch)).await.expect("insert_many");
            eprintln!("[seed] mongo {}/{} ({:.1}s)", i + 1, a.mongo, started.elapsed().as_secs_f64());
        }
    }
    if !batch.is_empty() {
        collection.insert_many(batch).await.expect("insert_many");
    }

    // The load test filters by tag; without this index every `find_all_by_tag`
    // is a collection scan and the benchmark measures Mongo's scan speed
    // rather than the framework's.
    collection
        .create_index(mongodb::IndexModel::builder().keys(doc! { "tag": 1 }).build())
        .await
        .expect("create tag index");

    let count = collection.count_documents(doc! {}).await.expect("count");
    eprintln!(
        "[seed] mongo: {} documents inserted in {:.1}s, collection now holds {count}",
        a.mongo,
        started.elapsed().as_secs_f64()
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let a = parse_args();
    eprintln!("[seed] kafka={} mongo={} brokers={} mongo-uri={}", a.kafka, a.mongo, a.brokers, a.mongo_uri);
    tokio::time::timeout(Duration::from_secs(900), async {
        seed_mongo(&a).await;
        seed_kafka(&a).await;
    })
    .await
    .expect("seeding timed out after 15 minutes");
    eprintln!("[seed] done");
}
