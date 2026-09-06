# Oxide Boot

A Spring-Boot-like application framework for Rust: components, dependency
injection, `application.yml` with profiles, and an opinionated boot lifecycle —
built on Tokio and battle-tested crates.

**Status: pre-1.0 and moving.** The web, observability, Kafka, MongoDB, Redis,
cache, scheduler, HTTP client and test starters are implemented, along with
dependency injection by type and name, `@Configuration`-style bean factories,
declarative retry, and a circuit breaker for outbound calls. There is no
security starter yet, and no SQL support. See `docs/design/` for the design
documents and the phase plans behind each of those.

APIs are not stable and nothing is published to crates.io.

## Quick start

```rust
use oxide_boot::prelude::*;
use oxide_boot::{component, ConfigProperties};

#[derive(ConfigProperties, serde::Deserialize)]
#[config(prefix = "greeter")]
struct GreeterProps { name: String }

#[component(post_construct = hello)]
struct Greeter { props: GreeterProps }

impl Greeter {
    async fn hello(&self) -> ComponentResult<()> {
        tracing::info!("Hello, {}!", self.props.name);
        Ok(())
    }
}

#[oxide_boot::main]
async fn main() {
    OxideApplication::run().await.unwrap();
}
```

With `application.yml`:

```yaml
greeter:
  name: World
```

Run the example: `cd examples/hello-world && cargo run`

Add `default` (`#[config(prefix = "greeter", default)]`) to tolerate a
missing section, falling back to `Default`. That variant additionally
requires `#[derive(serde::Serialize)]`: the struct's `Default` value is
serialized and seeded into the config tree before environment-variable
overrides run, so multi-segment kebab-case keys (e.g.
`shutdown-grace-seconds`) exist as leaves even when the section is absent
from YAML — see "Environment overrides" below.

## Development

Toolchain: Rust **1.95+** (`rust-version` in `Cargo.toml`); `rust-toolchain.toml`
pins an explicit stable version (with `rustfmt` and `clippy`) so local builds and
CI use the same compiler. `Cargo.lock` is tracked so builds and security scans
are reproducible.

One-time setup for the security tooling:

```sh
cargo install cargo-audit cargo-deny --locked
```

Container-backed tests (Kafka, MongoDB, the `kafka`/`rest-api` examples) need a
running Docker daemon; they pin host ports `9092` and `27017`.

```sh
make ci       # everything below
make check    # fmt --check, clippy -D warnings, cargo test --workspace
make audit    # cargo audit + cargo deny check (advisories, licenses, sources)
make msrv     # cargo +1.95 check --workspace --all-targets, against the declared MSRV
make loadtest # release build of the rest-api example under k6 (see below)
make floor          # the framework's HTTP ceiling, no backends (read the rest against this)
make benchmark      # all four integrations, phased (see below)
make stack / stack-down
make clean-cache    # drops the two caches that never shrink (~14 GB), keeps compiled deps
make clean    # cargo clean -- `target/` is the only thing here that grows without bound
```

`make msrv` needs the 1.95 toolchain installed once:

```sh
rustup toolchain install 1.95
```

Policy lives in `deny.toml`: RUSTSEC advisories fail the build, licenses must be
on the allowlist (permissive only, no copyleft), and every ignored advisory needs
a written reason next to it.

Debug builds are configured for size: dependencies are compiled without debug
info and workspace crates keep line tables only (`[profile.dev]` in the root
`Cargo.toml`). Backtraces and test panic locations still resolve to file and
line; the default settings produced a 61 GB `target/` on a full workspace test
build.

### Load test

`make loadtest` builds the `rest-api` example in release, starts a Redpanda
broker (the `POST` handler publishes to `notes-events`, so a run without a
broker would only measure the error path), waits for `/actuator/health` the way
a kubelet would, and drives it with [k6]. The scenario covers a plain read, a
write that also produces to Kafka, a path-parameter read, a deliberate miss
asserted to be RFC 7807 `problem+json`, and the health endpoint. Thresholds
(`http_req_failed < 1%`, `p95 < 250 ms`, `p99 < 1 s`) fail the run, so it is
usable as a regression gate and not just a report.

```sh
make loadtest                       # 50 VUs, 30s
VUS=200 DURATION=2m make loadtest   # knobs: VUS, DURATION, APP_PORT, MGMT_PORT, KAFKA_PORT
```

Artefacts land in `target/loadtest/` (`app.log`, `summary.json`), and the run
prints the framework's own `/actuator/prometheus` counters next to k6's
client-side numbers. Requires `docker` and `k6`.

[k6]: https://k6.io

### Benchmark (all four integrations)

`make benchmark` runs the `showcase` example — HTTP, MongoDB, Redis/Dragonfly
and Kafka in one service — against the local stack in `deploy/`, in three
phases, because a single mixed run cannot say which component is the limit:

1. **seed** — fill Mongo with a corpus and Kafka with a backlog *before* the
   app boots. The producer rate printed here is the broker's ceiling on this
   machine.
2. **drain** — start the app and measure how fast the Kafka listener eats the
   backlog with no HTTP load competing for the runtime: consumer throughput,
   isolated.
3. **load** — k6 drives the REST surface while the consumer keeps running. The
   drop from phase 2 is the cost of contention.

```sh
make stack        # docker compose: Redpanda, MongoDB, Dragonfly, Prometheus, Grafana
make benchmark    # knobs: KAFKA_SEED, MONGO_SEED, VUS, DURATION
make stack-down
```

Grafana is provisioned at <http://localhost:3000/d/oxide-showcase> (anonymous,
no login) with the service's own metrics next to k6's client-side numbers, so
offered load and handled load sit on the same axis. Nothing here goes near
Kubernetes: the backing services are local containers and the service itself
runs on the host from a release build.

`make capacity` answers "how much, and at what cost" in one table: HTTP alone,
Kafka alone, and both at once, differing only in which load is applied.
Measured on one replica, service and backing services in one docker network,
100 VUs, a 300k-message backlog left standing, 2,000 msg/s injected throughout:

| scenario | HTTP req/s | p50 / p95 | Kafka msg/s | CPU avg | CPU peak | RSS peak |
|---|---|---|---|---|---|---|
| HTTP only | **15,804** | 4.4 / 12.2 ms | 944¹ | 2.03 cores | 2.28 | **34 MB** |
| Kafka only | — | — | **5,928** | 0.76 cores | 0.99 | 154 MB |
| both | **15,870** | 4.3 / 12.5 ms | 3,648 | 1.93 cores | 2.32 | 178 MB |

¹ Not a backlog being drained — there is none in that scenario. It is the
events the REST writes themselves produce, consumed as fast as they arrive.

The interesting result is the third row: **HTTP throughput is unchanged by the
Kafka work** (15,804 vs 15,870), while the consumer drops from 5,928 to 3,648
msg/s. The contention is one-directional — the listener yields to HTTP, not
the other way round. Sizing follows from that: HTTP capacity per replica is
predictable, and Kafka consumption is what needs headroom or more partitions.

Note the memory: 34 MB serving 15,804 req/s. The 178 MB in the combined row is
almost entirely Kafka fetch buffers for the standing backlog.

The script refuses to run on a busy machine, and that guard is not decorative:
one run reported 980 req/s where the same commit did 17,700 minutes earlier,
because unrelated processes were competing for the same 12 cores. The first
scenario of any session also reads low — discard it, or run twice.

**Start with `make floor`.** It drives the rest-api example's in-memory handler
with no backing service in the path, and answers "is the framework the limit?"
before any other number is worth reading:

| | |
|---|---|
| `make floor` — in-memory handler | **~145,000 req/s**, p50 0.41 ms |
| showcase, one Redis-backed route | ~11,500-16,500 req/s, p50 ~1.2-1.9 ms |
| showcase, Mongo list of 50 docs | ~10,300 req/s, p50 1.79 ms |

A tenfold drop the moment a backing service enters the path, on a laptop where
that service is a container behind Docker Desktop's network. The framework is
not what these benchmarks measure once a backend is involved.

Reference numbers, M-series laptop, 100 VUs, 200k seeded events, 50k documents:

| | |
|---|---|
| HTTP throughput | ~11,500 req/s |
| HTTP p50 / p95 / p99 | 6.4 ms / 16 ms / 27 ms |
| HTTP failure rate | 0% (1.56M checks, none failed) |
| Kafka consume, idle (host) | ~5,000 msg/s |
| Kafka consume, idle (in-network) | ~9,090 msg/s |
| Seed producer rate | ~174,000 msg/s (broker ceiling) |

Those two numbers are measured **separately**, and that matters.

**Where the service runs changes the result.** `IN_NETWORK=1` runs it as a
compose service instead of on the host, so its Redis/Mongo/Kafka traffic stops
crossing Docker Desktop's host-to-VM boundary:

| | on the host | in the compose network |
|---|---|---|
| Kafka consume, idle | ~5,000 msg/s | **~9,090 msg/s** |
| HTTP, no backlog | ~11,500 req/s | ~10,900 req/s |

The consumer nearly doubles; HTTP does not, because k6 still reaches it across
that same boundary. The listener does one Redis round trip per message, and
~5,000 msg/s is close to the ~6,500 sequential ops/s one connection sustains
through the port mapping — that round trip, not the framework, is the cap.
`scripts/kafka-sweep.sh` confirms it: sweeping `oxide.kafka.consumer.concurrency`
from 4 to 256 against a fixed backlog moves nothing (4 and 64 both land at
~5,000 msg/s once warm). Note that the *first* iteration of any sweep reads
low; discard it.

**What actually made it faster.** On the cache-hit route, measured the same way
each time:

| change | req/s | p50 |
|---|---|---|
| starting point | 11,542 | 1.85 ms |
| counter moved off the request path | 17,247 (+49%) | 1.14 ms |
| `K6_IN_NETWORK=1` — load generator inside the network | **27,424** (+59%) | 0.67 ms |

2.4x in total, and neither step touched the handler's actual work. The counter
was a second Redis round trip on the cache *hit* — the fast path the cache
exists for. The second step removed the host-to-container hop for HTTP, the
same boundary that had been capping the Kafka consumer.

What did **not** help: replacing three sequential `get` calls in `/api/stats`
with one `get_many` (MGET) left throughput unchanged, 16.5k vs 15.2k, inside
run-to-run noise. That is informative rather than disappointing — if three
round trips cost the same as one, the round trips are not what the handler is
paying for. Both `get_many` and `pipeline` are in `RedisTemplate` regardless;
they earn their keep on higher-latency links, not on a loopback.

Even at 27k req/s the framework floor is 145k, so the remaining gap is still
the container network, not this code.

The stack publishes the service on **18080/18081**, not the framework defaults,
so `cargo test` and a running stack no longer fight over 8080/8081.

**Resource use** at ~10,900 req/s plus ~9,000 msg/s, from `docker stats`:

| | CPU (of one core) | memory |
|---|---|---|
| the service | 182% avg, 280-305% peak | 79-149 MB |
| MongoDB | 148% avg | 248 MB |
| Redpanda | 51% avg | 530 MB |
| Dragonfly | 40% avg | 50 MB |
| whole stack | ~5.1 of 12 cores | — |

So roughly two cores and under 150 MB for the service itself.

**Saturating both paths at once still costs more than it should.** With
`CONCURRENT=1 IN_NETWORK=1` the HTTP side drops to ~1,900 req/s while the
consumer runs at ~1,500-1,900 msg/s, and request *latency stays healthy*
(p50 5.7 ms, p95 22 ms) — so the requests that run are fast and the load
generator is simply completing fewer iterations. That gap is not explained
yet, and it is not the listener's concurrency. Treat the isolated figures as
the trustworthy ones.

The first run of this benchmark measured 364 req/s at a p95 of 544 ms. The
cause was not the framework: `GET /api/notes` had no result cap, so against a
20k-document corpus every request serialised ~6,700 documents. Capping it took
throughput to ~11,500 req/s — a 32x difference from one missing `limit`, which
is the most useful thing this benchmark has produced so far. Derived queries
still have no `limit`/`skip`/`sort`, so the example reaches for the driver
directly there; see the gap list below.

## Web (oxide-web)

Add `oxide-web` as a dependency and your application gains an HTTP server for
free: `/actuator/health` (and `/actuator/health/liveness`, `/actuator/health/readiness`)
are wired in automatically — served on a separate **management port**
(default `8081`, `oxide.management.port`), not the main application port —
and any `#[component]` you annotate with `#[controller("/base/path")]` gets
its routes mounted on boot. Errors that implement `IntoProblemDetail` (or are
constructed via `ApiError`) render as RFC 7807 `application/problem+json`
responses. (Unlike `oxide-observability` below, `oxide-web` doesn't need a
separate forcing import for this: writing at least one `#[controller(...)]`
impl — which every real use of `oxide-web` does — already references the
crate directly and links it in. A dependency on `oxide-web` with zero
controllers anywhere would not get an HTTP server, for the same linking
reason explained below.)

```rust
#[controller("/api/notes")]
impl NotesController {
    #[get("/{id}")]
    async fn get_by_id(&self, id: Path<u32>) -> Result<Json<Note>, ApiError> {
        self.store.find(*id).map(Json).ok_or_else(ApiError::not_found)
    }
}
```

Run the example: `cd examples/rest-api && cargo run`

Both HTTP servers are configurable:

```yaml
oxide:
  server:
    address: 0.0.0.0                 # main app server bind address
    port: 8080
    request-timeout-seconds: 30      # a slow request gets 503, not a hang
    max-body-bytes: 2097152          # 2 MiB; an oversized body gets 413
    cors:
      allowed-origins: []            # non-empty enables CORS; empty (default) leaves it off
    shutdown:
      readiness-grace-seconds: 0     # opt-in pause between readiness DOWN and drain start
  management:
    address: 0.0.0.0                 # management server bind address
    port: 8081
    request-timeout-seconds: 30
    max-body-bytes: 2097152
    cors:
      allowed-origins: []
    endpoints:
      exposure: "health,info"        # comma-separated, or "*" for everything
    health:
      cache-ttl-millis: 1000         # how long a health check result is reused
      show-details: "never"          # "always" to include indicator detail text
```

`oxide.management.endpoints.exposure` defaults to `"health,info"` —
**`/actuator/prometheus` is opt-in**, not mounted unless you add
`prometheus` to the list (or use `"*"`). This matters if you're upgrading
from an earlier version where every management route was always on: add
`prometheus` to `exposure` or your scrape target starts 404ing.

`oxide.management.health.show-details` only recognizes the exact strings
`"never"` and `"always"`; anything else (including a typo) is treated as
`"never"`, so `DOWN` indicator detail (which can carry driver internals
like Mongo/Kafka host names) stays hidden by default and on misconfiguration.

`oxide.server.shutdown.readiness-grace-seconds` should stay well below
`oxide.server.shutdown-grace-seconds` — the grace period runs *before* the
drain starts, so a grace period close to (or larger than) the drain budget
leaves little or no time to actually drain in-flight connections, and
`stop()` now returns an error when the drain is aborted.

## Observability (oxide-observability)

Add `oxide-observability` as a dependency **and** force the crate to be
linked into your binary with one line:

```rust
// oxide-observability activates itself via `inventory::submit!`
// registrations (logging, metrics, the actuator scrape route) — but Rust
// only links a dependency's code into the final binary if something
// actually references it. Nothing else in a typical app names an
// `oxide_observability` item, so this import is that reference; remove it
// and you silently get oxide-boot's plain-text logging fallback instead of
// JSON/pretty logs, and no `/actuator/prometheus` route at all.
#[allow(unused_imports)]
use oxide_observability as _;
```

With that in place your application gains structured logging and Prometheus
metrics for free — JSON logs when the `prod` profile is active
(pretty-printed otherwise), and a process-global metrics recorder, both
wired up automatically at boot.

Every request through `oxide-web` is measured automatically: a
`http_server_requests_seconds` histogram with `method`, `uri` (the route
TEMPLATE, e.g. `/api/notes/{id}`, never the concrete path), and `status`
labels. Record your own counters, gauges and histograms anywhere with the
`metrics` crate's macros:

```rust
metrics::counter!("notes_created_total").increment(1);
```

Everything — automatic and custom metrics alike — is exposed on
`/actuator/prometheus`, served on the **management port** (default `8081`,
separate from the main application port) alongside `/actuator/health` and
`/actuator/info` — but only once `prometheus` is added to
`oxide.management.endpoints.exposure` (default `"health,info"`; see the Web
section above). Without it the route isn't mounted at all.

Distributed tracing activates the moment `oxide.otel.endpoint` is
configured: spans export over OTLP, W3C `traceparent`/`tracestate` headers
propagate context across service calls, and sampling is parent-based,
controlled via `oxide.otel.sample-ratio`. On shutdown the tracer provider is
flushed with a bounded wait, `oxide.otel.shutdown-timeout-seconds` (default
`10`) — a hung or unreachable collector logs a warning and lets shutdown
continue rather than hanging it.

Run the example: `cd examples/rest-api && cargo run`, then
`curl http://localhost:8081/actuator/prometheus`.

## Kafka (oxide-kafka)

Add `oxide-kafka` as a dependency **and** force the crate to be linked into
your binary with one line:

```rust
// oxide-kafka registers its listener descriptors and the kafka health
// indicator via `inventory::submit!`; like oxide-observability, it only gets
// linked if something references it. This import is that reference.
#[allow(unused_imports)]
use oxide_kafka as _;
```

Kafka is configured under `oxide.kafka`. `bootstrap-servers` is **required** —
if the crate is linked and that key is missing, boot fails fast.

```yaml
oxide:
  kafka:
    bootstrap-servers: "localhost:9092"
    consumer:
      auto-offset-reset: earliest   # default
      concurrency: 4                # default: partitions processed in parallel
      retry:
        max-attempts: 3             # default (total incl. first)
        backoff-initial-ms: 100     # default
        backoff-multiplier: 2.0     # default
        dlt-enabled: true           # default
    producer:
      idempotence: true             # default
    properties:                     # optional librdkafka passthrough
      message.timeout.ms: 10000
```

Publish records with the injectable `KafkaProducer`:

```rust
#[component]
struct OrderService { producer: oxide_kafka::KafkaProducer }

impl OrderService {
    async fn create(&self, id: u32) {
        self.producer.send("orders", &id.to_string(), &json!({"id": id})).await.unwrap();
    }
}
```

Consume with `#[kafka_listeners]` on an impl block (each method becomes one
listener; the first parameter is the JSON payload type, optional `MessageKey`
/ `MessageHeaders` follow):

```rust
#[component]
struct OrderListener {}

#[kafka_listeners]
impl OrderListener {
    #[kafka_listener(topic = "orders", group_id = "orders-group")]
    async fn on_order(&self, order: serde_json::Value, key: oxide_kafka::MessageKey) -> Result<(), String> {
        tracing::info!(key = ?key.0, "order received");
        Ok(())
    }
}
```

Behavior: within a partition messages are processed strictly in order; up to
`consumer.concurrency` handlers run in parallel across partitions. Offsets are
committed **after** the handler succeeds (at-least-once). A failing handler is
retried with exponential backoff (blocking its partition); once `max-attempts`
are exhausted the record is published to `<topic>.DLT` with `x-exception-message`,
`x-original-topic`, `x-original-partition`, `x-original-offset` headers and then
committed (with `dlt-enabled: false` it is logged and committed instead). If the
DLT publish itself fails, the offset is **not** committed (the record is
redelivered rather than lost).

Observability is automatic: `kafka.send` (producer) and `kafka.process`
(consumer) spans connect **into one trace end-to-end** via a W3C `traceparent`
header injected into each record. Metrics:

- `kafka_producer_send_seconds{topic, result}` histogram
- `kafka_listener_seconds{topic, group, result}` histogram
- `kafka_listener_retries_total{topic, group}`, `kafka_listener_dlt_total{topic, group}` counters
- `kafka_consumer_lag{topic, partition, group}` gauge

and `/actuator/health` reports a `kafka` component (UP/DOWN with detail) as long
as `bootstrap-servers` is configured.

Run the end-to-end example: `cd examples/rest-api && cargo run`, then create a
note (`curl -X POST localhost:8080/api/notes -d '{"id":1,"text":"hi"}' -H 'content-type: application/json'`)
and watch the listener log a `note created event received`; `curl
localhost:8081/actuator/health` shows the `kafka` component UP.

There is also a self-contained `examples/kafka` demo (producer + listener +
retry/DLT): start Redpanda on :9092 and run `cargo run -p kafka`, or
`cargo test -p kafka` for a container-backed end-to-end verification.

## MongoDB (oxide-mongo)

Add `oxide-mongo` as a dependency and configure it under `oxide.mongo`
(`bootstrap`-style: `uri` **required**):

```yaml
oxide:
  mongo:
    uri: "mongodb://localhost:27017"
    database: oxide
    ping-on-start: true
```

Mark a struct as a document and define a repository in Spring-Data style. The
`#[mongo_repository]` macro parses the method names into queries and validates
against the document at compile time where possible; everything outside the
grammar uses the `#[query]` escape hatch (raw filter with `?1`/`?2`
placeholders).

```rust
use oxide_mongo::Document;

#[derive(Document, serde::Serialize, serde::Deserialize)]
#[collection = "users"]
struct User {
    #[id]
    #[serde(rename = "_id")]
    id: mongodb::bson::oid::ObjectId,
    email: String,
    status: String,
    age: i32,
}

#[oxide_mongo::mongo_repository(document = User)]
trait UserRepository {
    // CRUD from the codegen: find_by_id, find_all, count, save, delete_by_id
    async fn find_by_id(&self, id: mongodb::bson::oid::ObjectId) -> oxide_mongo::Result<Option<User>>;
    async fn save(&self, user: &User) -> oxide_mongo::Result<()>;

    // Derived queries: find_by_<field>[(_and_ <field>)*][(_greater_than|_lt|_in|...)],
    // count_by_<field>, exists_by_<field>, delete_by_<field>
    async fn find_all_by_status(&self, status: String) -> oxide_mongo::Result<Vec<User>>;
    async fn count_by_status(&self, status: String) -> oxide_mongo::Result<u64>;
    async fn find_by_age_greater_than(&self, age: i32) -> oxide_mongo::Result<Vec<User>>;

    // `_like` matches a "contains" substring; the argument is escaped so it's
    // never interpreted as a regular expression. `_is_null`/`_is_not_null`
    // take NO method argument — they compile to a fixed `$null`/`{$ne: null}`
    // check.
    async fn find_all_by_email_like(&self, fragment: String) -> oxide_mongo::Result<Vec<User>>;
    async fn find_all_by_email_is_null(&self) -> oxide_mongo::Result<Vec<User>>;

    // Escape hatch for anything special.
    #[query(filter = r#"{ "status": ?1, "age": { "$gt": ?2 } }"#)]
    async fn find_special(&self, status: String, min_age: i32) -> oxide_mongo::Result<Vec<User>>;
}
```

The repository implementation is `UserRepositoryImpl` (obtain it from the DI
context). `oxide-observability` metrics/spans fire per Mongo command, and
`/actuator/health` reports a `mongo` component (UP/DOWN) as long as
`oxide.mongo.uri` is configured.

| Spring Data | Oxide Boot |
|---|---|
| `@Document(collection = "users")` | `#[derive(Document)]` + `#[collection = "users"]` |
| `@Id` | `#[id]` on a (renamed `_id`) field |
| `findByStatusAndAgeGreaterThan(...)` | `find_all_by_status_and_age_greater_than(...)` |
| `MongoRepository<T, ID>` | `#[mongo_repository(document = T)] trait …` |
| `@Query("{...}")` with `?0` | `#[query(filter = r#"{... ?1 ...}"#)]` (1-based placeholders) |
| `spring.data.mongodb.uri` | `oxide.mongo.uri` |

## HTTP client (oxide-client)

Declarative, FeignClient-style HTTP clients: annotate a `trait` and the macro
generates a reqwest-backed client registered as an injectable component.

```rust
use oxide_client::{http_client, Json, Path};

#[derive(serde::Serialize, serde::Deserialize)]
struct Invoice { id: u32 }

#[http_client(base_url = "${oxide.clients.billing.url}")]
trait BillingClient {
    #[get("/invoices/{id}")]
    async fn get_invoice(&self, id: Path<u32>) -> Result<Invoice, oxide_client::ClientError>;

    #[post("/invoices")]
    async fn create(&self, body: Json<CreateInput>) -> Result<Invoice, oxide_client::ClientError>;
}
```

The client name is derived from the trait (`billing`), so the `${…}` placeholder
resolves from `oxide.clients.billing.url`. Each call:
- injects the W3C `traceparent` from the current span (traces flow through
  service calls),
- records an `http_client_requests_seconds{client, method, status, uri}`
  histogram,
- maps non-2xx responses to a typed `ClientError::Http { status, body }`
  (RFC 7807 bodies left in `body`),
- percent-encodes every `Path<T>` value before interpolating it into the
  route template, so a value can't inject an extra path segment or a query
  string.

Each client gets its own timeouts, retry policy and circuit breaker, under
`oxide.clients.<name>`:

```yaml
oxide:
  clients:
    billing:
      url: "http://localhost:9000"
      connect-timeout-seconds: 5     # default
      request-timeout-seconds: 30    # default
      retry:
        max-attempts: 1              # default: 1 = off
        backoff-ms: 100              # default
        max-backoff-ms: 5000         # default
        multiplier: 2.0              # default
        jitter: 0.25                 # default
        retry-non-idempotent: false  # default: POST/PATCH are not retried
      circuit-breaker:
        enabled: false               # default
        window-size: 20              # default
        minimum-calls: 10            # default
        failure-rate-threshold: 0.5  # default
        open-duration-seconds: 30    # default
```

Both are off by default: a retry that repeats a lost `POST` duplicates the
write, and a breaker that opens unexpectedly looks to the caller like an
outage that is not happening.

**Breaker outside, retry inside.** With an open circuit nothing is attempted
at all, and the call fails immediately with `ClientError::CircuitOpen`
without opening a connection. An exhausted retry counts as *one* failure for
the breaker rather than three, and a `4xx` counts as none — the upstream
answered correctly that the request was wrong, and a client fetching many
missing resources must not break itself against a healthy dependency.

Run the example end-to-end:
```sh
cargo run -p rest-api            # serves /api/notes on :8080
cargo test -p http-client        # boots an in-process server and calls the client
```

| Spring | Oxide Boot |
|---|---|
| `@FeignClient(name = "billing", url = "${...}")` | `#[http_client(base_url = "${...}")]` on a `trait` |
| `@GetMapping("/x/{id}")` | `#[get("/x/{id}")]` |
| `@PathVariable String id` | `id: Path<String>` |
| `@RequestBody X body` | `body: Json<X>` |
| `FeignException` / `ResponseEntity` | `ClientError::Http { status, body }` |
| `spring.cloud.openfeign...` | `oxide.clients.<name>.url`, `.connect-timeout-seconds`, `.request-timeout-seconds` |
| Resilience4j `CircuitBreaker` | `oxide.clients.<name>.circuit-breaker.*` — off by default |

## Tests (oxide-test)

A `spring-boot-test`-style harness: boot the application under test on
ephemeral ports (no collisions) and drive it through a preconfigured HTTP
client. Two ways to use it:

**Annotation** — the `@SpringBootTest` equivalent. Mark an `async fn` with
`#[oxide_test]`; it bootstraps the app from `CARGO_MANIFEST_DIR` on the `test`
profile and hands the body a booted `&TestApplication`:

```rust
use oxide_test::oxide_test;

#[oxide_test]
async fn notes_flow(app: &oxide_test::TestApplication) {
    app.client().post("/api/notes").json(&Note { id: 1, text: "hi".into() }).send().await.unwrap();
    let list: Vec<Note> = app.client().get("/api/notes").send().await.unwrap().json().await.unwrap();
    assert_eq!(list.len(), 1);
}
```

**Builder** — fine-grained control:

```rust
let app = oxide_test::TestApp::builder().base_dir(env!("CARGO_MANIFEST_DIR")).start().await.unwrap();
app.client().post("/api/notes").json(&Note { id: 1, text: "hi".into() }).send().await.unwrap();
app.stop().await.unwrap();
```

`TestApp::builder().start()` boots with `OXIDE_SERVER_PORT=0` /
`OXIDE_MANAGEMENT_PORT=0` (ephemeral ports) by default; `.test_profile()`
activates `application-test.yml`, and `.env(...)` adds overrides. `app.client()`
targets the main web server, `app.management_client()` the management
(`/actuator`) server. Component replacement (`replace::<T>(mock)`) and packaged
testcontainers fixtures are part of the roadmap.

Run an app's integration tests with a real container (see `examples/http-client`,
`examples/kafka`, `crates/oxide-kafka`, `crates/oxide-mongo` for Redpanda/Mongo-
backed suites).

| Spring Boot Test | Oxide Boot |
|---|---|
| `@SpringBootTest` | `#[oxide_test]` / `oxide_test::TestApp::builder().start()` |
| `@ActiveProfiles("test")` | `.test_profile()` |
| `MockMvc` / `TestRestTemplate` | `TestWebClient` (`app.client()` / `management_client()`) |
| random ports | implied (`OXIDE_SERVER_PORT=0` etc.) |

## Environment overrides

An env var `OXIDE_A_B` binds in this order: (1) an existing key under
`oxide.a.b` — the framework namespace wins if present; (2) an existing key
under `a.b`, with the `OXIDE_` prefix stripped — this makes app-level config
outside `oxide:` (e.g. `greeter:`) env-overridable, so `OXIDE_GREETER_NAME`
maps to `greeter.name` when `greeter:` exists in the YAML; (3) otherwise a new
key is created under `oxide.a.b`. "Existing" means intermediate levels must
already be present as keys — only the final leaf may be newly created, and
only once at least one existing level was matched. Kebab-case leaves (e.g.
`sample-ratio`, `shutdown-grace-seconds`) can therefore only be matched this
way, not newly created out of thin air: `OXIDE_OTEL_SAMPLE_RATIO` maps back
to `sample-ratio` only when that key already exists under `oxide.otel`.

For any `#[config(prefix = "...", default)]` struct (the framework's own
`ServerProps`, `ManagementProps`, `OtelProps`, etc.), that key always
"already exists": its `Default` is seeded into the config tree under its
prefix before env overrides run, so `oxide.otel.sample-ratio` is present
even with no `oxide.otel:` section in YAML at all, and `OXIDE_OTEL_SAMPLE_RATIO`
binds to it correctly. This seeding only fills in *absent* keys — anything
you do write in YAML wins over the seeded default. For properties without
`default` (e.g. `oxide.kafka.bootstrap-servers`, which is required), or for
app-level kebab-case keys you define yourself outside `oxide:`, the original
rule still applies as stated above: the section must already exist in YAML
for a multi-segment env var to bind to a hyphenated leaf inside it.

An env var's raw value is parsed as YAML, not just taken as a string:
`OXIDE_SERVER_PORT=9090` becomes the number `9090`, `OXIDE_FEATURE_ENABLED=true`
becomes a bool, and a bound key isn't limited to a scalar either —
`OXIDE_SERVER_CORS_ALLOWED_ORIGINS='["https://a.example","https://b.example"]'`
sets a list (`oxide.server.cors.allowed-origins` already exists as a seeded
default, so this matches rule 1 above), and flow-style YAML mappings work
the same way for a key that accepts one. A value that doesn't parse as YAML
(most ordinary strings, e.g. a bare hostname) falls back to a plain string,
so no extra quoting is needed for the common case.

## Spring-to-Oxide cheat sheet

| Spring Boot | Oxide Boot |
|---|---|
| `@Component` / `@Service` | `#[component]` |
| Constructor injection | struct fields (auto-wrapped in `Inject<T>`) |
| `@ConfigurationProperties(prefix = "x")` | `#[derive(ConfigProperties)]` + `#[config(prefix = "x")]` |
| `@PostConstruct` / `@PreDestroy` | `#[component(post_construct = m, pre_destroy = m)]` |
| `application.yml` + profiles | identical; profile via `OXIDE_PROFILES_ACTIVE` |
| Env relaxed binding | `OXIDE_SERVER_PORT` -> `oxide.server.port` |
| `SpringApplication.run(...)` | `OxideApplication::run().await` |
| `@SpringBootApplication` main | `#[oxide_boot::main]` |
| `@RestController` | `#[controller("/api/…")]` on an `impl` block |
| `@GetMapping("/x")` | `#[get("/x")]` |
| `@ExceptionHandler` / `ResponseEntity` error | `ApiError` (RFC 7807 `application/problem+json`) |
| Spring Actuator health | `/actuator/health` (+ `/liveness`, `/readiness`) |
| Micrometer `Counter.builder(...)` | `metrics::counter!(...)` |
| `management.server.port` | `oxide.management.port` |
| Sleuth / OTEL auto-instrumentation | automatic via `oxide-observability` |
| `/actuator/prometheus` | same path, served on the management port |
| `@KafkaListener(topic = "x", groupId = "g")` | `#[kafka_listener(topic = "x", group_id = "g")]` (inside `#[kafka_listeners]` impl) |
| `KafkaTemplate.send(topic, key, value)` | `KafkaProducer::send(topic, key, value)` |
| `DeadLetterPublishingRecoverer` | built in — exhausted records go to `<topic>.DLT` |
| `spring.kafka.bootstrap-servers` | `oxide.kafka.bootstrap-servers` |
| Spring-Kafka consumer lag | `kafka_consumer_lag{topic, partition, group}` |
| `@Repository` + Spring Data derived queries | `#[mongo_repository]` + `#[derive(Document)]` |
| `MongoTemplate` | `MongoClient` (injectable, `collection::<T>(name)`) |
| `RedisTemplate` / `StringRedisTemplate` | `RedisTemplate` (strings, hashes, lists, sets, TTL, `pipeline`) |
| `@RedisHash` + `CrudRepository` | `RedisRepository<T>` + `RedisEntity` |
| `@Cacheable` | `#[cacheable(cache = "notes", key = "id")]` |
| `@Scheduled(cron = ...)` | `#[scheduled(cron = "0 0 3 * * *")]` inside `#[scheduled_tasks]` |
| `CacheManager` / `opsForValue` | `CacheManager::get("name")` → raw or typed access |
| `spring.data.redis.url` | `oxide.redis.url` |
| `@FeignClient(url = "${svc.url}")` | `#[http_client(base_url = "${svc.url}")]` |
| Feign `Retryer` | `oxide.clients.<name>.retry.*` — off by default |
| Resilience4j `@CircuitBreaker` | `oxide.clients.<name>.circuit-breaker.*` — off by default |
| `@Retryable` / spring-retry | `#[retryable(max_attempts = 4, retry_if = "...")]` |
| `@Recover` | `#[retryable(recover = "fallback")]` |
| `@SpringBootTest` | `TestApp::builder().start()` |
| `@MockBean` | `TestApp::builder().replace(mock)` |
| `Pageable` / `Sort` | `Page::of(index, size).sorted_by(doc! { ... })` as the last parameter |
| `@DataMongoTest` / `@DataRedisTest` / `@EmbeddedKafka` | `MongoTest::start()` / `RedisTest::start()` / `KafkaTest::start()` |
| Beans of one interface as `List<T>` | `ctx.get_all::<dyn T>()` via `#[component(provides(T))]` |
| `@Configuration` + `@Bean` | `#[configuration]` on an impl, `#[bean]` on factory methods |
| `@Qualifier` / `@Autowired("name")` | `#[bean(name = "billing")]` + `ctx.get_named::<T>("billing")` |
| `@Primary` | `#[bean(primary)]` |
| Micrometer meter pre-registration | `MetricRegistration::counter(...)` |
| `management.metrics.distribution.*` | `oxide.metrics.http.{enabled,histogram,buckets}` |
| `server.ssl.*` | `oxide.server.ssl.{enabled,certificate,private-key}` (PEM) |
| `spring.kafka.properties.*` | `oxide.kafka.properties.*` (SASL, SCRAM, SSL — passthrough) |
| `spring.data.redis.url` with TLS | `rediss://user:pass@host` |

### Starter coverage

Seven of Spring Boot's starters have a counterpart here. Several large ones do
not, and the gap is worth knowing before betting on this:

| Spring Boot starter | Oxide Boot | State |
|---|---|---|
| `spring-boot-starter-web` | `oxide-web` | ✅ |
| `spring-boot-starter-actuator` | `oxide-web` + `oxide-observability` | ✅ |
| `spring-boot-starter-data-mongodb` | `oxide-mongo` | ✅ derived queries with paging and sorting |
| `spring-kafka` | `oxide-kafka` | ✅ listeners, retry, DLT, rebalance-safe |
| `spring-boot-starter-data-redis` | `oxide-redis` | ✅ template + health |
| `spring-boot-starter-test` | `oxide-test` | ✅ boot harness, containers |
| Spring Cloud OpenFeign | `oxide-client` | ✅ |
| `spring-boot-starter-validation` | — | ❌ no `@Valid` / Bean Validation |
| `spring-boot-starter-security` | — | ❌ no authn/authz at all |
| `spring-boot-starter-data-jpa` / JDBC | — | ❌ no SQL support |
| `spring-boot-starter-cache` (`@Cacheable`) | `oxide-cache` | ✅ named caches, in-memory + Redis backends |
| `@Scheduled` / `spring-boot-starter-quartz` | `oxide-scheduler` | ✅ cron, fixed delay, fixed rate — not distributed |
| `spring-boot-starter-amqp`, `mail`, `batch`, `graphql`, `websocket` | — | ❌ |

Still missing from the DI container: `@Import`-style explicit configuration
classes. In practice `inventory` already registers everything that is linked
in, so the case `@Import` solves — pulling in a configuration that would
otherwise not be scanned — mostly does not arise here.

Resolution is no longer type-only. `#[configuration]` marks an impl block
whose `#[bean]` methods are factories — which is how a type the application
does not own, or a second instance of a type, gets into the container.
`#[bean(name = "billing")]` qualifies an instance for
`ctx.get_named::<T>("billing")`, and `#[bean(primary)]` chooses which one an
unqualified lookup gets. An unqualified lookup with several candidates and no
primary returns `None` rather than picking one.

Injection by trait *is* now supported: `#[component(provides(Notifier))]`
registers a component under a trait as well as its concrete type, and
`ctx.get_all::<dyn Notifier>()` returns every implementation — the equivalent
of injecting `List<Notifier>`. `get_one` resolves a sole implementation and
returns `None` rather than choosing when there are several.

### Sizing this for Kubernetes

`deploy/k8s/showcase.yaml` is a reference manifest whose requests and limits
come from measurement, not from guessing. Measured over two hours of load runs
(~20,000 req/s plus a Kafka backlog and 2,000 msg/s of concurrent production),
read from the service's own `/actuator/prometheus`:

| | |
|---|---|
| RSS | 102 MB idle, 172 MB median, **223 MB peak** |
| CPU | **2.13 cores p95**, 2.20 peak |
| Threads | 26 |
| Virtual memory | 1,698 MB — *not* a sizing input, see below |

**Virtual memory is not memory.** It is reserved address space: mapped shared
libraries, an 8 MB stack reservation per thread (26 threads ≈ 208 MB of the
total, essentially untouched), and whatever the allocator asked the kernel for
without writing to. Kubernetes never looks at it — the kubelet evicts and the
OOM killer fires on `container_memory_working_set_bytes`, which is resident
pages plus active page cache. Size against RSS. The one use for the virtual
figure is its *trend*: rising while RSS stays flat means address space is
leaking.

That gives `requests: 256Mi / 2 cpu`, `limits: 512Mi / 3 cpu`. Memory limits
kill rather than throttle, so the headroom is deliberate; CPU limits throttle,
which on a tokio runtime surfaces as latency rather than errors, and skipping
the CPU limit entirely is a reasonable choice if p99 matters more than
noisy-neighbour isolation.

Two things the manifest documents because they are easy to get wrong:
liveness must not depend on a backing service (restarting a pod does not fix
someone else's database), and readiness here means "started and serving", not
"dependencies healthy" — verified by stopping MongoDB, after which
`/actuator/health` returns 503 while `/actuator/health/readiness` stays 200.
Pointing readiness at `/actuator/health` would drain every replica at once on
a shared-dependency blip.

### A Redpanda crash worth knowing about

Recreating the `notes-events` topic while the `showcase` consumer group still
holds committed offsets for the *deleted* topic kills Redpanda v24.2.1: the next
offset commit against the recreated, empty partitions trips an internal
assertion (`Empty batch written to __consumer_offsets`) and the broker exits
133. Reproduced twice. `scripts/benchmark.sh` therefore deletes the consumer
group before the topic; if you write your own reset step, do the same.

## Licence

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT licence ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this work by you, as defined in the Apache-2.0
licence, shall be dual-licensed as above, without any additional terms or
conditions.

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to run the checks, and
[SECURITY.md](SECURITY.md) for how to report a vulnerability.
