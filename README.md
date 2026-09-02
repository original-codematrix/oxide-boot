# Oxide Boot

A Spring-Boot-like application framework for Rust: components, dependency
injection, `application.yml` with profiles, and an opinionated boot lifecycle —
built on Tokio and battle-tested crates.

**Status: Phase 3 (integrations) complete; Phase 4 (hardening) in progress.**
Web, observability, Kafka, MongoDB, HTTP client and test starters are
implemented — see `docs/superpowers/specs/` for the design and the phase-4
hardening backlog.

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

## Development

Toolchain: Rust **1.88+** (`rust-version` in `Cargo.toml`); `rust-toolchain.toml`
selects the current stable with `rustfmt` and `clippy`. `Cargo.lock` is tracked so
builds and security scans are reproducible.

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
```

Policy lives in `deny.toml`: RUSTSEC advisories fail the build, licenses must be
on the allowlist (permissive only, no copyleft), and every ignored advisory needs
a written reason next to it.

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
`/actuator/info`.

Distributed tracing activates the moment `oxide.otel.endpoint` is
configured: spans export over OTLP, W3C `traceparent`/`tracestate` headers
propagate context across service calls, and sampling is parent-based,
controlled via `oxide.otel.sample-ratio`.

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
  (RFC 7807 bodies left in `body`).

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
| `spring.cloud.openfeign...` | `oxide.clients.<name>.url` (timeout/retry props are on the phase-4c backlog) |

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
`sample-ratio`, `shutdown-grace-seconds`) can only be matched this way, not
newly created: `OXIDE_OTEL_SAMPLE_RATIO` maps back to `sample-ratio` when that
key already exists under `oxide.otel`, but if it doesn't, the env var creates
a nested `oxide.otel.sample.ratio` path instead — silently missing the
kebab-case key it was meant to override.

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
