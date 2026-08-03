# Oxide Boot

A Spring-Boot-like application framework for Rust: components, dependency
injection, `application.yml` with profiles, and an opinionated boot lifecycle —
built on Tokio and battle-tested crates.

**Status: Phase 1 (foundation).** Web, MongoDB, Kafka, HTTP client and
observability starters are planned — see `docs/superpowers/specs/`.

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

## Environment overrides

An env var `OXIDE_A_B` binds in this order: (1) an existing key under
`oxide.a.b` — the framework namespace wins if present; (2) an existing key
under `a.b`, with the `OXIDE_` prefix stripped — this makes app-level config
outside `oxide:` (e.g. `greeter:`) env-overridable, so `OXIDE_GREETER_NAME`
maps to `greeter.name` when `greeter:` exists in the YAML; (3) otherwise a new
key is created under `oxide.a.b`. "Existing" means intermediate levels must
already be present as keys — only the final leaf may be newly created, and
only once at least one existing level was matched.

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
