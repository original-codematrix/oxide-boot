# Oxide Boot — Design-Dokument

**Datum:** 2026-07-31
**Status:** Entwurf zur Review
**Zielgruppe:** Java/Kotlin-Team mit Spring-Boot-Hintergrund, neu in Rust

## 1. Ziel und Motivation

Oxide Boot ist ein opinionated Rust-Framework, das die Entwicklererfahrung von Spring Boot
nachbildet: Annotations (als Proc-Macros), Dependency Injection, Auto-Configuration über
Starter-Crates und `application.yml` mit Profilen. Es baut auf etablierten, produktionserprobten
Rust-Crates auf (Tokio, Axum, offizieller MongoDB-Driver, rdkafka, tracing/OpenTelemetry) und
implementiert sie nicht neu — Oxide Boot ist die Konventions- und Glue-Schicht darüber.

**Anforderungen aus dem Brainstorming:**

- Workloads: REST/HTTP-APIs, Kafka-Consumer/-Producer, gemischte Services, Scheduler-Jobs
- Spring-Gefühl: Annotations-Stil, DI, Auto-Configuration, `application.yml` + Profiles —
  die API soll sich für Spring-Entwickler sofort vertraut anfühlen
- Durchsatz: 1.000–10.000 req/s bzw. msg/s pro Service, vollständig async und multithreaded
- Deployment: Kubernetes (Health/Readiness-Probes, Graceful Shutdown, Prometheus-Metriken)
- Observability: Metriken **automatisch** (Micrometer-Semantik), Tracing über OTEL/OTLP,
  strukturierte Logs mit Trace-Korrelation
- Deklarativer HTTP-Client (FeignClient/WebClient-Äquivalent)
- Pilot: ein bestehender Spring-Boot-Service wird als Erstes portiert

**Getroffene Entscheidungen:**

| Frage | Entscheidung |
|---|---|
| Architekturansatz | Opinionated Framework über etablierten Crates (kein Spring-Klon von Grund auf) |
| Name | Oxide Boot, Crate-Prefix `oxide-` |
| API-Fehlerformat | RFC 7807 Problem Detail (wie Spring Boot 3) |
| Kafka-Fehlerbehandlung | Retry + Dead-Letter-Topic ab Version 1 |
| Mongo-Repositories | Derived Queries aus Methodennamen (Spring-Data-Stil), mit definierter Grammatik |
| Ablageort | Cargo-Workspace `~/projects/private/oxide-boot` |

**Nicht-Ziele (V1):**

- Kein Runtime-IoC-Container mit Bean-Lifecycle/Conditional Beans (Ansatz B wurde verworfen)
- Keine weiteren Datenbanken (SQL, Redis) — Architektur lässt spätere Starter zu
- Kein Templating/Server-Side-Rendering; reine JSON-APIs
- Keine Veröffentlichung auf crates.io in V1 (interner Workspace/Registry)

## 2. Reactor/WebFlux-Äquivalenz

Kein Nachbau nötig. Tokio ist eine multithreaded async-Runtime; das Programmiermodell
entspricht WebFlux ohne Reactor-Operatoren:

| Spring/Reactor | Rust/Oxide Boot |
|---|---|
| `Mono<T>` | `async fn … -> T` (Future) |
| `Flux<T>` | `impl Stream<Item = T>` |
| Scheduler/EventLoop | Tokio Multi-Thread-Runtime (Work-Stealing) |
| `block()`-Verbot | Compile-Zeit: kein `.await` in Sync-Kontext möglich |

Alle Framework-APIs sind async-first. Blockierende Aufrufe (falls unvermeidbar) laufen über
`tokio::task::spawn_blocking`, dokumentiert im Kochbuch.

## 3. Workspace-Layout

Cargo-Workspace mit diesen Crates:

| Crate | Spring-Äquivalent | Inhalt |
|---|---|---|
| `oxide-boot` | `spring-boot-starter` | Umbrella-Crate, Prelude, `#[oxide_boot::main]`, `OxideApplication::run()`, App-Lifecycle |
| `oxide-core` | Spring Context | DI-Container, Component-Registry, Config-Loading, Lifecycle-Hooks, Shutdown-Koordination |
| `oxide-macros` | Annotations | Alle Proc-Macros (eigenes Crate, da Proc-Macros ein eigenes Crate erfordern) |
| `oxide-web` | Web/WebFlux-Starter | HTTP-Server auf Axum, Routing, Extractors, RFC-7807-Error-Handling, Actuator-Endpoints |
| `oxide-client` | WebClient/FeignClient | Deklarativer HTTP-Client auf reqwest: Pooling, Timeouts, Retry, Trace-Propagation, Metriken |
| `oxide-mongo` | Spring Data MongoDB | `#[derive(Document)]`, Repositories mit CRUD + Derived Queries, Index-Deklaration |
| `oxide-kafka` | Spring Kafka | `#[kafka_listener]`, Producer-Component, Serde-JSON, Retry + DLT, Offset-Management |
| `oxide-observability` | Micrometer + OTEL | Tracing-Init (OTLP), automatische Metriken, Prometheus-Exporter, Log-Setup |
| `oxide-test` | spring-boot-test | Test-Harness (App im Test booten, Components mocken), Testcontainers-Integration |

**Auto-Configuration-Prinzip:** Crate als Dependency + zugehöriger Abschnitt in
`application.yml` = Feature ist aktiv und verdrahtet. Integration-Crates registrieren sich
beim Boot über einen Registrierungsmechanismus (`inventory`-Crate oder explizite
Feature-Flags auf `oxide-boot`), sodass `OxideApplication::run()` alles Vorhandene startet.
Fehlt die Config für ein eingebundenes Crate, bricht der Start mit klarer Fehlermeldung ab
(Fail-Fast wie Spring).

## 4. Dependency Injection (`oxide-core` + `oxide-macros`)

- `#[derive(Component)]` auf Structs; benannte Varianten `#[service]`, `#[repository]` sind
  Aliase mit gleicher Semantik (Lesbarkeit für Spring-Entwickler).
- **Constructor-Injection:** Felder eines Components sind selbst Components (oder Config-Structs,
  Producer, Clients …). Der Container baut den Abhängigkeitsgraphen beim Start per topologischer
  Sortierung auf.
- Alle Components sind **Singletons**, intern `Arc`-geteilt, `Send + Sync` erzwungen —
  damit thread-safe über alle Tokio-Worker.
- Fehlerbilder: fehlende Dependency und Zyklen werden beim Start (teils zur Compile-Zeit)
  mit klarer Meldung inkl. Abhängigkeitspfad gemeldet — kein Laufzeit-`NoSuchBeanDefinitionException`-Raten.
- Trait-Injection (Interface-Programmierung) über `Arc<dyn Trait>` mit `#[component(binds = MyTrait)]` —
  ermöglicht Mocking in Tests.
- Lifecycle-Hooks: `#[post_construct]` (async möglich) und geordneter Shutdown in umgekehrter
  Abhängigkeitsreihenfolge.

## 5. Konfiguration

Modell 1:1 wie Spring:

```yaml
oxide:
  application: { name: user-service }
  server: { port: 8080 }
  mongo: { uri: "mongodb://localhost:27017", database: users }
  kafka:
    bootstrap-servers: "kafka:9092"
    consumer: { auto-offset-reset: earliest, concurrency: 4 }
  otel: { endpoint: "http://otel-collector:4317" }
```

- Ladereihenfolge: `application.yml` → `application-{profile}.yml` → Umgebungsvariablen
  (`OXIDE_SERVER_PORT=9090` überschreibt `oxide.server.port`). Profil via `OXIDE_PROFILES_ACTIVE`.
- `#[derive(ConfigProperties)]` + `#[config(prefix = "oxide.clients.billing")]` = typisierte
  Config-Structs mit Defaults und Validierung beim Start (`@ConfigurationProperties`-Äquivalent).
- Platzhalter `${…}` in Config-Werten und Macro-Attributen (z. B. `base_url = "${oxide.clients.billing.url}"`).
- Secrets kommen wie in K8s üblich als Env-Vars/mounted files — kein eigener Secret-Mechanismus.

## 6. Web-Layer (`oxide-web`)

Auf Axum/Hyper/Tower (produktionserprobt, erfüllt die Durchsatzanforderung deutlich).

- `#[controller("/api/users")]` auf einem `impl`-Block; Methoden mit `#[get("/{id}")]`,
  `#[post("")]`, etc. Extractors: `Path<T>`, `Query<T>`, `Json<T>`, `Header<T>` —
  Namensgebung an Spring angelehnt.
- **Fehlerformat: RFC 7807 Problem Detail** (Entscheidung). Framework-`ApiError`-Typ mit
  Konstruktoren (`not_found()`, `bad_request(detail)`, …); jeder Handler-Fehler wird als
  `application/problem+json` serialisiert. Eigene Fehler-Enums implementieren `IntoProblemDetail`.
  Validierungsfehler (Request-Body-Validierung über `validator`-Attribute) erzeugen automatisch
  ein Problem Detail mit Feldfehlern — wie Spring `@Valid`.
- Middleware-Kette (Tower-Layers, vom Framework vorkonfiguriert): Request-Tracing,
  Metriken, Timeout, optional CORS/Compression per Config.
- Graceful Shutdown: SIGTERM → Readiness auf `DOWN`, laufende Requests drainen
  (konfigurierbarer Timeout), dann Listener/Runtime geordnet beenden.

### Actuator

- `/actuator/health` — aggregiert; `/actuator/health/liveness` und `/readiness` für K8s-Probes.
  Integrations-Crates registrieren automatisch Health-Indicators (Mongo-Ping, Kafka-Broker-Erreichbarkeit).
- `/actuator/prometheus` — Prometheus-Format (siehe Observability).
- `/actuator/info` — Name, Version, Git-Info (Build-Zeit eingebettet).
- Actuator läuft standardmäßig auf einem separaten Management-Port (Config: `oxide.management.port`),
  damit er im Cluster nicht öffentlich exponiert wird.

## 7. HTTP-Client (`oxide-client`)

Deklarativ wie FeignClient:

```rust
#[http_client(base_url = "${oxide.clients.billing.url}")]
trait BillingClient {
    #[get("/invoices/{id}")]
    async fn get_invoice(&self, id: Path<String>) -> Result<Invoice, ClientError>;

    #[post("/invoices")]
    async fn create(&self, body: Json<CreateInvoice>) -> Result<Invoice, ClientError>;
}
```

- Macro generiert die Implementierung auf `reqwest` (Connection-Pooling, HTTP/2) und registriert
  sie als injizierbare Component.
- Per Config pro Client: `connect-timeout`, `request-timeout`, `retry` (Anzahl, Exponential
  Backoff, nur idempotente Methoden per Default), Basis-Header.
- Automatisch: W3C-`traceparent`-Propagation, Client-Metriken, Fehler als typisiertes
  `ClientError` (Status, Body, Problem-Detail-Parsing wenn vorhanden).

## 8. MongoDB (`oxide-mongo`)

Auf dem offiziellen async MongoDB-Rust-Driver.

- `#[derive(Document)]` auf Structs: Collection-Name (Default: Struct-Name in snake_case,
  überschreibbar), `#[id]`-Feld (ObjectId oder eigener Typ), optionale Index-Deklarationen
  (`#[index(fields = "email", unique)]`) — Indizes werden beim Start angelegt (abschaltbar).
- Repository-Definition als Trait mit Macro:

```rust
#[mongo_repository(document = User)]
trait UserRepository {
    // CRUD kommt aus dem Basis-Trait: find_by_id, find_all, save, insert, delete_by_id,
    // count, exists_by_id, Pagination über Pageable/Page<T>

    // Derived Queries (Entscheidung: Spring-Data-Stil):
    async fn find_by_email(&self, email: &str) -> Result<Option<User>, MongoError>;
    async fn find_by_status_and_created_at_greater_than(&self, status: Status, after: DateTime) -> Result<Vec<User>, MongoError>;
    async fn count_by_status(&self, status: Status) -> Result<u64, MongoError>;
    async fn delete_by_email(&self, email: &str) -> Result<u64, MongoError>;

    // Escape-Hatch für alles Spezielle:
    #[query(filter = r#"{ "status": ?1, "tags": { "$in": ?2 } }"#)]
    async fn find_special(&self, status: Status, tags: Vec<String>) -> Result<Vec<User>, MongoError>;
}
```

- **Derived-Query-Grammatik (bewusst begrenzt, damit Compile-Fehler verständlich bleiben):**
  - Verben: `find_by`, `find_all_by`, `count_by`, `exists_by`, `delete_by`
  - Bedingungen: Feldname, verkettet mit `_and_` / `_or_` (keine Mischung in einer Methode)
  - Operatoren-Suffixe: `_greater_than`, `_less_than`, `_greater_than_equal`, `_less_than_equal`,
    `_in`, `_not`, `_like` (Regex), `_between` (2 Parameter), `_is_null`, `_is_not_null`
  - Sortierung: `_order_by_<feld>_asc|_desc`; Rückgabetypen `Option<T>`, `Vec<T>`, `Page<T>` (mit `Pageable`-Parameter)
  - Der Macro validiert Feldnamen gegen das `Document`-Struct zur **Compile-Zeit**
    (Tippfehler im Methodennamen = Compile-Fehler mit Hinweis auf gültige Felder).
  - Alles außerhalb der Grammatik → `#[query(...)]`-Escape-Hatch mit Roh-Filter und
    Positions-Platzhaltern.
- Transactions/Sessions: dünner Wrapper über Driver-Sessions (`repo.with_session(...)`), keine
  deklarative `@Transactional`-Magie in V1.
- Automatische Metriken + Spans pro Mongo-Command (über Driver-Command-Events).

## 9. Kafka (`oxide-kafka`)

Auf `rdkafka` (librdkafka-Bindings, Industriestandard für hohen Durchsatz).

- **Consumer:** `#[kafka_listener(topic = "user-events", group_id = "user-service")]` auf
  async Methoden eines Components. Payload wird per Serde-JSON deserialisiert (Content-Type
  erweiterbar). Zugriff auf Key/Headers/Metadaten über optionale Parameter
  (`key: MessageKey`, `headers: MessageHeaders`).
- **Concurrency:** pro Listener konfigurierbar (`oxide.kafka.consumer.concurrency`):
  Partitionen werden parallel verarbeitet, innerhalb einer Partition strikt sequenziell
  (Ordering-Garantie wie Spring Kafka mit Concurrency = Partitionszahl-Kappung).
- **Offset-Management:** Commit nach erfolgreichem Handler-Durchlauf (at-least-once).
- **Fehlerbehandlung (Entscheidung: Retry + DLT ab V1):**
  - Handler-Fehler → Retry mit Exponential Backoff (Default: 3 Versuche, konfigurierbar),
    ohne die Partition für andere Messages zu blockieren länger als nötig.
  - Nach Ausschöpfung → Message inkl. Fehler-Headers (`x-exception-message`,
    `x-original-topic`, `x-original-offset`) in `<topic>.DLT` produziert, Offset committet.
  - DLT-Name und -Aktivierung per Listener konfigurierbar.
- **Producer:** injizierbare Component `KafkaProducer` mit `send(topic, key, &value)` (JSON)
  und `send_raw`; Delivery-Bestätigung als `await`-bares Ergebnis; idempotenter Producer
  per Default aktiviert.
- Automatisch: Consumer-Lag-, Processing-Time-, Producer-Metriken; Span pro Message mit
  `traceparent`-Propagation über Kafka-Headers (Trace-Kette über Servicegrenzen, kompatibel
  mit Spring-Sleuth/OTEL-instrumentierten Java-Services).

## 10. Scheduler

- `#[scheduled(cron = "0 */5 * * * *")]` oder `#[scheduled(fixed_delay = "30s")]` /
  `fixed_rate` auf async Methoden eines Components.
- Läuft auf der Tokio-Runtime; überlappende Ausführungen per Default verhindert
  (fixed_delay-Semantik), Fehler werden geloggt + Metrik gezählt.
- Kein verteiltes Locking in V1 (bei mehreren Replicas laufen Jobs mehrfach —
  im Kochbuch dokumentiert; ShedLock-Äquivalent ist ein späterer Kandidat).

## 11. Observability (`oxide-observability`)

Automatisch aktiv, sobald das Crate eingebunden ist — kein Handanlegen (Micrometer-Prinzip):

- **Metriken** (Prometheus-Endpoint, Micrometer-kompatible Namen/Label-Semantik, damit
  bestehende Grafana-Dashboards weitgehend weiterverwendbar sind):
  - `http_server_requests_seconds` (Histogram; method, uri-Template, status)
  - `http_client_requests_seconds` (Histogram; client, method, status)
  - Kafka: Consumer-Lag, Processing-Time, Retry-/DLT-Zähler, Producer-Send-Latenz
  - Mongo: Command-Latenz pro Operation/Collection, Pool-Metriken
  - Runtime: Tokio-Worker-Auslastung, Task-Queue-Tiefe, Prozess-CPU/RSS/FDs
  - Eigene Metriken: `metrics::counter!/gauge!/histogram!`-Makros (Micrometer-artige API)
- **Tracing:** `tracing` + OpenTelemetry, OTLP-Export (Endpoint per Config), W3C Trace
  Context über HTTP und Kafka. Sampling per Config (Default: parent-based, 100 %).
- **Logs:** `tracing`-basiert; JSON-Format in prod-Profil, menschenlesbar in dev;
  `trace_id`/`span_id` automatisch in jeder Zeile; Log-Level per Config
  (`oxide.logging.level.<target>` — wie `logging.level.*` in Spring).

## 12. Testen (`oxide-test`)

- `#[oxide_test]` bootet die Anwendung im Test mit Test-Profil (`application-test.yml`),
  ohne echten Server-Port-Konflikt (Port 0), und stellt einen vorkonfigurierten Test-Client bereit.
- Components lassen sich für Tests überschreiben (`TestApp::builder().replace::<UserRepository>(mock)`),
  Trait-Injection macht Mocking natürlich (`mockall`-freundlich).
- Testcontainers-Integration für Mongo und Kafka (Redpanda-Container für Geschwindigkeit)
  als vorgefertigte Fixtures.
- Framework-eigene Tests: jede Integration hat Integrationstests gegen echte Container;
  Macro-Tests über `trybuild` (Compile-Fehlermeldungen der Macros werden mitgetestet —
  wichtig, weil verständliche Fehler ein Kernversprechen sind).

## 13. Performance-Leitplanken

Ziel 1.000–10.000 req/s bzw. msg/s pro Service ist mit dem gewählten Stack konservativ
erreichbar (Axum/Tokio-Services liegen typischerweise deutlich darüber). Leitplanken:

- Kein Locking im Request-Pfad des Frameworks (DI-Graph ist nach dem Start immutabel).
- Metriken-Aufzeichnung lock-arm (sharded Histogramme), Tracing-Export gebatcht und
  außerhalb des Request-Pfads.
- Benchmarks im Repo (`criterion` Micro-Benchmarks + Lasttest-Setup mit `oha`/`k6` als
  Referenzszenario), Regressionen werden vor Releases geprüft.
- Definition of Done für Phase 5: Referenz-Service hält 10k req/s bei p99 < 50 ms auf
  einem definierten Referenz-Setup, ohne Memory-Wachstum über 1 h Dauerlast.

## 14. Umsetzungsphasen

1. **Fundament:** Workspace, `oxide-core`, `oxide-macros`, Config-Loading, DI —
   ein Hello-World-Binary bootet, liest `application.yml`, injiziert Components.
2. **Web + Observability:** `oxide-web` (Controller, RFC 7807, Actuator) +
   `oxide-observability` — ab hier ist jede weitere Integration ab Tag 1 instrumentiert.
3. **Integrationen:** `oxide-mongo` (CRUD + Derived Queries), `oxide-kafka`
   (Listener + Retry/DLT + Producer), `oxide-client`, Scheduler.
4. **Pilot:** Port des bestehenden Spring-Boot-Services; Lücken fließen als Anforderungen
   zurück ins Framework.
5. **Härten:** Lasttests gegen die Performance-Ziele, `oxide-test` finalisieren,
   Kochbuch/Doku für das Team (inkl. „Spring-Konzept → Oxide-Äquivalent"-Tabelle).

## 15. Risiken

| Risiko | Umgang |
|---|---|
| Proc-Macro-Komplexität (v. a. Derived Queries) unterschätzt | Grammatik bewusst begrenzt (Abschnitt 8); `trybuild`-Tests für Fehlermeldungen; Escape-Hatch `#[query]` existiert ab Tag 1 |
| Rust-Lernkurve des Teams | Framework kapselt Lifetimes/Ownership-Schmerz; Kochbuch mit Spring-Mapping; Compile-Fehler der Macros werden aktiv auf Verständlichkeit getestet |
| rdkafka/librdkafka-Build (C-Dependency) | `cmake-build`-Feature dokumentiert, CI-Basisimage mit Toolchain; Alternative `rskafka` bewusst verworfen (Feature-Umfang) |
| Version-Drift der Basis-Crates | Workspace-weite Dependency-Versionen, Renovate/Dependabot, Framework kapselt Breaking Changes der Basis-Crates vor den Services |
| Scheduler bei mehreren Replicas läuft mehrfach | In V1 dokumentierte Einschränkung; verteiltes Locking als späterer Starter |
