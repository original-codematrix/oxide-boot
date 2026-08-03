# Oxide Boot Phase 3a (Kafka) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `oxide-kafka`-Starter mit `#[kafka_listeners]`/`#[kafka_listener(...)]`, injizierbarem `KafkaProducer`, Retry + Dead-Letter-Topic, at-least-once Offset-Management, partitionsparalleler Verarbeitung — und vollständiger Observability ab Tag 1 (Processing-/Send-Histogramme, Retry-/DLT-Zähler, Consumer-Lag, Spans mit W3C-`traceparent` über Kafka-Header). Vorgelagert: die drei Fundament-Tickets aus dem 2b-Final-Review, die Kafka voraussetzt (Shutdown-Ordering, Boot-Rollback, HealthIndicator-Seam).

**Architecture:** `oxide-kafka` (Runtime) + `oxide-kafka-macros` auf `rdkafka` (librdkafka). Listener werden als `ListenerDescriptor` per inventory registriert; ein `KafkaListenerManager`-Component startet pro Listener einen `StreamConsumer` und verarbeitet über `split_partition_queue` partitionsparallel (Semaphore = `concurrency`), innerhalb einer Partition strikt sequenziell; Commit nach Handler-Erfolg bzw. nach DLT-Produce. Der `KafkaProducer` (FutureProducer, idempotent) injiziert `traceparent` in Message-Header; die Consumer-Seite extrahiert ihn und parented den Processing-Span — Traces laufen durch Spring- und Oxide-Services hindurch. Metriken über die `metrics`-Facade (kein Zwang zu oxide-observability: ohne Recorder sind sie No-Ops, mit ihm landen sie automatisch im Prometheus-Scrape). Shutdown: Listener drainen vor dem Producer (Dependency-Ordnung), OTEL-Flush garantiert zuletzt über die neue `destroy_order`-Primitive.

**Tech Stack:** rdkafka (features `cmake-build`, `tokio`), testcontainers-rs mit Redpanda für Integrationstests (Docker erforderlich!), bestehender Stack (metrics, tracing, opentelemetry-Propagator-API, serde).

**API-Latitude-Regel (wie in 2b bewährt):** rdkafka-/testcontainers-API-Details in Referenzcode-Blöcken dürfen mechanisch an die resolved Versionen angepasst werden — DIE TESTS UND DIE SEMANTIK SIND DER VERTRAG (Metriknamen/Labels, Header-Namen, Commit-Zeitpunkte, Ordering-Garantien niemals ändern). Bei echten Design-Konflikten: BLOCKED melden statt Semantik verbiegen.

## Global Constraints

- Alles Etablierte gilt weiter: Edition 2024, Versionen nur in `[workspace.dependencies]`, thiserror/kein anyhow, kein `unwrap()`/`expect()` in Library-Code außer geflaggten unreachable-by-construction-Stellen, Englisch, Conventional Commits, TDD mit ROHEM RED/GREEN-Output, Gates `cargo test --workspace` + `cargo clippy --workspace --all-targets --keep-going -- -D warnings`.
- Spec §9 ist der Kafka-Vertrag: at-least-once (Commit NACH Handler-Erfolg), Partition = Ordnungseinheit (parallel über Partitionen, sequenziell innerhalb), Retry mit Exponential Backoff (Default 3 Versuche), danach DLT `<topic>.DLT` mit Headern `x-exception-message`, `x-original-topic`, `x-original-partition`, `x-original-offset`, dann Commit. Producer idempotent per Default.
- Metrik-Verträge (Micrometer-orientiert, alle über `metrics`-Facade):
  - `kafka_listener_seconds{topic, group, result}` Histogram, result ∈ `success|error` (error = einzelner fehlgeschlagener Versuch)
  - `kafka_listener_retries_total{topic, group}`, `kafka_listener_dlt_total{topic, group}` Counter
  - `kafka_consumer_lag{topic, partition, group}` Gauge (aus librdkafka-Statistics)
  - `kafka_producer_send_seconds{topic, result}` Histogram, result ∈ `success|error`
- Span-Verträge: Consumer-Span `kafka.process` (Felder: topic, partition, offset, group), Parent aus `traceparent`-Message-Header (W3C-Propagator; ohne gesetzten globalen Propagator no-op); Producer-Span `kafka.send` (topic), Producer injiziert `traceparent` in die Header des gesendeten Records.
- Kafka-Config unter `oxide.kafka`: `bootstrap-servers` (PFLICHT — Crate eingebunden + Abschnitt fehlt = Fail-Fast beim Boot, Spec §3), `consumer { auto-offset-reset: earliest, concurrency: 4, retry { max-attempts: 3, backoff-initial-ms: 100, backoff-multiplier: 2.0 }, dlt-enabled: true }`, `producer { idempotence: true }`, `properties: { <librdkafka-key>: <value> }` Passthrough-Map (String→String) für beide Client-Typen.
- Integrationstests brauchen Docker (Redpanda-Testcontainer). Ein Container PRO TEST-BINARY (teuer!), geteilt über ein OnceCell-artiges Harness; Tests im Binary serialisieren über die etablierte BOOT_LOCK-Disziplin. Topics pro Test eindeutig benennen (`{test}-{nanos}`), damit Binaries wiederholbar sind.
- inventory-Regeln wie gehabt (prozess-global, Forcing-Imports, eigene Binaries für globalen Zustand).
- Nicht-Ziele 3a: Mongo/Client/Scheduler (3b/3c), Batch-Listener, Transaktionen/exactly-once, Schema-Registry/Avro, verteiltes Locking, Env-Binding-Creation-Pass-Fix (Ticket bleibt offen — `bootstrap-servers` steht immer in der YAML, damit greift Pass 1/2).

## File Structure (Endzustand 3a, neu/geändert)

```
oxide-boot/
├── Cargo.toml                                # + oxide-kafka(-macros), rdkafka, testcontainers
├── README.md                                 # Kafka-Abschnitt + Cheat-Sheet-Zeilen
├── crates/
│   ├── oxide-core/src/container.rs           # Builder: .destroy_order(i32); Doku
│   ├── oxide-boot/src/app.rs                 # stop(): Sortierung nach (destroy_order, reverse ctor); start(): Rollback bei post_construct-Fehler
│   ├── oxide-web/src/health.rs + management.rs  # HealthIndicator-Seam + Aggregation in /actuator/health
│   ├── oxide-observability/src/otel.rs       # OtelLifecycle: destroy_order(100)
│   ├── oxide-kafka/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                        # Module, __private (rdkafka NICHT re-exportiert — internes Detail), prelude-frei wie oxide-web
│   │       ├── config.rs                     # KafkaProps (+ Retry-/Producer-/Consumer-Props), ClientConfig-Bau inkl. properties-Passthrough
│   │       ├── producer.rs                   # KafkaProducer-Component (send/send_raw, Metriken, Span, traceparent-Injection)
│   │       ├── listener.rs                   # ListenerDescriptor + inventory, KafkaListenerManager (Partition-Queues, Semaphore, Commit, Drain)
│   │       ├── retry.rs                      # Backoff-Iterator, DLT-Produce mit Headern
│   │       ├── observe.rs                    # Header-Carrier (Extract/Inject), Metrik-Helfer, Statistics→Lag-Gauges
│   │       └── health.rs                     # Kafka-HealthIndicator (Metadata-Fetch mit Timeout)
│   ├── oxide-kafka-macros/
│   │   └── src/{lib.rs, listeners.rs}        # #[kafka_listeners] Impl-Block-Macro
│   └── oxide-kafka/tests/
│       ├── harness/mod.rs                    # Redpanda-Container + KafkaProps-Helper (shared via #[path])
│       ├── producer.rs                       # Binary 1: Producer E2E
│       ├── listener.rs                       # Binary 2: Listener E2E (Erfolg, Ordering, Commit, Drain)
│       ├── retry_dlt.rs                      # Binary 3: Retry/DLT E2E
│       └── tracing_lag.rs                    # Binary 4: Span-Parenting + Lag-Metrik (InMemory-Exporter → eigenes Binary)
└── examples/rest-api/                        # produziert note-created-Events; neuer Listener loggt sie (Ende-zu-Ende-Demo)
```

Neue `[workspace.dependencies]` (Startpunkte, Latitude-Regel gilt):

```toml
oxide-kafka = { path = "crates/oxide-kafka" }
oxide-kafka-macros = { path = "crates/oxide-kafka-macros" }
rdkafka = { version = "0.37", features = ["cmake-build", "tokio"] }
testcontainers = "0.23"
```

(Falls ein `testcontainers-modules`-Crate mit fertigem Redpanda-Modul in kompatibler Version existiert, darf es zusätzlich verwendet werden; sonst GenericImage `redpandadata/redpanda` mit `--mode dev-container`.)

---

### Task 1: `destroy_order` — deterministischer Shutdown-Slot (Ticket #1)

**Files:**
- Modify: `crates/oxide-core/src/container.rs`, `crates/oxide-boot/src/app.rs`, `crates/oxide-observability/src/otel.rs`
- Test: `crates/oxide-boot/tests/stop_failure.rs` (erweitern) oder neues Binary `stop_ordering.rs`

**Interfaces:**
- Consumes: ComponentDescriptor-Builder (const fn), `Application::stop` (läuft seit 2b alle Hooks durch)
- Produces:
  - Builder-Methode `.destroy_order(order: i32)` (Default 0). Semantik: `stop()` führt pre_destroy-Hooks aus in Sortierung **(destroy_order ASC, dann reverse Konstruktionsreihenfolge)** — alle Order-0-Hooks laufen wie bisher (reverse ctor), höhere Order-Werte danach. Feld `pub destroy_order: i32` am Descriptor (non_exhaustive schützt).
  - `OtelLifecycle` bekommt `.destroy_order(100)` + Kommentar: „flush must run after every span-producing component has stopped" — der bisherige alphabetische Zufall wird zur Garantie.
  - Doku am Builder: Framework-Konvention 0 = normal, 100 = Telemetrie-Flush; Werte dazwischen frei für Starter.

- [ ] **Step 1: Failing Test** — neues Binary `crates/oxide-boot/tests/stop_ordering.rs`: drei `#[component]`s `AlphaStop`/`MidStop` (Order 0, via Macro — Macro-Arg kommt NICHT in 3a, also handgeschriebene Builder-Descriptors für die Order-Fälle) und `LateStop` (destroy_order 100, handgeschrieben). Statischer `Vec<&str>`-Recorder (Mutex) in den Hooks. Boot + stop → Reihenfolge asserten: `["MidStop", "AlphaStop", "LateStop"]` bei Registrierung Alpha vor Mid (ctor: Alpha, Mid → reverse: Mid, Alpha; Late wegen Order zuletzt). RAW RED (Builder-Methode fehlt).
- [ ] **Step 2: Implementierung** — Builder-Methode (const fn, Feld-Default 0 im `builder()`); in `app.rs::stop` vor der Hook-Schleife: Indizes der order-Liste stabil sortieren nach `(destroy_order, Position-in-reverse-ctor)` — konkret: `let mut hooks: Vec<&d> = self.order.iter().rev().collect(); hooks.sort_by_key(|d| d.destroy_order);` (stable sort erhält die Reverse-Reihenfolge innerhalb gleicher Order) — dann wie bisher alle ausführen, Fehler sammeln. OtelLifecycle-Descriptor um `.destroy_order(100)`.
- [ ] **Step 3: GREEN** — neues Binary + `cargo test --workspace` + Clippy-Gate, RAW.
- [ ] **Step 4: Commit** — `feat(core,boot)!: destroy_order shutdown slots; otel flush guaranteed last`

---

### Task 2: Boot-Failure-Rollback (Ticket #4)

**Files:**
- Modify: `crates/oxide-boot/src/app.rs`
- Test: NEUES Binary `crates/oxide-boot/tests/boot_rollback.rs`

**Interfaces:**
- Consumes: `AppBuilder::start` (bricht heute bei post_construct-Fehler ab, ohne bereits gestartete Components zu stoppen — Management-Server lief dann weiter, empirisch belegt im 2b-Review)
- Produces: Wenn ein post_construct-Hook fehlschlägt, führt `start()` VOR der Fehlerrückgabe die pre_destroy-Hooks aller Components aus, deren post_construct bereits ERFOLGREICH lief (in der Task-1-Shutdown-Sortierung, beschränkt auf diese Teilmenge). Rollback-Fehler werden geloggt (`tracing::error!`), ändern aber den zurückgegebenen Original-Fehler nicht. Doc-Kommentar am start().

- [ ] **Step 1: Failing Test** — `boot_rollback.rs`: Component `AServer` (post_construct setzt STARTED-Counter, pre_destroy setzt STOPPED-Counter — beide handgeschrieben oder via Macro-Args) + `ZFailing` (post_construct schlägt fehl; Z sortiert nach A, läuft also nach dessen Erfolg). start() → Err asserten UND `STOPPED == STARTED == 1` (Rollback lief). RAW RED (heute bleibt STOPPED bei 0).
- [ ] **Step 2: Implementierung** — in der post_construct-Schleife die bereits erfolgreichen Descriptors sammeln; im Fehlerfall Rollback-Helfer (geteilte Logik mit stop(): kleine private fn `run_pre_destroys(ctx, descriptors) -> Option<BootError>` extrahieren, von stop() und Rollback genutzt — DRY statt Copy).
- [ ] **Step 3: GREEN** + Gates, RAW.
- [ ] **Step 4: Commit** — `fix(boot): roll back started components when a later post_construct fails`

---

### Task 3: HealthIndicator-Seam (Ticket #6)

**Files:**
- Modify: `crates/oxide-web/src/health.rs`, `crates/oxide-web/src/management.rs`, `crates/oxide-web/src/lib.rs`
- Test: `crates/oxide-web/tests/server.rs` (erweitern)

**Interfaces:**
- Consumes: Management-Router, Readiness-Mechanik
- Produces:
  - `pub struct HealthIndicator { pub name: &'static str, pub check: fn(&Context) -> BoxFuture<'static, HealthStatus> }` + `inventory::collect!`; `pub enum HealthStatus { Up, Down { detail: String } }`
  - `/actuator/health` aggregiert: Gesamt = UP nur wenn Readiness UP UND alle Indicators Up; Body erweitert um `"components": { "<name>": { "status": "UP"|"DOWN", "detail"? } }` (Spring-Actuator-Form). `/liveness`/`/readiness` unverändert. Indicator-Checks laufen pro Request mit Timeout 2s (Timeout ⇒ Down mit detail "timed out").
  - Der Check bekommt `&Context` beim ROUTER-BAU aufgelöst (Closure-Capture wie überall): konkret `check`-fn wird beim Mount einmal mit ctx aufgerufen? NEIN — Checks brauchen Laufzeit-Zugriff: `check` ist fn(&Context)->Future, der Health-Handler cloned sich beim Router-Bau die benötigten Inject-Handles NICHT generisch vorab — stattdessen: Handler hält `Arc<Context>`? Context ist nicht als Arc im Router verfügbar. LÖSUNG (verbindlich): `HealthIndicator.register: fn(&Context) -> Option<RegisteredCheck>` wobei `RegisteredCheck = Box<dyn Fn() -> BoxFuture<'static, HealthStatus> + Send + Sync>` — beim Management-Router-Bau wird jeder Indicator EINMAL mit &Context registriert (löst seine Inject-Handles auf und moved sie in die Box-Closure); `None` = Indicator nicht aktiv (Component fehlt). Der Handler iteriert die registrierten Checks.
- [ ] **Step 1: Failing Test** — in `tests/server.rs`: Test-Indicator (inventory-submit) mit schaltbarem `AtomicBool` (static): true ⇒ `/actuator/health` 200 + `components.test-indicator.status == "UP"`; auf false schalten ⇒ 503 + DOWN + detail. BOOT_LOCK-diszipliniert. RAW RED.
- [ ] **Step 2: Implementierung** wie oben; Aggregation im bestehenden health-Handler des Management-Routers.
- [ ] **Step 3: GREEN** + Gates (auch rest-api/observability-Binaries — sie registrieren keine Indicators, Aggregat bleibt reines Readiness-Verhalten), RAW.
- [ ] **Step 4: Commit** — `feat(web): HealthIndicator registry aggregated into /actuator/health`

---

### Task 4: Scaffold oxide-kafka + Test-Harness (Redpanda)

**Files:**
- Modify: `Cargo.toml` (Members + Deps)
- Create: `crates/oxide-kafka/Cargo.toml`, `src/lib.rs`, `src/config.rs`; `crates/oxide-kafka-macros/{Cargo.toml, src/lib.rs}`; `crates/oxide-kafka/tests/harness/mod.rs`, `crates/oxide-kafka/tests/producer.rs` (nur Harness-Smoke)

**Interfaces:**
- Consumes: —
- Produces:
  - Beide Crates buildbar; oxide-kafka deps: oxide-core, oxide-boot, oxide-kafka-macros, rdkafka, serde, serde_json, metrics, tracing, opentelemetry, tokio, thiserror, futures, inventory; dev: testcontainers, tempfile, oxide-observability (für Tracing-Tests später)
  - `config.rs`: `KafkaProps` via `#[config(prefix = "oxide.kafka")]` OHNE default-Flag (bootstrap-servers ist Pflicht ⇒ Fail-Fast wenn Crate eingebunden aber Abschnitt fehlt — genau das Spec-§3-Verhalten; Sub-Structs mit serde-defaults wie in Global Constraints; expliziter Default NUR für die Sub-Structs). Fn `client_config(&KafkaProps, extra: &[(&str, String)]) -> rdkafka::ClientConfig` inkl. properties-Passthrough.
  - Harness (`tests/harness/mod.rs`, eingebunden via `#[path = "harness/mod.rs"] mod harness;` pro Binary): startet EINEN Redpanda-Container pro Binary (tokio OnceCell), liefert `bootstrap_servers()`-String + `unique_topic(prefix)`-Helper + fertigen tempdir-Config-Writer. VORAB-CHECK: Wenn Docker nicht läuft, panict das Harness mit klarer Meldung („Kafka integration tests require Docker — start Docker Desktop or skip with --skip …"). Erster Smoke-Test in `producer.rs`: Container startet, rdkafka-AdminClient/Metadata-Fetch erreicht den Broker.
- [ ] **Step 1: Docker-Vorabprüfung** — `docker info` im Worktree ausführen; wenn Docker fehlt: STATUS BLOCKED an den Controller melden (der Mensch muss Docker starten) statt weiterzubauen.
- [ ] **Step 2: Scaffold + Harness + Smoke-Test** (RED: Crate fehlt → Compile; dann Container-Smoke GREEN, RAW-Output des ersten erfolgreichen Container-Starts ins Report inkl. Dauer)
- [ ] **Step 3: Gates** (Kafka-Binaries laufen mit; Buildzeit von librdkafka/cmake dokumentieren)
- [ ] **Step 4: Commit** — `chore(kafka): scaffold oxide-kafka crates with redpanda test harness`

---

### Task 5: KafkaProducer (Send, Metriken, Span, traceparent-Injection)

**Files:**
- Create: `crates/oxide-kafka/src/producer.rs`, `src/observe.rs` (Header-Carrier Inject-Seite, Metrik-Helfer)
- Test: `crates/oxide-kafka/tests/producer.rs` (erweitern)

**Interfaces:**
- Consumes: KafkaProps, Harness
- Produces:
  - `pub struct KafkaProducer` (Component via handgeschriebenem Builder-Descriptor; deps: KafkaProps): wrappt `FutureProducer` (idempotence aus Props, Default an); `pub async fn send<T: Serialize>(&self, topic: &str, key: &str, value: &T) -> Result<Delivery, KafkaError>` (JSON) + `pub async fn send_raw(&self, topic: &str, key: Option<&[u8]>, payload: &[u8], headers: Option<OwnedHeaders>) -> Result<Delivery, KafkaError>`; `Delivery { partition, offset }`; `pub enum KafkaError` (thiserror: Config/Send{topic}/Serialize/Timeout…)
  - Jeder send: Span `kafka.send` (topic-Feld), `traceparent` aus dem AKTUELLEN Span-Kontext in die Record-Header injiziert (W3C-Propagator, Inject-Carrier über OwnedHeaders — no-op ohne globalen Propagator), Histogram `kafka_producer_send_seconds{topic, result}`.
  - pre_destroy: `flush(timeout)` des Producers (Restnachrichten raus), destroy_order 0 (vor OTEL-Flush automatisch).
- [ ] **Step 1: Failing E2E** — Boot mit Harness-Config; `producer.send(topic, "k1", &Note{..})`; danach mit einem rohen rdkafka-BaseConsumer (Testcode) die Message lesen und asserten: Payload-JSON korrekt, Key korrekt, Header `traceparent` VORHANDEN wenn ein Span aktiv war (Test setzt Propagator via oxide-observability-Seam ODER direkt `set_text_map_propagator` im Test — dokumentieren). Plus Scrape-freier Metrik-Check ist hier nicht nötig (Metriken via Recorder-Handle nur wenn oxide-observability geladen — der Producer-Test prüft stattdessen, dass send Ok liefert und das Histogram-Makro nicht panict; der volle Scrape-Beweis kommt in Task 9-Beispiel). RAW RED zuerst (Typen fehlen).
- [ ] **Step 2: Implementierung**, **Step 3: GREEN + Gates**, **Step 4: Commit** — `feat(kafka): idempotent KafkaProducer with send metrics, span and traceparent injection`

---

### Task 6: `#[kafka_listeners]`-Macro + ListenerDescriptor

**Files:**
- Create: `crates/oxide-kafka-macros/src/listeners.rs`; Modify: macros lib.rs, oxide-kafka lib.rs (`__private`, Re-Export `pub use oxide_kafka_macros::kafka_listeners;`)
- Create: `crates/oxide-kafka/src/listener.rs` (nur Descriptor-Teil)
- Test: trybuild-Fälle in `crates/oxide-kafka/tests/ui/` + Unit-Expansion-Smoke im Listener-E2E (Task 7)

**Interfaces:**
- Consumes: Context/Inject via `::oxide_kafka::__private`
- Produces:
  - Außen `#[kafka_listeners]` auf einem impl-Block (Controller-Muster: verarbeitet und STRIPPT innere Attribute), innen `#[kafka_listener(topic = "...", group_id = "...")]` auf async Methoden (`group_id` optional → Default `{oxide.application.name}`-Fallback zur Laufzeit). Methodenvertrag: `async fn name(&self, payload: T) -> Result<(), E>` wobei `T: DeserializeOwned` (erster Nicht-self-Param = Payload), optional weitere Params `key: MessageKey` (`pub struct MessageKey(pub Option<String>)`) und `headers: MessageHeaders` (`pub struct MessageHeaders(pub Vec<(String, Vec<u8>)>)`) in beliebiger Reihenfolge NACH dem Payload, erkannt am Typnamen (letztes Pfadsegment `MessageKey`/`MessageHeaders`); `E: Display` beliebig. `&self`-Pflicht + plain-`ident: Type`-Params wie beim Controller (gleiche Fehlermeldungen sinngemäß).
  - Pro Listener-Methode ein inventory-Submit `ListenerDescriptor { topic: &'static str, group_id: Option<&'static str>, listener_name: &'static str /* "Type::method" */, start: fn(&Context, &ListenerInfra) -> Result<PreparedListener, KafkaError> }` — `start` löst das Component-Handle EINMAL auf und liefert eine boxed Handler-Closure `Box<dyn Fn(ReceivedMessage) -> BoxFuture<'static, Result<(), String>> + Send + Sync>` (Fehler als String via Display — fürs DLT-Header-Detail reicht das); `ReceivedMessage { payload: Vec<u8>, key: Option<Vec<u8>>, headers: Vec<(String, Vec<u8>)>, topic, partition, offset }`. JSON-Deserialisierung passiert IN der generierten Closure (Deserialize-Fehler ⇒ `Err("deserialize: …")` ⇒ normale Retry/DLT-Maschinerie).
  - trybuild: (1) Methode ohne `&self`, (2) fehlendes `topic`-Arg, (3) `#[kafka_listeners]` auf fn statt impl — Meldungen analog Controller-Stil, .stderr committen.
- [ ] Steps: Macro implementieren → trybuild overwrite → MANUELL prüfen → Gates → Commit `feat(kafka): #[kafka_listeners] macro with listener descriptors`

---

### Task 7: KafkaListenerManager — Consume, Partition-Parallelität, Commit, Drain

**Files:**
- Modify: `crates/oxide-kafka/src/listener.rs`
- Test: NEUES Binary `crates/oxide-kafka/tests/listener.rs`

**Interfaces:**
- Consumes: ListenerDescriptor, KafkaProps, Harness
- Produces:
  - `KafkaListenerManager`-Component (handgeschriebener Descriptor; deps: KafkaProps, KafkaProducer — Producer wegen DLT in Task 8). post_construct: pro ListenerDescriptor einen `StreamConsumer` (group_id aus Descriptor oder App-Name; `enable.auto.commit=false`, `auto.offset.reset` aus Props) erzeugen, subscriben und einen Supervisor-Task spawnen. Verarbeitung: Hauptloop `recv()` je Message → Route in eine Per-Partition-Worker-Queue (`split_partition_queue` wo verfügbar; SONST akzeptierte Vereinfachung mit identischer Garantie: ein `mpsc`-Worker pro (topic,partition), Erzeugung on-demand, globale `Semaphore` mit `consumer.concurrency` Permits begrenzt gleichzeitige Handler-Läufe) — innerhalb einer Partition strikt sequenziell, Commit (`commit_message`, Async) NACH Handler-Erfolg. WICHTIG bei der Worker-Variante: der Hauptloop darf erst `recv()`en, wenn die Partition-Queue Kapazität hat (bounded channel), sonst leakt Ordnung nicht, aber Memory.
  - Metrik ab diesem Task: `kafka_listener_seconds{topic, group, result}` um jeden Handler-Lauf.
  - pre_destroy (destroy_order 0; läuft VOR Producer-Flush wegen Dependency-Reverse): Supervisor-Tasks stoppen (CancellationToken/watch), in-flight Handler zu Ende laufen lassen (Timeout = `shutdown-grace` analog Web), Consumer schließen. „Drain": keine neuen recv(), laufende Handler fertig, letzte Commits raus.
- [ ] **Step 1: Failing E2E** (`tests/listener.rs`, eigenes Binary): Test-Component mit `#[kafka_listeners]`-Listener auf `unique_topic`, schreibt empfangene Payloads in statisches Mutex-Vec. Produce 5 Messages mit DEMSELBEN Key (eine Partition) → asserten: alle 5 in SENDEREIHENFOLGE empfangen (Ordering-Beweis), danach App-Restart im selben Binary (zweiter Boot, gleiche group) → asserten: KEINE Redelivery (Commit-Beweis). RAW RED.
- [ ] **Step 2: Implementierung**, **Step 3: GREEN + Gates** (Flaky-Achtung: großzügige Timeouts, RAW-Ausgabe), **Step 4: Commit** — `feat(kafka): listener manager with partition-ordered processing and post-success commits`

---

### Task 8: Retry + Backoff + Dead-Letter-Topic

**Files:**
- Create: `crates/oxide-kafka/src/retry.rs`; Modify: `listener.rs`
- Test: NEUES Binary `crates/oxide-kafka/tests/retry_dlt.rs`

**Interfaces:**
- Consumes: RetryProps, KafkaProducer
- Produces: Handler-Fehler ⇒ Retry mit Exponential Backoff (`max-attempts` GESAMT-Versuche inkl. Erstversuch — dokumentieren!, `backoff-initial-ms`, `backoff-multiplier`), Partition bleibt währenddessen sequenziell blockiert (bewusst — Ordnung vor Durchsatz, wie Spring mit BlockingRetry); nach Ausschöpfung: wenn `dlt-enabled` (Default) Message nach `<topic>.DLT` mit Original-Payload/-Key + Headern `x-exception-message` (Display des letzten Fehlers), `x-original-topic`, `x-original-partition`, `x-original-offset` (Strings), DANN Commit; wenn DLT-Produce selbst fehlschlägt: NICHT committen (Redelivery statt Message-Verlust — at-least-once), Fehler loggen. `dlt-enabled: false` ⇒ nach Ausschöpfung loggen + committen (Verhalten dokumentieren). Metriken: `kafka_listener_retries_total` je Wiederholungsversuch, `kafka_listener_dlt_total` je DLT-Produce.
- [ ] **Step 1: Failing E2E**: Listener der bei den ersten 2 Versuchen `Err` liefert, beim 3. Ok (statischer AtomicUsize) ⇒ asserten: genau 3 Versuche, Message NICHT im DLT. Zweiter Testfall (permanent Err, max-attempts 2, backoff-initial 50ms): DLT-Message mit rohem Consumer lesen ⇒ Payload identisch + alle 4 Header korrekt (Werte prüfen!), Original-Offset committed (kein Redelivery nach Restart). RAW RED.
- [ ] **Step 2-4**: Implementierung → GREEN + Gates → Commit `feat(kafka): retry with exponential backoff and dead-letter topic`

---

### Task 9: Consumer-Observability — Span-Parenting + Consumer-Lag

**Files:**
- Modify: `crates/oxide-kafka/src/{listener.rs, observe.rs}`
- Test: NEUES Binary `crates/oxide-kafka/tests/tracing_lag.rs`

**Interfaces:**
- Consumes: oxide-observability-Test-Seam (`install_in_memory…`), Statistics-Callback von rdkafka
- Produces:
  - Um jeden Handler-Lauf: Span `kafka.process` (topic, partition, offset, group), Parent per W3C-Extract aus den Message-Headern (Carrier über `ReceivedMessage.headers`); Metriken aus Task 7/8 laufen innerhalb des Spans.
  - Consumer-Lag: `statistics.interval.ms` (z. B. 5000, konfigurierbar via properties-Passthrough oder festem Default) + rdkafka `ClientContext::stats`-Callback → pro Topic/Partition `consumer_lag` aus den Statistics (committed/hi-offsets) → `metrics::gauge!("kafka_consumer_lag", "topic" => …, "partition" => …, "group" => …)`.
  - Producer→Consumer-Trace-Kette: der in Task 5 injizierte `traceparent` wird hier extrahiert — Ende-zu-Ende ein Trace.
- [ ] **Step 1: Failing E2E** (eigenes Binary — globaler Tracer-Provider!): InMemory-Exporter installieren; im Test einen Span öffnen und DARIN `producer.send(...)`; Listener empfängt; stop + Flush ⇒ asserten: exportierte Spans enthalten `kafka.send` und `kafka.process` MIT DERSELBEN trace_id (Kette bewiesen). Zweiter Assert im selben Test (Lag): nach dem Konsum > Statistics-Intervall warten ODER Intervall klein konfigurieren; Recorder-Handle-Render enthält `kafka_consumer_lag` mit topic-Label (Wert-Assert nur ≥ 0 — Redpanda-Timing). RAW RED.
- [ ] **Step 2-4**: Implementierung → GREEN + Gates → Commit `feat(kafka): kafka.process spans with w3c parenting and consumer lag gauge`

---

### Task 10: Kafka-HealthIndicator + Beispiel + README

**Files:**
- Create: `crates/oxide-kafka/src/health.rs`; Modify: `examples/rest-api/*`, `README.md`
- Test: rest-api-Test erweitern

**Interfaces:**
- Consumes: HealthIndicator-Seam (Task 3), KafkaProducer/Props
- Produces:
  - HealthIndicator `"kafka"`: Metadata-Fetch gegen den Broker mit 2s-Timeout (spawn_blocking — rdkafka-Metadata ist blocking) ⇒ Up/Down{detail}. Registriert nur wenn KafkaProps im Context (register-Seam liefert None sonst).
  - Beispiel rest-api: `oxide-kafka` + Forcing-Import; `application.yml` + `kafka: { bootstrap-servers: "localhost:9092" }`; beim Note-Create `producer.send("notes-events", &note.id.to_string(), &note)`; neuer `#[kafka_listeners]`-Component loggt Events. Der INTEGRATIONSTEST des Beispiels bootet jetzt mit Kafka-Pflicht-Config: Harness-ähnlicher Redpanda-Start ODER (einfacher, verbindlich): der rest-api-Test bekommt einen eigenen Redpanda via testcontainers-dev-dep und setzt `OXIDE_KAFKA_BOOTSTRAP_SERVERS` env-Override (Key existiert in YAML ⇒ Binding-Pass 1 greift — dogfooding). Test: Note anlegen → Event kommt beim Listener an → `/actuator/health` zeigt `components.kafka.status == "UP"` → Scrape zeigt `kafka_producer_send_seconds` + `kafka_listener_seconds`.
  - README: „## Kafka (oxide-kafka)"-Abschnitt (Listener-Snippet, Producer, Retry/DLT-Defaults, Metrik-/Span-Liste, Aktivierung inkl. Forcing-Import + Pflicht-Config) + Cheat-Sheet: `@KafkaListener` → `#[kafka_listener(...)]`; `KafkaTemplate.send` → `KafkaProducer::send`; `DeadLetterPublishingRecoverer` → eingebaut (`<topic>.DLT`); `spring.kafka.bootstrap-servers` → `oxide.kafka.bootstrap-servers`; Consumer-Lag-Metrik → `kafka_consumer_lag`.
- [ ] Steps: TDD (Health-Test zuerst gegen laufenden Container, dann Beispiel-E2E) → Gates → Commit `feat: kafka health indicator, rest-api event flow, docs`

---

### Task 11: Abschluss — Gates, manueller Lauf, Spec-Abgleich

- [ ] `cargo test --workspace` grün (inkl. aller Kafka-Binaries gegen echte Container), Clippy-Gate sauber; Gesamtlaufzeit der Kafka-Binaries im Report dokumentieren
- [ ] Manueller Lauf: Redpanda via `docker run` (Kommando ins Report), `cd examples/rest-api && cargo run` mit lokalem Broker; curl: Note anlegen → Listener-Log erscheint; `/actuator/health` (kafka UP), `/actuator/prometheus` (`kafka_producer_send_seconds`, `kafka_listener_seconds`, `kafka_consumer_lag` sichtbar); Broker stoppen → health kafka DOWN (detail) bei weiterlaufender App; SIGTERM → Drain-Logs (Listener → Producer-Flush → OTEL → Server) — RAW-Belege
- [ ] Spec-Abgleich §9: alles abgedeckt AUSSER bewusst: Batch/exactly-once/Schema-Registry (nicht V1), `#[kafka_listener]` braucht den äußeren `#[kafka_listeners]`-Block (Macro-Realität, im README dokumentiert — Spec-Sample zeigt nur die Methode)
- [ ] Ledger-/Ticket-Stand aktualisieren: Tickets #1/#4/#6 geschlossen; offen bleiben #2 (Init-Seam-Split), #3 (Env-Creation-Pass), #5 (Collector-E2E, testing-Feature), plus alles Neue aus den Reviews
- [ ] Danach: Phase 3b (Mongo) planen

## Risiken (3a-spezifisch)

| Risiko | Umgang |
|---|---|
| librdkafka-cmake-Build (C-Dependency) schlägt lokal fehl | Task 4 prüft früh; BLOCKED an den Menschen mit konkreter Toolchain-Meldung (cmake/openssl), nicht raten |
| Docker fehlt / Container-Flakiness in Tests | Harness-Vorabcheck mit klarer Meldung; großzügige Timeouts; unique Topics; ein Container pro Binary |
| Partition-Parallelität subtil falsch (Ordering-Verletzung) | Task-7-Test beweist Ordnung über 5 Messages/einem Key; Review prüft die Queue-Mechanik als benannten Risk |
| rdkafka-API-Drift vs Referenzcode | Latitude-Regel; Tests sind der Vertrag |
| DLT-Produce-Fehler ⇒ Doppelverarbeitung | bewusst at-least-once (kein Commit ohne DLT-Erfolg) — dokumentiert, Review-checkbar |
