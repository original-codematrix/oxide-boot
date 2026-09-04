# Phase 4b — Härten, robuster machen, konsolidieren: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Alle 33 Findings aus Anhang A der 4a-Spec abarbeiten: Angriffsfläche der HTTP-Server begrenzen, Injection- und Leak-Pfade in oxide-mongo/oxide-client schließen, Kafka-Shutdown und Rebalance korrekt machen, ein einziges Registrierungsmuster etablieren und die aufgeschobenen Major-Upgrades ziehen.

**Architecture:** Drei Blöcke, jeder für sich lauffähig und grün. Block A ändert die HTTP-Oberfläche (neue Config-Properties, Tower-Layer, Health-Cache), Block B die Laufzeitrobustheit (Kafka-Worker-Lifecycle, Fehler statt Panics), Block C Struktur und Dependencies. Jede Verhaltensänderung wird durch einen Test abgesichert, der ohne die Änderung fehlschlägt.

**Tech Stack:** Rust 1.88, axum 0.8, tower-http 0.7 (neu), rdkafka, mongodb 3.8, reqwest, opentelemetry 0.32 (Upgrade), testcontainers 0.28, trybuild.

**Spec:** `docs/design/specs/2026-09-02-phase-4b-hardening-design.md`

## Global Constraints

- Branch: von `main` (HEAD `ad0ebd8`) abzweigen, kein Remote, nichts pushen.
- MSRV 1.88, `rust-version` im Workspace bleibt `1.88`.
- Nach **jedem** Task: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` sauber, und die vom Task berührten Tests grün. `make ci` läuft am Ende jedes Blocks (Tasks 10, 16, 23), nicht nach jedem Task — die Container-Suite dauert 5–8 Minuten.
- Docker muss laufen. Container-Tests pinnen die Host-Ports 9092 und 27017; bei „port in use“ `docker ps -a` und `docker rm -f <id>`.
- Testzahl darf nie unter den Stand des Vortasks fallen. Ausgangswert: **92**.
- Neue Config-Keys werden kebab-case geschrieben und über `#[serde(rename = "...")]` gebunden, wie `shutdown-grace-seconds` in `ServerProps`.
- Commit-Messages: Conventional Commits, jeweils mit den Trailern:
  ```
  Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE
  ```
- Keine Features aus Anhang A.4 (Scheduler, Validierung, Trait-Injection, Pagination, `TestApp::replace`, `@Import`). Kein Client-Retry.
- `F5` (dupliziertes `name_to_snake`) wird bewusst **nicht** angefasst.

---

# Block A — Angriffsfläche (Tasks 1–10)

### Task 1: Bind-Adresse für beide Server

**Files:**
- Modify: `crates/oxide-web/src/server.rs` (`ServerProps`, `WebServer`), `crates/oxide-web/src/management.rs` (`ManagementProps`, `ManagementServer`), `crates/oxide-web/src/serve.rs` (Bind-Aufruf)
- Test: `crates/oxide-web/src/server.rs` (Unit), `crates/oxide-web/tests/server.rs` (Integration)

**Interfaces:**
- Produces: `ServerProps.address: String` und `ManagementProps.address: String`, beide Default `"0.0.0.0"`; die Serving-Funktion bindet auf `format!("{address}:{port}")` statt auf einen hartkodierten Host.

- [ ] **Step 1: Failing Test für den Default und das Binden**

In `crates/oxide-web/src/server.rs` im `mod tests`:

```rust
#[test]
fn server_props_default_address_is_all_interfaces() {
    assert_eq!(ServerProps::default().address, "0.0.0.0");
}

#[test]
fn server_props_deserialize_reads_address() {
    let props: ServerProps = serde_yaml_ng::from_str("address: 127.0.0.1\nport: 1234\n").unwrap();
    assert_eq!(props.address, "127.0.0.1");
    assert_eq!(props.port, 1234);
}
```

In `crates/oxide-web/tests/server.rs` (nutzt das dort etablierte Boot-Muster mit `OXIDE_SERVER_PORT=0`):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn server_binds_to_configured_loopback_address() {
    let env = std::collections::HashMap::from([
        ("OXIDE_SERVER_PORT".to_string(), "0".to_string()),
        ("OXIDE_SERVER_ADDRESS".to_string(), "127.0.0.1".to_string()),
        ("OXIDE_MANAGEMENT_PORT".to_string(), "0".to_string()),
    ]);
    let app = OxideApplication::builder().base_dir(env!("CARGO_MANIFEST_DIR")).env(env).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();
    assert_eq!(addr.ip().to_string(), "127.0.0.1");
    app.stop().await.unwrap();
}
```

- [ ] **Step 2: Tests laufen lassen, Fehlschlag bestätigen**

Run: `cargo test -p oxide-web 2>&1 | tail -20`
Expected: Kompilierfehler „no field `address`“ bzw. fehlgeschlagene Assertion.

- [ ] **Step 3: Implementieren**

In beiden Props-Structs ein Feld ergänzen (Muster wie beim vorhandenen `port`):

```rust
fn default_address() -> String {
    "0.0.0.0".to_string()
}
```
```rust
    #[serde(default = "default_address")]
    pub address: String,
```
Das handgeschriebene `impl Default` beider Structs um `address: default_address()` erweitern. In `serve.rs` die Bind-Adresse aus den Props durchreichen: die Serving-Funktion nimmt zusätzlich `address: &str` und bindet `TcpListener::bind((address, port))` statt der bisherigen Konstante. `WebServer::start` und `ManagementServer::start` reichen `&self.props.address` durch.

- [ ] **Step 4: Tests laufen lassen**

Run: `cargo test -p oxide-web 2>&1 | grep -E '^test result'`
Expected: alle grün, Anzahl gegenüber vorher um 3 gestiegen.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-web
git commit -m "feat(web)!: configurable bind address for both servers

oxide.server.address and oxide.management.address, both defaulting to
0.0.0.0 so kubelet probes against the pod IP keep working. Deployments that
want the actuator on loopback only can now say so.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 2: Endpoint-Freigabe für den Actuator

**Files:**
- Modify: `crates/oxide-web/src/management.rs` (`ManagementProps`, `ManagementRoute`, `build_management_router`), `crates/oxide-web/src/health.rs` (Registrierung der Health-Routen), `crates/oxide-observability/src/actuator.rs` (Prometheus-Route)
- Test: `crates/oxide-web/src/management.rs` (Unit), `crates/oxide-observability/tests/observability.rs` (Integration)

**Interfaces:**
- Consumes: `ManagementProps` aus Task 1.
- Produces: `ManagementRoute` bekommt ein zusätzliches Feld `endpoint: &'static str` (der Actuator-Name, z. B. `"health"`, `"info"`, `"prometheus"`). `ManagementProps.endpoints_exposure: String` (Default `"health,info"`), gebunden an `oxide.management.endpoints.exposure`. Eine Hilfsfunktion `fn is_exposed(exposure: &str, endpoint: &str) -> bool` behandelt `"*"` als „alles“ und trimmt die kommaseparierte Liste.

- [ ] **Step 1: Failing Tests**

In `crates/oxide-web/src/management.rs` im `mod tests`:

```rust
#[test]
fn exposure_matches_listed_endpoints_only() {
    assert!(is_exposed("health,info", "health"));
    assert!(is_exposed(" health , info ", "info"));
    assert!(!is_exposed("health,info", "prometheus"));
}

#[test]
fn exposure_wildcard_matches_everything() {
    assert!(is_exposed("*", "prometheus"));
}

#[test]
fn management_props_default_exposes_health_and_info_only() {
    assert_eq!(ManagementProps::default().endpoints_exposure, "health,info");
}
```

In `crates/oxide-observability/tests/observability.rs` (dort existiert bereits ein Test, der `/actuator/prometheus` scrapt — dieser wird angepasst, statt einen zweiten zu schreiben):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn prometheus_route_is_absent_unless_exposed() {
    // Boot ohne exposure-Override: /actuator/prometheus darf es nicht geben.
    // (Boot-Muster und Port-0-Env wie im bestehenden Test der Datei.)
    let status = reqwest::get(format!("http://{mgmt_addr}/actuator/prometheus")).await.unwrap().status();
    assert_eq!(status, 404);
}
```
Der bestehende Scrape-Test setzt zusätzlich `OXIDE_MANAGEMENT_ENDPOINTS_EXPOSURE=health,info,prometheus`.

- [ ] **Step 2: Fehlschlag bestätigen**

Run: `cargo test -p oxide-web -p oxide-observability 2>&1 | tail -20`
Expected: `is_exposed` nicht gefunden; der 404-Test bekommt 200.

- [ ] **Step 3: Implementieren**

`ManagementProps` um das Feld erweitern:
```rust
fn default_exposure() -> String {
    "health,info".to_string()
}
```
```rust
    #[serde(rename = "endpoints", default)]
    pub endpoints: EndpointsProps,
```
mit
```rust
#[derive(Debug, Clone, serde::Deserialize)]
pub struct EndpointsProps {
    #[serde(default = "default_exposure")]
    pub exposure: String,
}
```
und passendem `Default`. (Zugriff dann `props.endpoints.exposure`; die Tests oben entsprechend auf `endpoints.exposure` anpassen, falls diese Schachtelung gewählt wird — sie bildet den YAML-Baum `oxide.management.endpoints.exposure` ab und ist deshalb der Vorzug.)

`ManagementRoute` bekommt `pub endpoint: &'static str`. Alle Registrierungsstellen (`health.rs`, `management.rs` für `/actuator/info`, `oxide-observability/src/actuator.rs`) setzen den Namen. `build_management_router` überspringt Routen, deren `endpoint` nicht freigegeben ist.

- [ ] **Step 4: Tests grün**

Run: `cargo test -p oxide-web -p oxide-observability 2>&1 | grep -E '^test result'`

- [ ] **Step 5: rest-api-Beispiel und Kafka-Test nachziehen**

`examples/rest-api/application.yml` bekommt:
```yaml
  management:
    endpoints:
      exposure: "health,info,prometheus"
```
`examples/rest-api/tests/api.rs` und `crates/oxide-kafka/tests/*`, die den Scrape prüfen, setzen dieselbe Env bzw. verlassen sich auf die yml.

Run: `cargo test -p rest-api 2>&1 | grep -E '^test result'`

- [ ] **Step 6: Commit**

```bash
git add crates examples
git commit -m "feat(web)!: actuator endpoint exposure, prometheus off by default

oxide.management.endpoints.exposure (default \"health,info\") decides which
actuator routes get mounted; everything else is simply absent. Scraping
setups must now opt in with \"health,info,prometheus\" — the rest-api example
shows how.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 3: Request-Timeout, Body-Limit, optional CORS

**Files:**
- Modify: `crates/oxide-web/Cargo.toml` (+`tower-http`), `Cargo.toml` (workspace dep), `crates/oxide-web/src/routing.rs` (`RouterLayer`-Signatur), `crates/oxide-web/src/server.rs` + `management.rs` (Props), `crates/oxide-web/src/lib.rs` (Layer-Registrierung), `crates/oxide-observability/src/http.rs` (bestehender RouterLayer)
- Test: `crates/oxide-web/tests/limits.rs` (neu)

**Interfaces:**
- Produces: `RouterLayer.apply: fn(&Context, Router) -> Router` (bisher `fn(Router) -> Router`). Neue Properties: `oxide.server.request-timeout` (Default `"30s"`), `oxide.server.max-body-size` (Default `"2MiB"`), `oxide.server.cors.allowed-origins` (Default leer). Für die Zeit- und Größenangaben genügen `u64`-Sekunden bzw. `usize`-Bytes mit kebab-case-Keys `request-timeout-seconds` und `max-body-bytes` — keine eigene Parser-Grammatik bauen (YAGNI).

- [ ] **Step 1: Failing Tests**

Neue Datei `crates/oxide-web/tests/limits.rs`. Sie braucht einen Controller mit einem langsamen Handler und einem POST-Endpunkt; das Muster (Controller im Test registrieren, App booten) steht in `crates/oxide-web/tests/server.rs`.

```rust
#[tokio::test(flavor = "multi_thread")]
async fn slow_handler_hits_the_request_timeout() {
    // OXIDE_SERVER_REQUEST_TIMEOUT_SECONDS=1, Handler schläft 3s
    let resp = reqwest::Client::new().get(format!("http://{addr}/slow")).send().await.unwrap();
    assert_eq!(resp.status(), 503);
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_body_is_rejected() {
    // OXIDE_SERVER_MAX_BODY_BYTES=1024
    let big = "x".repeat(4096);
    let resp = reqwest::Client::new().post(format!("http://{addr}/echo")).body(big).send().await.unwrap();
    assert_eq!(resp.status(), 413);
}
```

- [ ] **Step 2: Fehlschlag bestätigen**

Run: `cargo test -p oxide-web --test limits 2>&1 | tail -20`
Expected: 200 statt 503 bzw. 413.

- [ ] **Step 3: Implementieren**

Workspace-`Cargo.toml`: `tower-http = { version = "0.7", features = ["timeout", "limit", "cors"] }`; in `crates/oxide-web/Cargo.toml` als `tower-http.workspace = true`.

`RouterLayer.apply` auf `fn(&Context, Router) -> Router` umstellen und alle Aufrufstellen anpassen (`crates/oxide-observability/src/http.rs` ist die einzige externe). In `oxide-web` einen eigenen `RouterLayer` registrieren, der aus dem Context die `ServerProps` zieht und `TimeoutLayer`, `RequestBodyLimitLayer` und — nur wenn `cors.allowed-origins` nicht leer ist — `CorsLayer` anhängt. Der Management-Router bekommt dieselben Limits aus `ManagementProps`.

- [ ] **Step 4: Tests grün**

Run: `cargo test -p oxide-web -p oxide-observability 2>&1 | grep -E '^test result'`

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates
git commit -m "feat(web): request timeout, body limit and optional CORS as configured layers

RouterLayer now receives the Context, so layers can be driven by config.
Adds oxide.server.request-timeout-seconds (30), max-body-bytes (2 MiB) and
cors.allowed-origins (off unless set), applied to both servers.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 3b: Env-Overrides für kebab-case-Properties (eingeschoben 2026-09-03)

Nachträglich eingeschoben, weil der Review von Task 3 einen Framework-Fehler belegt hat: eine
mehrteilige `OXIDE_*`-Variable bindet nur dann an einen kebab-case-Schlüssel, wenn dieser bereits
in der YAML steht — sonst entsteht stillschweigend eine tote verschachtelte Struktur. Das betrifft
alle in dieser Phase eingeführten Properties und das bestehende `shutdown-grace-seconds`.

Lösung: Die Phase-1-Bindungsregel bleibt unangetastet; stattdessen werden die Defaults aller
`#[config(prefix = "...", default)]`-Structs vor den Env-Overrides in den Config-Baum eingesetzt,
sodass die Schlüssel existieren. Vollständiger Task-Text:
`.superpowers/sdd/2026-09-02-phase-4b-hardening/task-3b-brief.md`.

---

### Task 4: Health-Cache und `show-details`

**Files:**
- Modify: `crates/oxide-web/src/health.rs`, `crates/oxide-web/src/management.rs` (Props)
- Test: `crates/oxide-web/src/health.rs` (Unit), `crates/oxide-web/tests/health_details.rs` (neu)

**Interfaces:**
- Produces: `oxide.management.health.cache-ttl-millis` (Default `1000`), `oxide.management.health.show-details` (`"never"` | `"always"`, Default `"never"`). Der Aggregator cached das letzte Ergebnis samt Zeitstempel in einem `Mutex<Option<(Instant, Value)>>` bzw. `tokio::sync::Mutex`.

- [ ] **Step 1: Failing Tests**

Unit-Test in `health.rs`, der zählt, wie oft ein Indicator läuft:

```rust
#[tokio::test]
async fn indicators_run_once_within_the_cache_ttl() {
    static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    // Aggregator zweimal hintereinander mit TTL 1000ms aufrufen
    assert_eq!(CALLS.load(std::sync::atomic::Ordering::SeqCst), 1);
}
```

Integrationstest `crates/oxide-web/tests/health_details.rs`: ein Indicator, der ein `detail` liefert.

```rust
#[tokio::test(flavor = "multi_thread")]
async fn details_are_hidden_by_default() {
    let body: serde_json::Value = /* GET /actuator/health */;
    assert_eq!(body["components"]["demo"]["status"], "UP");
    assert!(body["components"]["demo"].get("detail").is_none(), "detail leaked: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn details_are_shown_when_configured() {
    // OXIDE_MANAGEMENT_HEALTH_SHOW_DETAILS=always
    assert!(body["components"]["demo"]["detail"].is_string());
}
```

- [ ] **Step 2: Fehlschlag bestätigen**

Run: `cargo test -p oxide-web 2>&1 | tail -20`

- [ ] **Step 3: Implementieren**

`HealthProps` als geschachtelte Props in `ManagementProps` (`health.cache-ttl-millis`, `health.show-details`). `aggregated_health_body` nimmt die Props entgegen, prüft den Cache, lässt bei Miss alle Indicators laufen und schreibt das Ergebnis zurück. Beim Rendern wird `detail` nur bei `show-details: always` in das JSON aufgenommen.

- [ ] **Step 4: Tests grün + bestehende Tests nachziehen**

Alle Tests, die heute `detail` oder Kafka-/Mongo-Details prüfen (`crates/oxide-kafka/tests`, `crates/oxide-mongo/tests`, `examples/rest-api/tests/api.rs`), setzen `OXIDE_MANAGEMENT_HEALTH_SHOW_DETAILS=always`.

Run: `cargo test -p oxide-web 2>&1 | grep -E '^test result'`

- [ ] **Step 5: Commit**

```bash
git add crates examples
git commit -m "feat(web)!: cache health results and hide indicator details by default

/actuator/health no longer re-runs every indicator per request (cache-ttl-millis,
default 1000) and no longer echoes driver error text: show-details defaults to
\"never\" like Spring, so Mongo/Kafka messages with hosts stay internal.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 5: Readiness-Karenz und ehrlicher `stop()`

**Files:**
- Modify: `crates/oxide-web/src/server.rs`, `crates/oxide-web/src/serve.rs`
- Test: `crates/oxide-web/tests/shutdown_grace.rs` (neu)

**Interfaces:**
- Produces: `oxide.server.shutdown.readiness-grace-seconds` (Default `0`). `pre_destroy` setzt Readiness auf DOWN, schläft die Karenz, drainiert dann. Läuft der Drain in den Abbruch (`handle.abort()`), gibt `stop()` einen `WebError` zurück statt `Ok`.

- [ ] **Step 1: Failing Test**

```rust
#[tokio::test(flavor = "multi_thread")]
async fn readiness_is_down_before_the_drain_starts() {
    // readiness-grace-seconds = 2; Stop in einer Task starten,
    // währenddessen /actuator/health/readiness abfragen.
    // Erwartung: 503 auf readiness, während der Hauptport noch annimmt.
}

#[tokio::test(flavor = "multi_thread")]
async fn aborted_drain_surfaces_an_error() {
    // shutdown-grace-seconds = 1, ein Handler hängt 5s → stop() ist Err
    assert!(app.stop().await.is_err());
}
```

- [ ] **Step 2–4:** Fehlschlag bestätigen, implementieren, Tests grün (`cargo test -p oxide-web`).

- [ ] **Step 5: Commit**

```bash
git commit -am "fix(web): readiness grace before draining, error on aborted drain

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 6: Mongo — `_like` escapen, `_is_null` korrigieren

**Files:**
- Modify: `crates/oxide-mongo-macros/src/repository.rs` (`parse_field_and_op`, Codegen), `crates/oxide-mongo/src/config.rs` (`derived_filter`), `crates/oxide-mongo/Cargo.toml` (+`regex`)
- Test: `crates/oxide-mongo/src/config.rs` (Unit), `crates/oxide-mongo/tests/repository.rs` (Integration)

**Interfaces:**
- Produces: `derived_filter` erhält für `$regex` den bereits escapten Wert; `_is_null`/`_is_not_null` erzeugen Filter **ohne** Argument. Die Operator-Tabelle bildet `_like` auf `"$regex"`, `_is_null` auf `"$null"` und `_is_not_null` auf `"$notnull"` ab; die beiden letzten sind Marker, die `derived_filter` ohne Wert behandelt.

- [ ] **Step 1: Failing Unit-Tests** in `crates/oxide-mongo/src/config.rs`:

```rust
#[test]
fn like_escapes_regex_metacharacters() {
    let d = derived_filter(&[("name", "$regex")], vec![Bson::String("a.*b".into())]);
    let sub = d.get_document("name").unwrap();
    assert_eq!(sub.get_str("$regex").unwrap(), regex::escape("a.*b"));
}

#[test]
fn is_null_needs_no_argument_and_matches_null() {
    let d = derived_filter(&[("deleted_at", "$null")], vec![]);
    assert_eq!(d.get("deleted_at"), Some(&Bson::Null));
}

#[test]
fn is_not_null_matches_ne_null() {
    let d = derived_filter(&[("deleted_at", "$notnull")], vec![]);
    assert_eq!(d.get_document("deleted_at").unwrap().get("$ne"), Some(&Bson::Null));
}
```

- [ ] **Step 2: Fehlschlag bestätigen** — `cargo test -p oxide-mongo --lib`.

- [ ] **Step 3: Implementieren.** In `parse_field_and_op` die drei Einträge korrigieren. Im Codegen dürfen Marker-Operatoren keinen Argumentwert konsumieren; die Zuordnung Argument→Bedingung verschiebt sich entsprechend. In `derived_filter` die drei Fälle behandeln.

- [ ] **Step 4: Integrationstest** in `crates/oxide-mongo/tests/repository.rs` um eine `find_all_by_name_like`-Methode und einen Datensatz mit `a.*b` ergänzen; prüfen, dass ein Dokument `axxb` **nicht** gefunden wird.

Run: `cargo test -p oxide-mongo 2>&1 | grep -E '^test result'`

- [ ] **Step 5: Commit**

```bash
git commit -am "fix(mongo)!: escape _like input, correct _is_null/_is_not_null semantics

_like built a \$regex from raw user input (regex injection / ReDoS) and now
escapes it; _is_null produced \$exists: <arg> and _is_not_null \$ne: <arg>,
which was simply wrong — both now take no argument.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 7: `raw_filter` in einem Durchlauf

**Files:**
- Modify: `crates/oxide-mongo/src/config.rs` (`raw_filter`), `crates/oxide-mongo/src/lib.rs` (Fehlervariante)
- Test: `crates/oxide-mongo/src/config.rs`

- [ ] **Step 1: Failing Tests**

```rust
#[test]
fn placeholder_ten_is_not_matched_by_placeholder_one() {
    let values: Vec<Bson> = (1..=10).map(|i| Bson::Int32(i)).collect();
    let d = raw_filter(r#"{ "a": ?1, "b": ?10 }"#, values).unwrap();
    assert_eq!(d.get("a"), Some(&Bson::Int32(1)));
    assert_eq!(d.get("b"), Some(&Bson::Int32(10)));
}

#[test]
fn substituted_values_are_not_substituted_again() {
    let d = raw_filter(r#"{ "a": ?1, "b": ?2 }"#, vec![Bson::String("?2".into()), Bson::String("x".into())]).unwrap();
    assert_eq!(d.get_str("a").unwrap(), "?2");
    assert_eq!(d.get_str("b").unwrap(), "x");
}

#[test]
fn missing_argument_for_placeholder_is_an_error() {
    assert!(raw_filter(r#"{ "a": ?2 }"#, vec![Bson::Int32(1)]).is_err());
}
```

- [ ] **Step 2: Fehlschlag bestätigen** — `cargo test -p oxide-mongo --lib`. Erwartung: Test 1 und 2 schlagen fehl.

- [ ] **Step 3: Implementieren.** Template zeichenweise scannen: bei `?` die folgenden Ziffern vollständig lesen, Index auflösen, gerenderten Wert in einen Ausgabe-String schreiben, ansonsten Zeichen übernehmen. Fehlender Index → neue Variante `Error::Placeholder { index: usize }`.

- [ ] **Step 4: Tests grün** — `cargo test -p oxide-mongo --lib`.

- [ ] **Step 5: Commit**

```bash
git commit -am "fix(mongo): single-pass placeholder substitution in raw_filter

?1 also matched inside ?10, and an already-substituted string value that
happened to contain ?2 was substituted again.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 8: Keine Secrets in Debug und Fehlern

**Files:**
- Modify: `crates/oxide-mongo/src/config.rs` (`MongoProps`, Verbindungsfehler), `crates/oxide-mongo/src/health.rs`, `crates/oxide-kafka/src/health.rs`
- Test: `crates/oxide-mongo/src/config.rs`

- [ ] **Step 1: Failing Test**

```rust
#[test]
fn debug_output_redacts_the_uri() {
    let props = MongoProps { uri: "mongodb://user:secret@host:27017".into(), ..Default::default() };
    let rendered = format!("{props:?}");
    assert!(!rendered.contains("secret"), "credentials leaked: {rendered}");
    assert!(rendered.contains("<redacted>"));
}
```

- [ ] **Step 2: Fehlschlag bestätigen** (`derive(Debug)` gibt die URI aus).

- [ ] **Step 3: Implementieren.** `derive(Debug)` durch ein handgeschriebenes `impl std::fmt::Debug` ersetzen, das `uri` als `"<redacted>"` ausgibt. Verbindungsfehler nicht mehr mit dem rohen Treibertext formatieren, sondern generisch („failed to connect to MongoDB“) plus `source`. Health-Indicators geben nur noch eine generische Meldung als `detail`; die Details sind ohnehin per Default aus (Task 4).

- [ ] **Step 4: Tests grün** — `cargo test -p oxide-mongo --lib -p oxide-kafka --lib`.

- [ ] **Step 5: Commit**

```bash
git commit -am "fix(mongo,kafka): keep credentials and driver internals out of logs and health

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 9: HTTP-Client — Encoding, Timeouts, Metrik-Label

**Files:**
- Modify: `crates/oxide-client/src/lib.rs` (`send`, `resolve_base_url`, Props), `crates/oxide-client-macros/src/http_client.rs` (Codegen), `crates/oxide-client/Cargo.toml` (+`percent-encoding`)
- Test: `crates/oxide-client/src/lib.rs` (Unit), `examples/http-client/tests/call_rest_api.rs` (Integration)

**Interfaces:**
- Produces: `oxide.clients.<name>.connect-timeout-seconds` (Default `5`), `.request-timeout-seconds` (Default `30`). Das Metrik-Label `uri` trägt das Routen-Template. `resolve_base_url` liefert `Result<String, ClientError>`.

- [ ] **Step 1: Failing Tests**

```rust
#[test]
fn path_segments_are_percent_encoded() {
    assert_eq!(encode_path_segment("a/../b"), "a%2F..%2Fb");
    assert_eq!(encode_path_segment("x?y=1"), "x%3Fy%3D1");
}

#[test]
fn missing_base_url_key_is_an_error_not_a_panic() {
    let config = /* leere AppConfig */;
    assert!(resolve_base_url("${oxide.clients.nope.url}", &config).is_err());
}
```

Integration in `examples/http-client/tests/call_rest_api.rs`: ein Aufruf mit einem Path-Parameter, der `/` enthält, darf die Route nicht verlassen (erwarteter Status 404 vom Zielserver, nicht 200 einer anderen Route).

- [ ] **Step 2: Fehlschlag bestätigen** — `cargo test -p oxide-client -p http-client`.

- [ ] **Step 3: Implementieren.** `percent-encoding` mit einem `AsciiSet` für Pfadsegmente; der Codegen ruft die Encode-Funktion für jeden `Path<T>`-Wert auf (`format!("{}", …)` → encodiert). `reqwest::Client::builder().connect_timeout(..).timeout(..)` aus den Props des jeweiligen Clients. Im `send` das Template-Label statt `uri.path()` verwenden — das Template ist im Makro bekannt und wird als weiteres Argument übergeben (der bisher ungenutzte Parameter `_method_name` entfällt zugunsten von `route_template`).

- [ ] **Step 4: Tests grün** — `cargo test -p oxide-client -p http-client 2>&1 | grep -E '^test result'`.

- [ ] **Step 5: Commit**

```bash
git commit -am "fix(client): percent-encode path params, apply timeouts, bound metric cardinality

Path values could contain ../ or ?x= and change the route; the reqwest client
had no timeouts at all; the uri metric label carried concrete paths.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 10: README für Block A und Blockabnahme

**Files:**
- Modify: `README.md`

- [ ] **Step 1: README ergänzen.** Im Web-Abschnitt: `address`, `endpoints.exposure` (mit dem Hinweis, dass Prometheus jetzt Opt-in ist), `request-timeout-seconds`, `max-body-bytes`, `cors.allowed-origins`, `health.show-details`, `health.cache-ttl-millis`, `shutdown.readiness-grace-seconds`. Im Client-Abschnitt die Timeout-Properties (und die Zeile über die 4c-Backlog-Props korrigieren: Retry bleibt offen, Timeouts nicht). Im Mongo-Abschnitt die korrigierte Grammatik für `_like`, `_is_null`, `_is_not_null`. Neuer Abschnitt „Environment overrides“ um den Hinweis erweitern, dass eine `OXIDE_*`-Variable neue Schlüssel anlegen und strukturierte YAML-Werte einspeisen kann.

- [ ] **Step 2: Blockabnahme**

Run: `make ci 2>&1 | tail -5`
Expected: Exit 0, Tests ≥ 92 + die in Block A ergänzten.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: document the new web, client and mongo configuration surface

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

# Block B — Robustheit (Tasks 11–16)

### Task 11: Panics zu Fehlern

**Files:**
- Modify: `crates/oxide-kafka/src/listener.rs` (Zeilen um 168, 249, 429, 435), `crates/oxide-kafka/src/producer.rs` (39, 128), `crates/oxide-mongo/src/config.rs` (80, 87), `crates/oxide-web/src/management.rs` (90, 154, 162), `crates/oxide-web/src/server.rs` (88, 93), `crates/oxide-observability/src/otel.rs` (184), `crates/oxide-client/src/lib.rs` (Header-`expect`), `crates/oxide-boot/src/app.rs` (149, 69), `crates/oxide-core/src/container.rs` (`insert`)
- Test: `crates/oxide-boot/tests/boot_failure.rs` (erweitern)

**Interfaces:**
- Produces: keine API-Änderung außer `ContextBuilder::insert`, das künftig `Result<(), ContextError>` liefert, wenn dieselbe `TypeId` zweimal eingefügt wird. **Vorher prüfen:** `crates/oxide-core/src/container.rs` hat bereits einen Test `duplicate_registration_is_an_error` — falls die Duplikatserkennung schon im `assemble`-Pfad greift, genügt eine Dokumentationszeile an `insert`, und die Signatur bleibt. Diese Entscheidung im Report festhalten.

- [ ] **Step 1: Bestandsaufnahme.** `grep -rn 'expect(\|unwrap()' crates --include='*.rs' | grep -v '/tests/' | grep -v '#\[cfg(test)\]'` und die Liste im Report festhalten. Jede Fundstelle bekommt eine Entscheidung: zu `Result` machen, oder mit einem Kommentar begründen, warum sie unerreichbar ist.

- [ ] **Step 2: Failing Test** — ein Kafka-Listener mit unerreichbarem Broker muss einen Boot-**Fehler** liefern statt zu panicken:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn unreachable_broker_fails_boot_instead_of_panicking() {
    // OXIDE_KAFKA_BOOTSTRAP_SERVERS auf einen freien Port
    let result = OxideApplication::builder().base_dir(env!("CARGO_MANIFEST_DIR")).env(env).start().await;
    assert!(result.is_err());
}
```

- [ ] **Step 3–4:** Implementieren und Tests grün. `std::env::vars()` in `app.rs:69` durch `vars_os()` mit Lossy-Konvertierung ersetzen (R9).

- [ ] **Step 5: Commit**

```bash
git commit -am "fix: surface boot and runtime failures as errors instead of panicking

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 12: Kafka-Shutdown wartet auf Worker

**Files:**
- Modify: `crates/oxide-kafka/src/listener.rs` (`supervised_loop`, `KafkaListenerManager`), `crates/oxide-kafka/src/config.rs` (`ConsumerProps`)
- Test: `crates/oxide-kafka/tests/shutdown.rs` (neu)

**Interfaces:**
- Produces: `oxide.kafka.consumer.shutdown-timeout-seconds` (Default `30`). Der Supervisor hält die `JoinHandle`s aller Partition-Worker; `pre_destroy` schließt die Sender, `join`t mit Timeout und committet erst danach.

- [ ] **Step 1: Failing Test** (nutzt das Redpanda-Harness der Crate):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_waits_for_in_flight_handler_and_commits_its_offset() {
    // Handler schläft 2s und setzt danach ein AtomicBool.
    // Produce 1 Nachricht, warte bis der Handler begonnen hat, dann app.stop().
    assert!(FINISHED.load(Ordering::SeqCst), "handler was cut off");
    // Danach: Consumer derselben Gruppe bekommt die Nachricht NICHT erneut.
    assert_eq!(redelivered, 0);
}
```

- [ ] **Step 2: Fehlschlag bestätigen** — der Handler wird heute abgeschnitten.

- [ ] **Step 3: Implementieren.** `subscribe`-Fehler bricht den Boot ab (R4). Worker-Handles in einer `Vec<JoinHandle<()>>` sammeln, `pre_destroy` awaitet sie mit `tokio::time::timeout`.

- [ ] **Step 4: Tests grün** — `cargo test -p oxide-kafka 2>&1 | grep -E '^test result'`.

- [ ] **Step 5: Commit**

```bash
git commit -am "fix(kafka): await in-flight workers and commit their offsets before shutdown

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 13: Rebalance-Handling

**Files:**
- Modify: `crates/oxide-kafka/src/listener.rs` (eigener `ConsumerContext`)
- Test: `crates/oxide-kafka/tests/rebalance.rs` (neu)

- [ ] **Step 1: Failing Test.** Zwei Consumer derselben Gruppe auf einem Topic mit zwei Partitionen; der zweite tritt bei, danach darf der erste für die entzogene Partition keine Offsets mehr committen und keine gepufferten Records mehr verarbeiten.

- [ ] **Step 2–4:** Fehlschlag bestätigen, `ConsumerContext` mit `pre_rebalance`/`post_rebalance` implementieren (Worker der entzogenen Partitionen beenden und deren Offsets committen; für neue Partitionen Worker starten), Tests grün.

- [ ] **Step 5: Commit**

```bash
git commit -am "fix(kafka): handle rebalances instead of running per-partition workers forever

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 13b: Dokumentierte kebab-case-Keys binden wirklich (eingeschoben 2026-09-03)

Aus dem Review von Task 12 hervorgegangen und vom Controller verifiziert: sechs im README
dokumentierte Properties tragen keinen serde-Rename und binden deshalb nie —
`max-attempts`, `backoff-initial-ms`, `backoff-multiplier`, `dlt-enabled`, `auto-offset-reset`
und `ping-on-start`. Gesetzte Werte werden still verworfen. Behoben mit
`#[serde(rename_all = "kebab-case")]` auf den betroffenen Structs plus je einem Test, der den
dokumentierten Schlüssel mit einem vom Default abweichenden Wert bindet. Vollständiger Task-Text:
`.superpowers/sdd/2026-09-02-phase-4b-hardening/task-13b-brief.md`.

---

### Task 14: Poison Pill und Backoff

**Files:**
- Modify: `crates/oxide-kafka-macros/src/listeners.rs` (Deserialisierungspfad), `crates/oxide-kafka/src/retry.rs`, `crates/oxide-kafka/src/config.rs`
- Test: `crates/oxide-kafka/src/retry.rs` (Unit), `crates/oxide-kafka/tests/retry_dlt.rs` (erweitern)

**Interfaces:**
- Produces: `oxide.kafka.consumer.retry.max-backoff-millis` (Default `30000`). Backoff bekommt Cap und Jitter (±20 %). Deserialisierungsfehler gehen ohne Retry in die DLT.

- [ ] **Step 1: Failing Tests**

```rust
#[test]
fn backoff_is_capped_and_jittered() {
    let props = RetryProps { max_attempts: 10, backoff_initial_ms: 1000, backoff_multiplier: 3.0, max_backoff_ms: 5000, ..Default::default() };
    for d in backoff_sequence(&props) {
        assert!(d <= Duration::from_millis(6000), "cap exceeded: {d:?}");
    }
}
```
In `retry_dlt.rs`: eine Nachricht mit kaputtem JSON produzieren und prüfen, dass der Handler **nicht** dreimal läuft und der Record sofort in `<topic>.DLT` liegt.

- [ ] **Step 2–4:** Fehlschlag bestätigen, implementieren, Tests grün.

- [ ] **Step 5: Commit**

```bash
git commit -am "fix(kafka): poison pills go straight to the DLT; backoff gets a cap and jitter

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 15: Producer-Konfiguration und Health-Consumer

**Files:**
- Modify: `crates/oxide-kafka/src/producer.rs`, `crates/oxide-kafka/src/config.rs` (`ProducerProps`), `crates/oxide-kafka/src/health.rs`
- Test: `crates/oxide-kafka/src/config.rs` (Unit), `crates/oxide-kafka/tests/producer.rs`

**Interfaces:**
- Produces: `oxide.kafka.producer.send-timeout-seconds` (Default `10`), `.flush-timeout-seconds` (Default `10`). `message.send.max.retries` wird nur bei `idempotence: true` hochgesetzt.

- [ ] **Step 1: Failing Unit-Test**

```rust
#[test]
fn non_idempotent_producer_does_not_set_unbounded_retries() {
    let props = KafkaProps { producer: ProducerProps { idempotence: false, ..Default::default() }, ..demo() };
    let cfg = props.client_config(&[]);
    assert!(cfg.get("message.send.max.retries").is_none());
}
```

- [ ] **Step 2–4:** Fehlschlag bestätigen, implementieren (inkl. Health-Consumer einmal bauen und wiederverwenden), Tests grün.

- [ ] **Step 5: Commit**

```bash
git commit -am "fix(kafka): bound producer retries to idempotent mode, configurable timeouts, cached health consumer

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 16: OTel-Shutdown mit Timeout und Blockabnahme

**Files:**
- Modify: `crates/oxide-observability/src/otel.rs` (`flush`)
- Test: `crates/oxide-observability/tests/otel.rs`

- [ ] **Step 1–4:** Test, der einen hängenden Exporter simuliert und prüft, dass `stop()` innerhalb von `oxide.otel.shutdown-timeout-seconds` (Default `10`) zurückkehrt; implementieren mit `tokio::time::timeout` um das `spawn_blocking`.

- [ ] **Step 5: Blockabnahme** — `make ci 2>&1 | tail -5`, Exit 0.

- [ ] **Step 6: Commit**

```bash
git commit -am "fix(observability): bound the OTel shutdown flush with a timeout

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

# Block C — Konsistenz, Struktur, Upgrades (Tasks 17–23)

### Task 17: Ein Registrierungsmuster

**Files:**
- Modify: `crates/oxide-macros/src/component.rs` (Argument `destroy_order`), `crates/oxide-web/src/server.rs`, `crates/oxide-web/src/management.rs`, `crates/oxide-observability/src/otel.rs`
- Test: `crates/oxide-boot/tests/ui/` (trybuild), `crates/oxide-boot/tests/stop_ordering.rs`

**Interfaces:**
- Produces: `#[component(destroy_order = -100)]` erzeugt einen Descriptor mit gesetztem `destroy_order`. Danach werden `WebServer`, `ManagementServer` und `OtelLifecycle` über das Makro registriert; die handgeschriebenen `inventory::submit!`-Blöcke entfallen.

- [ ] **Step 1: Failing Test.** `stop_ordering.rs` prüft heute die Reihenfolge über die handgeschriebenen Descriptors; ein neuer Test bootet eine App mit zwei `#[component(destroy_order = …)]`-Komponenten und prüft die Stop-Reihenfolge.

- [ ] **Step 2–4:** Fehlschlag bestätigen, `parse_args` um `destroy_order` erweitern, Migration der drei Komponenten, Tests grün. Dabei die doppelten `default_port()`-Funktionen und die dreifache `oxide.application.name`-Auflösung in eine gemeinsame Hilfsfunktion ziehen (F10).

- [ ] **Step 5: Commit**

```bash
git commit -am "refactor(macros,web,observability): one component registration pattern

#[component] learns destroy_order, so WebServer, ManagementServer and the OTel
lifecycle no longer need hand-written inventory::submit! blocks.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 18: Container-Fixtures in oxide-test

**Files:**
- Create: `crates/oxide-test/src/containers.rs`
- Modify: `crates/oxide-test/Cargo.toml` (Feature `containers`), `crates/oxide-kafka/tests/harness/mod.rs`, `crates/oxide-mongo/tests/repository.rs`, `examples/kafka/tests/kafka_flow.rs`, `examples/rest-api/tests/api.rs`, `crates/oxide-web/src/health.rs` (Helper), `crates/oxide-kafka/src/health.rs`, `crates/oxide-mongo/src/health.rs`

**Interfaces:**
- Produces: `oxide_test::containers::redpanda() -> impl Future<Output = ContainerHandle>` und `::mongo()`, beide idempotent pro Prozess, mit Wait-Strategie und Aufräumen **ohne** `unsafe libc::atexit` (testcontainers 0.28 räumt beim Drop auf; der Handle wird in einem `OnceCell` gehalten und beim Prozessende über `Drop` bzw. den Ryuk-Reaper entsorgt). Zusätzlich `oxide_web::health::simple_indicator(name, check)` für die Indicator-Boilerplate.

- [ ] **Step 1:** Fixtures schreiben, dabei die Wait-Logik aus `crates/oxide-kafka/tests/harness/mod.rs` übernehmen (sie ist die vollständigste).
- [ ] **Step 2:** Die vier Testdateien auf die Fixtures umstellen; alle `unsafe`-Blöcke und die `libc`-dev-dependencies entfernen.
- [ ] **Step 3:** `simple_indicator` einführen und die beiden Health-Module darauf umstellen.
- [ ] **Step 4:** `cargo test -p oxide-kafka -p oxide-mongo -p kafka -p rest-api 2>&1 | grep -E '^test result'` — alle grün, `grep -rn 'libc::atexit' crates examples` liefert nichts.
- [ ] **Step 5: Commit**

```bash
git commit -am "refactor(test): shared container fixtures, drop the three copied harnesses

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 19: `listener.rs` aufteilen

**Files:**
- Create: `crates/oxide-kafka/src/listener/descriptor.rs`, `worker.rs`, `manager.rs`
- Modify: `crates/oxide-kafka/src/listener.rs` → `crates/oxide-kafka/src/listener/mod.rs` (per `git mv`)

- [ ] **Step 1:** `git mv crates/oxide-kafka/src/listener.rs crates/oxide-kafka/src/listener/mod.rs`, dann die drei Bereiche herausziehen: Typen/Descriptor, Worker/Retry-Pfad, Manager/Lifecycle. `RouteMessage` entfällt zugunsten von `ReceivedMessage`; das leere `ListenerInfra` wird aus der öffentlichen API entfernt (Aufrufstellen im Makro anpassen).
- [ ] **Step 2:** Reine Umstrukturierung — keine Verhaltensänderung. Nachweis: `cargo test -p oxide-kafka` unverändert grün, Testzahl gleich.
- [ ] **Step 3: Commit**

```bash
git commit -am "refactor(kafka): split listener into descriptor, worker and manager modules

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 20: Mongo-Repositories als Components

**Files:**
- Modify: `crates/oxide-mongo-macros/src/repository.rs`, `crates/oxide-mongo/src/config.rs` (`save`)
- Test: `crates/oxide-mongo/tests/repository.rs`

**Interfaces:**
- Produces: `#[mongo_repository]` registriert `…Impl` per `inventory::submit!` als Component (Muster wie `#[http_client]`), sodass es injizierbar ist. `save` wird zum Upsert über den `#[id]`-Wert. `find_by_<feld>` liefert `Option<T>`, wenn die Signatur `Option<T>` sagt.

- [ ] **Step 1–4:** Test, der das Repository aus dem Context zieht statt es von Hand zu bauen; Upsert-Test (zweimal `save` mit derselben Id → ein Dokument); `find_by_email` mit `Option`-Rückgabe. Implementieren, Tests grün.
- [ ] **Step 5: Commit**

```bash
git commit -am "feat(mongo): repositories are injectable components; save upserts; find_by returns Option

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 21: Modulgrenzen und Kleinkram

**Files:**
- Modify: `crates/oxide-core/src/config.rs` (Walker), `crates/oxide-observability/Cargo.toml` + `src/actuator.rs` + `src/http.rs`, `crates/oxide-boot/src/lib.rs` (`__private`), `crates/oxide-kafka/Cargo.toml`, alle `crates/*/Cargo.toml` (`publish = false`)

- [ ] **Step 1:** `set_relaxed`/`set_existing` zu einem Walker mit Modus-Flag zusammenführen (F7); die vorhandenen Config-Tests müssen unverändert grün bleiben.
- [ ] **Step 2:** `oxide-observability` bekommt eine eigene `axum`-Dependency und nutzt sie statt `oxide_web::__private::axum` (F8); `oxide-boot::__private` re-exportiert nur die tatsächlich benötigten Typen statt `oxide_core::*`.
- [ ] **Step 3:** `opentelemetry_sdk` mit Feature `testing` in `oxide-kafka` zur dev-dependency machen (F9).
- [ ] **Step 4:** `publish = false` in allen Bibliotheks-Crates ergänzen (F12) und die 14 `version = "0.1.0"`-Pins im Workspace-`Cargo.toml` entfernen, da `allow-wildcard-paths` jetzt greift. **Nachweis:** `cargo deny check bans 2>&1 | tail -2` muss `bans ok` zeigen; wenn nicht, Pins behalten und den Grund im Report festhalten.
- [ ] **Step 5:** `cargo test --workspace 2>&1 | grep -E '^test result'`, dann Commit:

```bash
git commit -am "refactor: tighten module boundaries, dedupe the env walker, mark crates unpublished

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 22: Major-Upgrades

**Files:**
- Modify: `Cargo.toml` (workspace deps), `Cargo.lock`, `crates/oxide-observability/src/otel.rs`, ggf. `crates/oxide-kafka/src/observe.rs`, `crates/oxide-client/src/lib.rs`, `rust-toolchain.toml`, `Makefile`

**Interfaces:**
- Produces: opentelemetry/opentelemetry_sdk/opentelemetry-otlp `0.32`, tracing-opentelemetry `0.33`, metrics-exporter-prometheus `0.18`, rdkafka `0.39`, reqwest `0.13`, tower-http `0.7`. `rust-toolchain.toml` pinnt einen expliziten Channel; `make msrv` prüft `cargo +1.88 check --workspace`.

- [ ] **Step 1:** Ein Crate nach dem anderen anheben, jeweils `cargo check --workspace --all-targets` und die betroffene Testdatei laufen lassen. Reihenfolge: metrics-exporter-prometheus, tower-http, reqwest, rdkafka, dann der OTel-Block gemeinsam (opentelemetry + sdk + otlp + tracing-opentelemetry).
- [ ] **Step 2:** Nach dem OTel-Block: `cargo tree -d 2>/dev/null | grep -c '^tonic'` muss `0` ergeben (nur noch eine Major-Version im Graph).
- [ ] **Step 3:** `rust-toolchain.toml` auf den aktuellen expliziten Stable-Channel pinnen (`rustc --version` ablesen) und in `Makefile` ergänzen:
  ```make
  msrv:
  	cargo +1.88 check --workspace --all-targets
  ```
  plus `msrv` in `.PHONY`. Falls die Toolchain 1.88 lokal fehlt: `rustup toolchain install 1.88` gehört in die README-Anleitung, das Ziel darf trotzdem existieren.
- [ ] **Step 4:** `make ci 2>&1 | tail -5` (Exit 0) und `cargo audit 2>&1 | tail -3` (keine neue Advisory).
- [ ] **Step 5: Commit** (ein Commit pro Upgrade-Schritt ist erlaubt und erwünscht; mindestens einer für den OTel-Block):

```bash
git commit -am "build(deps)!: opentelemetry 0.32, tracing-opentelemetry 0.33 — collapses the duplicate tonic stack

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 23: Abnahme

**Files:** keine Änderungen außer README/Spec-Nachträgen.

- [ ] **Step 1:** `make ci` und `make msrv` — beide grün.
- [ ] **Step 2:** Nachweise aus Spec §7 einsammeln: `cargo tree -d | grep tonic` (eine Major), `grep -rn 'libc::atexit' crates examples` (leer), `cargo audit` (0 Vulnerabilities).
- [ ] **Step 3:** README auf Vollständigkeit prüfen: alle in Block A–C eingeführten Properties dokumentiert; der Status-Satz oben auf „Phase 4 (Härtung) abgeschlossen“ aktualisiert.
- [ ] **Step 4:** In der 4b-Spec einen Abschnitt „Umsetzungsnotizen“ ergänzen: welche Findings anders gelöst wurden als geplant, was nach 4c verschoben wurde.
- [ ] **Step 5: Commit**

```bash
git commit -am "docs: phase 4b completion notes and configuration reference

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```
