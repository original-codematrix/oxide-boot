# Phase 4b — Härten, robuster machen, konsolidieren

Datum: 2026-09-02
Status: freigegeben
Vorgänger: `2026-09-02-phase-4a-consolidation-design.md` (Anhang A = Fundstellenliste), `2026-07-31-oxide-boot-design.md` (Gesamtdesign)

## 1. Ausgangslage

Phase 4a ist auf `main` (1d1b910): alle Starter committet, MSRV 1.88, `Cargo.lock` getrackt,
`deny.toml`, `make ci` grün (fmt, clippy `-D warnings`, 92 Tests, 0 Vulnerabilities).

Offen sind die in Anhang A von 4a katalogisierten Findings: 10 Sicherheitsprobleme im eigenen
Code (S1–S10), 10 Robustheitsprobleme (R1–R10) und 13 Refactoring-/Konsistenzpunkte (F1–F13).
Diese Phase arbeitet alle drei Gruppen ab (Entscheidung 2026-09-02: „alle drei am Stück“).

## 2. Ziel

Nach dieser Phase gilt:

1. Kein extern erreichbarer Endpunkt ohne Timeout, Body-Limit und bewusste Freigabe.
2. Keine Injection- oder Leak-Pfade mehr in oxide-mongo und oxide-client.
3. Kafka verliert beim Shutdown und beim Rebalance keine Nachrichten mehr und blockiert nicht.
4. Genau ein Registrierungsmuster für Starter-Komponenten; Testcontainer-Fixtures leben in
   `oxide-test` statt dreifach kopiert in Testdateien.
5. Keine veralteten Major-Versionen mehr in der Kern-Abhängigkeitskette; nur noch ein
   tonic/prost-Stack im Graph.
6. `make ci` bleibt grün, `cargo audit`/`cargo deny` ohne Vulnerabilities.

## 3. Teil A — Angriffsfläche (S1–S9, R7)

### A.1 Management-Endpunkte (S1)

`ManagementProps` bekommt:

```yaml
oxide:
  management:
    port: 8081
    address: "0.0.0.0"          # Default unverändert
    endpoints:
      exposure: "health,info"   # Default; Liste oder "*"
```

**Entscheidung 2026-09-02:** Der Default der Bind-Adresse bleibt `0.0.0.0`. Kubelet-Probes
verbinden sich auf die Pod-IP, ein Loopback-Default würde Liveness/Readiness brechen. Der
Schutz kommt stattdessen aus `exposure`: nur freigegebene Endpunkte werden gemountet, alles
andere existiert nicht (404). `/actuator/prometheus` ist damit ab sofort **nicht mehr per
Default aktiv** — Scraping-Setups müssen `exposure: "health,info,prometheus"` setzen.
`ServerProps` bekommt analog `address` (Default `0.0.0.0`).

`ManagementRoute`-Registrierungen tragen künftig einen Endpoint-Namen, gegen den die Exposure-
Liste geprüft wird. `/actuator/health/liveness` und `/actuator/health/readiness` gehören zum
Endpoint `health`.

### A.2 Request-Pipeline (S2)

`RouterLayer` bekommt Zugriff auf den Context: die registrierte Funktion wird von
`fn(Router) -> Router` zu `fn(&Context, Router) -> Router`. Damit sind config-getriebene
Layer möglich. Eingebaut werden (auf beiden Servern, Werte aus `oxide.server` bzw.
`oxide.management`):

| Property | Default | Wirkung |
|---|---|---|
| `request-timeout` | `30s` | `tower_http::timeout::TimeoutLayer`, danach 503 |
| `max-body-size` | `2MiB` | `tower_http::limit::RequestBodyLimitLayer`, danach 413 |
| `cors.allowed-origins` | leer (aus) | `tower_http::cors::CorsLayer`, nur wenn gesetzt |

Neue Dependency: `tower-http` mit Features `["timeout", "limit", "cors"]`.

### A.3 Health-Endpunkt (S3, S4)

- Ergebnis wird für `oxide.management.health.cache-ttl` (Default `1s`) gecacht; parallele
  Anfragen im TTL teilen sich ein Ergebnis. Das entschärft den DoS-Verstärker.
- `oxide.management.health.show-details` mit Werten `never` (Default) und `always`, wie in
  Spring. Bei `never` enthält die Antwort nur `status` je Komponente, keine `detail`-Texte.
  Damit können Mongo-/Kafka-Treiberfehler (inkl. Hostnamen) nicht mehr nach außen gelangen.
  Bestehende Tests, die Details prüfen, setzen `always`.

### A.4 oxide-mongo (S5, S6, S7)

- **`_like`** escaped den Wert und sucht „enthält“: `{ field: { $regex: <escaped>, $options: "" } }`.
  Damit ist Regex-Injection/ReDoS ausgeschlossen. Neue Dependency: `regex` (nur für `escape`).
- **`_is_null` / `_is_not_null`** nehmen **kein Argument** mehr (Breaking Change) und erzeugen
  `{ field: null }` bzw. `{ field: { $ne: null } }`. Der bisherige Code erzeugte
  `$exists: <arg>` bzw. `$ne: <arg>` und war schlicht falsch.
- **`raw_filter`** ersetzt Platzhalter in **einem** Durchlauf: der Template-String wird
  gescannt, `?<ziffern>` als Token erkannt und direkt durch den gerenderten Wert ersetzt.
  Damit matcht `?1` nicht mehr in `?10`, und bereits eingesetzte Werte werden nicht erneut
  ersetzt. Ein Platzhalter ohne passendes Argument ist ein Fehler (`Error::Placeholder`).
- **`MongoProps`** bekommt ein handgeschriebenes `Debug`, das `uri` als `"<redacted>"`
  ausgibt. Verbindungsfehler werden nicht mehr mit dem rohen Treibertext propagiert.

### A.5 oxide-client (S8)

- Path-Parameter werden percent-encodiert (`percent-encoding`, Set `NON_ALPHANUMERIC` minus
  der in Pfadsegmenten erlaubten Zeichen), damit `../` oder `?x=` aus Nutzerdaten die Route
  nicht verändern.
- `reqwest::Client` wird mit Timeouts gebaut, pro Client konfigurierbar:
  `oxide.clients.<name>.connect-timeout` (Default `5s`), `.request-timeout` (Default `30s`).
  Retry bleibt Phase 4c.
- Das Metrik-Label `uri` trägt künftig das **Routen-Template** (zur Compile-Zeit bekannt),
  nicht den konkreten Pfad — sonst unbegrenzte Kardinalität.
- `resolve_base_url` panict nicht mehr, sondern liefert einen Boot-Fehler.

### A.6 Konfiguration (S9)

Das Env-Binding bleibt inhaltlich wie in Phase 1 festgenagelt (`OXIDE_A_B` → Fallback-
Stripping, YAML-Parsing des Werts). Env-Variablen sind vertrauenswürdiger Input; eine
Einschränkung würde legitime Fälle brechen. **Kein Code-Change**, aber ein expliziter
Abschnitt im README, der das Verhalten und seine Konsequenz (eine `OXIDE_*`-Variable kann
neue Schlüssel anlegen und strukturierte Werte einspeisen) dokumentiert.

### A.7 Shutdown-Karenz (R7)

`oxide.server.shutdown.readiness-grace` (Default `0s`, opt-in): Readiness geht auf DOWN,
dann wird die Karenzzeit abgewartet, erst danach beginnt der Drain. Damit können
Load-Balancer die Instanz aus der Rotation nehmen, bevor Verbindungen abgelehnt werden.
Bricht der Drain nach dem Grace-Timeout ab, liefert `stop()` künftig einen Fehler statt `Ok`.

## 4. Teil B — Kafka-Robustheit (R1–R6, S10)

| # | Änderung |
|---|---|
| R1 | Worker-`JoinHandle`s werden gehalten; `pre_destroy` schließt die Sender, wartet mit Timeout (`oxide.kafka.consumer.shutdown-timeout`, Default `30s`) auf alle Worker und committet danach die letzten Offsets, bevor der Consumer fällt |
| R2 | Eigener `ConsumerContext` mit `post_rebalance`: bei Revoke werden die Worker der entzogenen Partitionen beendet und ihre Offsets committet; danach werden für neu zugeteilte Partitionen Worker gestartet |
| R3 | Deserialisierungsfehler gehen **ohne** Retry direkt in die DLT (Poison Pill). Backoff bekommt Cap (`retry.max-backoff`, Default `30s`) und Jitter |
| R4 | `subscribe`-Fehler bricht den Boot ab statt `continue` |
| R5 | `message.send.max.retries` wird nur mit aktivierter Idempotenz hochgesetzt; Producer-Timeouts kommen aus `oxide.kafka.producer.*` statt hartkodierter 10 s |
| R6 | Der Health-Consumer wird einmal gebaut und wiederverwendet |
| S10 | Alle `expect`/`unwrap` in `listener.rs`, `producer.rs`, `mongo/config.rs`, `web/management.rs`, `web/server.rs`, `observability/otel.rs`, `client/lib.rs` werden zu Boot- bzw. Laufzeitfehlern |

Zusätzlich R8 (OTel-Shutdown mit Timeout), R9 (`env::vars_os`), R10 (`ContextBuilder::insert`
meldet doppelte `TypeId` als Fehler statt still zu überschreiben).

## 5. Teil C — Konsistenz, Struktur, Upgrades (F1–F13)

### C.1 Ein Registrierungsmuster (F1, F10)

`#[component]` lernt `destroy_order = N` und Hooks mit Context-Zugriff. Danach werden
`WebServer`, `ManagementServer` und der OTel-Lifecycle über das Makro registriert statt über
handgeschriebene `inventory::submit!`-Blöcke. Die doppelten `default_port()`-Funktionen und
die dreifache Auflösung von `oxide.application.name` werden zusammengeführt.

### C.2 Test-Fixtures (F2, F3)

`oxide-test` bekommt hinter dem Feature `containers` die Fixtures `redpanda()` und `mongo()`
(inkl. Wait-Strategie und Aufräumen ohne `unsafe libc::atexit`). Die drei kopierten Harnesse
in `crates/oxide-kafka/tests`, `crates/oxide-mongo/tests` und `examples/kafka/tests` sowie der
Container-Block in `examples/rest-api/tests` nutzen sie. Ein `oxide_web::health`-Helper
ersetzt die identische Indicator-Boilerplate in Kafka und Mongo.

### C.3 Struktur (F4, F6, F7, F8, F9)

- `listener.rs` (448 Zeilen) wird in `descriptor.rs`, `worker.rs` und `manager.rs` geteilt;
  `RouteMessage` (Duplikat von `ReceivedMessage`) und das leere `ListenerInfra` entfallen.
- Mongo-Repositories registrieren sich als Components; `save` wird zum Upsert; `find_by_<feld>`
  liefert `Option<T>`, wenn die Signatur es sagt, `find_all_by_…` bleibt `Vec<T>`.
- Die beiden fast identischen Env-Binding-Walker in `oxide-core/src/config.rs` werden zu einem
  Walker mit Modus-Flag.
- `oxide-observability` bekommt eine eigene `axum`-Dependency statt `oxide_web::__private::axum`;
  `oxide-boot::__private` re-exportiert nur noch die tatsächlich benötigten Core-Typen.
- `opentelemetry_sdk` mit Feature `testing` wird in `oxide-kafka` zur dev-dependency.

**F5 (dupliziertes `name_to_snake`)** wird bewusst nicht zusammengelegt: die beiden
Proc-Macro-Crates müssten dafür eine gemeinsame Hilfs-Crate bekommen, was für zwölf Zeilen
mehr Struktur als Nutzen wäre. Als akzeptierte Duplikation dokumentiert.

### C.4 Upgrades (F11, F13)

| Crate | von → nach |
|---|---|
| opentelemetry, opentelemetry_sdk, opentelemetry-otlp | 0.27 → 0.32 |
| tracing-opentelemetry | 0.28 → 0.33 |
| metrics-exporter-prometheus | 0.16 → 0.18 |
| rdkafka | 0.37 → 0.39 |
| reqwest | 0.12 → 0.13 |
| tower-http | neu, 0.7 |

Das OTel-Upgrade zieht tonic 0.14 und beseitigt damit den doppelten tonic/prost-Stack (F11).
`rust-toolchain.toml` pinnt einen expliziten Channel; `make msrv` prüft mit
`cargo +1.88 check --workspace`, dass die deklarierte MSRV stimmt (F13).

**F12 (fehlende `description`/`repository`):** Die Beispiele haben `publish = false`, die
Bibliotheks-Crates nicht. Da V1 ausdrücklich nicht auf crates.io veröffentlicht, bekommen alle
Crates `publish = false`; die Metadaten folgen, wenn eine Veröffentlichung ansteht.

## 6. Abgrenzung

- Keine neuen Features aus Anhang A.4 (Scheduler, Validierung, Trait-Injection, Pagination,
  `TestApp::replace`, `@Import`-Mechanismus) — das ist Phase 4c.
- Kein Client-Retry (nur Timeouts).
- Kein CI-Workflow, kein Remote, kein crates.io-Release.

## 7. Testen und Abnahme

Jede Änderung kommt mit einem Test, der ohne sie fehlschlägt. Insbesondere:

| Bereich | Nachweis |
|---|---|
| Exposure | `/actuator/prometheus` ist ohne Freigabe 404, mit Freigabe 200 |
| Timeout/Body-Limit | Handler, der länger als der Timeout braucht → 503; Body über Limit → 413 |
| Health-Cache | zwei Anfragen innerhalb des TTL lösen genau einen Indicator-Lauf aus |
| show-details | Default-Antwort enthält keinen `detail`-Schlüssel |
| `_like` | ein Wert mit `.*` findet nur den wörtlichen Treffer |
| `raw_filter` | ein Template mit `?1` und `?10` bindet korrekt; ein Wert, der `?2` enthält, bleibt unangetastet |
| MongoProps | `format!("{:?}")` enthält das Passwort nicht |
| Client-Pfad | ein Path-Parameter `a/../b` landet encodiert in der URL |
| Kafka-Shutdown | ein laufender Handler wird zu Ende geführt, sein Offset ist committet |
| Rebalance | nach Revoke werden für die entzogene Partition keine Offsets mehr committet |
| Poison Pill | eine nicht deserialisierbare Nachricht liegt nach genau einem Versuch in der DLT |
| Duplikat-Component | zwei Descriptors mit gleicher `TypeId` → Boot-Fehler |

Definition of Done: `make ci` grün, `make msrv` grün, `cargo tree -d | grep tonic` zeigt nur
noch eine Major-Version, README beschreibt Exposure, show-details, Timeouts, Body-Limit,
Client-Timeouts und das Env-Binding-Verhalten.

## 8. Risiken

| Risiko | Umgang |
|---|---|
| OTel 0.32 bricht die Tracer-/Exporter-API stärker als erwartet | Upgrade als eigener, letzter Task-Block; bei Blockade Rückfall auf 0.31 oder Aufteilung in eigene Phase |
| `exposure`-Default macht bestehende Prometheus-Scrapes still kaputt | Im README und in der Commit-Message prominent; das rest-api-Beispiel setzt die Freigabe explizit |
| Rebalance-Handling ist schwer testbar | Testfall über zwei Consumer derselben Gruppe, die sich die Partitionen teilen |
| Breaking Changes in den Mongo-Derived-Queries | Nur ein interner Nutzer (Testsuite); Grammatik im README korrigiert |
| Sehr großer Umfang in einer Phase | Plan ist in drei Blöcke geteilt, jeder Block endet grün und committet |

## 9. Abschluss (2026-09-03)

Phase 4b ist abgeschlossen. 37 Commits auf `phase-4b`, alle 24 Tasks umgesetzt und
reviewt. Die Definition of Done ist erfüllt:

| Kriterium | Ergebnis |
|---|---|
| `make ci` | grün — fmt, clippy `-D warnings`, 144 Tests in 73 Binaries, `cargo audit`, `cargo deny` (`advisories ok, bans ok, licenses ok, sources ok`) |
| `make msrv` | grün gegen 1.88 |
| Duplizierte tonic-Stacks | `cargo tree -d \| grep -c '^tonic'` → 0 |
| RUSTSEC-Advisories | 0 bei 415 gescannten Dependencies |
| README | Exposure, `show-details`, Timeouts, Body-Limit, Client-Timeouts und Env-Binding dokumentiert |

Über die Spec hinaus in dieser Phase gelandet:

- Toolchain-Pin 1.96.0 → **1.98.1**, `make ci` und `make msrv` darauf grün, keine
  neuen Clippy-Lints. Deklarierte MSRV bleibt 1.88.
- **Debug-Profile begrenzt**: ein voller Workspace-Testbuild hatte `target/` auf
  61 GB getrieben, davon ~42 GB Debug-Info in `debug/deps`. Dependencies bauen
  jetzt ohne Debug-Info, Workspace-Crates behalten Line-Tables. Dazu `make clean`.
- **`make loadtest`**: Release-Build des rest-api-Beispiels unter k6, gegen
  dieselbe Redpanda-Konfiguration wie die Testfixtures, mit Thresholds, die den
  Lauf scheitern lassen — damit als Regressions-Gate nutzbar, nicht nur als Report.
- Der Kommentar zu den Versions-Pins in `deny.toml` war sachlich falsch (er
  begründete sie mit publizierbaren Crates; alle Crates sind `publish = false`).
  Der wirkliche Grund ist, dass die `publish = false`-Ausnahme nur zusammen mit
  `allow-wildcard-paths` greift, das cargo-deny standardmäßig aus lässt.

Offen und nach Phase 4c verschoben: die Feature-Lücken aus Anhang A.4 und die
Spring-Parität aus A.5 (Scheduler, Validation, Trait-Injection, `TestApp::replace`,
`@Import`-Mechanismus, Pagination) sowie ein `oxide-redis`-Starter.
