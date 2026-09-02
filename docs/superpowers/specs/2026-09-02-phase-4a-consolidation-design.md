# Phase 4a — Konsolidieren und absichern

Datum: 2026-09-02
Status: freigegeben
Vorgänger: `2026-07-31-oxide-boot-design.md` (Gesamtdesign), Pläne Phase 1–3a

## 1. Ausgangslage

Stand am 2026-09-02 auf Branch `continue-next` (3 Commits vor `main`, kein Remote):

- Workspace kompiliert warnungsfrei (`cargo check`, `cargo clippy --all-targets`),
  `cargo test --workspace` mit Redpanda-/Mongo-Containern: 92 Tests bestanden.
- Die Phase-3-Starter `oxide-kafka`, `oxide-mongo`, `oxide-client`, `oxide-test` samt
  Makro-Crates, `examples/kafka`, `examples/http-client` und die Kafka-Erweiterung von
  `examples/rest-api` sind **komplett uncommitted** (untracked bzw. modified).
- `README.md` behauptet „Status: Phase 1“.
- `.claude/worktrees/phase-3a` (Branch `phase-3a-kafka`) ist inhaltlich identisch mit
  `continue-next` und veraltet.
- `.idea/` liegt untracked im Baum.
- `Cargo.lock` ist gitignored; CVE-Scans sind damit nicht reproduzierbar.
- Workspace-`rust-version = "1.85"`, aber `mongodb 3.8` verlangt bereits 1.88.
- Keine CI, kein `deny.toml`, kein Renovate/Dependabot.

### CVE-Scan (cargo-audit 0.22.2, cargo-deny 0.20.2, Advisory-DB Stand 2026-09-02)

| Advisory | Crate | Reichweite | Fix |
|---|---|---|---|
| RUSTSEC-2026-0258 | h2 0.4.15 (unbounded empty DATA frames) | produktiv (hyper/reqwest/tonic) | `cargo update` → 0.4.19 |
| RUSTSEC-2026-0009 | time 0.3.45 (Stack Exhaustion, CVSS 6.8) | produktiv (mongodb → bson) | ≥ 0.3.47, braucht Rust 1.88 |
| RUSTSEC-2025-0111 | tokio-tar 0.3.1 (PAX-Header, CVE-2025-62518) | nur dev (testcontainers 0.23) | testcontainers 0.28 |
| RUSTSEC-2025-0052 | async-std unmaintained | nur dev (testcontainers 0.23) | testcontainers 0.28 |
| RUSTSEC-2025-0134 | rustls-pemfile unmaintained | nur dev (bollard 0.18) | testcontainers 0.28 |

Veraltete Major-Versionen ohne bekannte CVE (bewusst **nicht** Teil dieser Phase, siehe §6):
opentelemetry 0.27 → 0.32, tracing-opentelemetry 0.28 → 0.33, tonic 0.12 → 0.14
(zieht heute ein doppeltes axum 0.7 in den Graph), metrics-exporter-prometheus 0.16 → 0.18,
reqwest 0.12 → 0.13, rdkafka 0.37 → 0.39, tower-http 0.6 → 0.7.

## 2. Ziel

Nach dieser Phase gilt:

1. Die gesamte Phase-3-Arbeit ist in nachvollziehbaren Commits in der Historie.
2. `cargo audit` und `cargo deny check` laufen fehlerfrei; alle bekannten Advisories sind
   geschlossen oder — falls unvermeidbar — mit Begründung in `deny.toml` ignoriert.
3. MSRV und Toolchain sind korrekt deklariert und reproduzierbar (`rust-toolchain.toml`,
   eingechecktes `Cargo.lock`).
4. Ein lokaler Einstiegspunkt (`make ci`) führt Format-Check, Clippy, Tests und den
   Security-Scan aus; das README beschreibt ihn.
5. Alle 92 Tests bleiben grün; keine Framework-Logik wird verändert.

## 3. Historie aufräumen

Reihenfolge der Commits auf `continue-next`, jeder Zwischenstand muss bauen
(`cargo check --workspace --all-targets`):

| # | Commit | Inhalt |
|---|---|---|
| 1 | `feat(kafka): …` | `crates/oxide-kafka`, `crates/oxide-kafka-macros`, `examples/kafka`, Kafka-Teile des `examples/rest-api`-Diffs (Cargo.toml, application.yml, lib.rs, tests/api.rs), Workspace-Einträge, README-Abschnitt „Kafka“ |
| 2 | `feat(mongo): …` | `crates/oxide-mongo`, `crates/oxide-mongo-macros`, Workspace-Einträge, README-Abschnitt „MongoDB“ |
| 3 | `feat(client): …` | `crates/oxide-client`, `crates/oxide-client-macros`, `examples/http-client`, Workspace-Einträge, README-Abschnitt „HTTP client“ |
| 4 | `feat(test): …` | `crates/oxide-test`, `crates/oxide-test-macros`, Workspace-Einträge, README-Abschnitt „Tests“ |
| 5 | `chore: …` | `.idea/` in `.gitignore`; README-Status auf „Phase 3 (Integrationen)“; Worktree `.claude/worktrees/phase-3a` entfernen (`git worktree remove`), Branch `phase-3a-kafka` löschen; Spring-Cheat-Sheet- und Environment-Abschnitte des README-Diffs, falls nicht bereits in 1–4 gelandet |

Regeln:

- README-Diff wird per `git add -p` bzw. gezielt zerlegt; wenn ein sauberer Split nicht
  praktikabel ist, landet der README-Diff komplett in Commit 1 und die Spec vermerkt das.
- `examples/rest-api` hängt von `oxide-kafka` ab (Cargo.toml-Diff); daher gehört sein Diff zu
  Commit 1.
- Commit-Messages im bestehenden Conventional-Commit-Stil des Repos.

## 4. Toolchain und Dependencies

1. `Cargo.toml` (Workspace): `rust-version = "1.88"`.
2. Neue `rust-toolchain.toml`:
   ```toml
   [toolchain]
   channel = "stable"
   components = ["rustfmt", "clippy"]
   ```
3. `Cargo.lock` aus `.gitignore` entfernen und einchecken.
4. `cargo update` (Minor/Patch innerhalb der deklarierten Ranges). Erwartung: h2 → 0.4.19,
   time → ≥ 0.3.47, hyper → 1.11.1 u. a.
5. `testcontainers` 0.23 → 0.28 in `[workspace.dependencies]`. Die drei Testharnesse
   (`crates/oxide-kafka/tests/harness/mod.rs`, `crates/oxide-mongo/tests/repository.rs`,
   `examples/kafka/tests/kafka_flow.rs`, dazu `examples/rest-api/tests/api.rs`) werden nur so
   weit angepasst, wie die neue API es verlangt (z. B. `ContainerAsync`, Image-Builder,
   `.start().await`). **Kein** Zusammenlegen der Harnesse — das ist Phase 4b.
6. Verifikation: `cargo audit` → 0 vulnerabilities; `cargo deny check advisories` → ok;
   `cargo test --workspace` → 92 bestanden (Zahl kann durch Upgrades nicht sinken).
7. Falls ein Advisory nach dem Upgrade bleibt (z. B. transitiv ohne Fix), wird es in
   `deny.toml` unter `[advisories] ignore` mit `reason` eingetragen und im README-Abschnitt
   „Security“ erwähnt.

## 5. Policy und lokale Pipeline

### deny.toml

```toml
[graph]
all-features = true

[advisories]
version = 2
yanked = "deny"
ignore = []   # jeder Eintrag braucht ein `reason`

[licenses]
version = 2
allow = [
  "MIT", "Apache-2.0", "Apache-2.0 WITH LLVM-exception",
  "BSD-2-Clause", "BSD-3-Clause", "ISC", "Zlib",
  "Unicode-3.0", "Unlicense", "MPL-2.0", "CC0-1.0", "OpenSSL",
]
confidence-threshold = 0.8

[bans]
multiple-versions = "warn"
wildcards = "deny"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
```

Die Allowlist wird beim ersten Lauf gegen den tatsächlichen Graphen abgeglichen; Lizenzen,
die im Graphen auftauchen und nicht in dieser Liste stehen, werden einzeln bewertet und
entweder ergänzt oder das Crate wird ersetzt. Copyleft (GPL/AGPL/LGPL) wird nicht
aufgenommen.

### Makefile

```make
.PHONY: fmt clippy test check audit ci
fmt:    ; cargo fmt --all -- --check
clippy: ; cargo clippy --workspace --all-targets -- -D warnings
test:   ; cargo test --workspace
check:  fmt clippy test
audit:  ; cargo audit && cargo deny check
ci:     check audit
```

Voraussetzung (im README dokumentiert): `cargo install cargo-audit cargo-deny --locked`,
laufender Docker für die Container-Tests.

### README

Neuer Abschnitt „Development“ nach „Quick start“: Toolchain (MSRV 1.88, `rust-toolchain.toml`),
`make ci` und die Einzelziele, Tool-Installation, Hinweis auf Docker. Statuszeile oben auf
„Status: Phase 3 (Integrationen) abgeschlossen; Phase 4 (Härtung) in Arbeit“.

## 6. Abgrenzung (Nicht-Ziele dieser Phase)

- Keine Major-Upgrades (OTel, tonic, reqwest, rdkafka, metrics-exporter-prometheus, tower-http).
- Keine Änderungen an Framework-Logik, kein Refactoring, keine neuen Features.
- Kein GitHub-/GitLab-CI-Workflow (Entscheidung 2026-09-02: nur lokal, bis ein Remote existiert).
- Kein Renovate/Dependabot (kein Remote).
- Bugs oder Sicherheitsprobleme, die beim Upgrade auffallen, werden **nicht** hier behoben,
  sondern in Anhang A dieser Spec ergänzt und in Phase 4b geplant.

## 7. Testen und Abnahme

Definition of Done, in dieser Reihenfolge nachzuweisen:

1. `git status` sauber; `git log` zeigt die Commits aus §3.
2. `make check` grün (fmt, clippy `-D warnings`, 92+ Tests).
3. `make audit` grün (0 vulnerabilities, deny ok oder ausschließlich dokumentierte Ignores).
4. `cargo tree -d | grep -c tokio-tar` → 0; `cargo tree -i time` zeigt ≥ 0.3.47.
5. `git worktree list` zeigt nur das Hauptverzeichnis; Branch `phase-3a-kafka` existiert nicht mehr.

## 8. Risiken

| Risiko | Umgang |
|---|---|
| testcontainers 0.28 ändert die Container-API stärker als erwartet | Minimal-invasive Anpassung; wenn > 1 Tag, Rückfall auf die kleinste Version ≥ 0.24, die tokio-tar ersetzt hat (prüfen via `cargo tree`) |
| `cargo update` hebt transitiv ein Crate über die MSRV 1.88 | `cargo update` läuft mit MSRV-aware Resolver (Cargo ≥ 1.84 default), also automatisch gekappt |
| Lizenz-Allowlist blockt ein benötigtes Crate | Einzelfallentscheidung, Eintrag mit Kommentar in `deny.toml` |
| README-Diff lässt sich nicht sauber auf 4 Commits verteilen | Fallback: kompletter README-Diff in Commit 1, in Spec vermerkt |

---

## Anhang A — Review-Findings für Phase 4b (Härten und Refactoren)

Erhoben am 2026-09-02 durch zwei Code-Reviews; **nicht** Teil von Phase 4a. Reihenfolge nach
Priorität.

### A.1 Sicherheit im eigenen Code

| # | Finding | Ort |
|---|---|---|
| S1 | Management- und Hauptserver binden fest auf `0.0.0.0`, keine `address`-Property, kein Auth für `/actuator/*` | `crates/oxide-web/src/serve.rs:47`, `ServerProps`/`ManagementProps` |
| S2 | Kein Request-Timeout, kein konfigurierbares Body-Limit, keine Header-Limits; `RouterLayer` (`fn(Router) -> Router`) bekommt weder Config noch Context | `crates/oxide-web/src/serve.rs:54`, `routing.rs:44` |
| S3 | `/actuator/health` führt bei jedem Request alle Indicators erneut aus (kein Cache, kein Limit) | `crates/oxide-web/src/health.rs:68` |
| S4 | Indicator-`detail` (Mongo-/Kafka-Fehlertexte, evtl. URIs) landet ungefiltert im Health-JSON | `crates/oxide-web/src/health.rs:85`, `crates/oxide-mongo/src/health.rs:23`, `crates/oxide-kafka/src/health.rs:25,30` |
| S5 | Mongo `_like` erzeugt `$regex` aus rohem Input (Regex-Injection/ReDoS); `_is_null`/`_is_not_null` semantisch falsch | `crates/oxide-mongo-macros/src/repository.rs:186-190` |
| S6 | `raw_filter`: sequentielles `str::replace`; `?1` matcht `?10`, bereits eingesetzte Werte werden erneut ersetzt | `crates/oxide-mongo/src/config.rs:200-210` |
| S7 | `MongoProps` mit `derive(Debug)` inkl. URI (Passwort); `failed to connect: {e}` leakt Driver-Text | `crates/oxide-mongo/src/config.rs:11-15,92` |
| S8 | HTTP-Client: `Path<T>` ohne Percent-Encoding in URL; `reqwest::Client::new()` ohne Timeouts; Metrik-Label `uri` = konkreter Pfad (Kardinalität) | `crates/oxide-client-macros/src/http_client.rs:86,222`, `crates/oxide-client/src/lib.rs:63` |
| S9 | Env-Binding parst jeden `OXIDE_*`-Wert als YAML und legt beliebige neue Keys an | `crates/oxide-core/src/config.rs:93-105` |
| S10 | `expect`/`unwrap` im Boot-/Nicht-Test-Pfad statt Boot-Fehler | u. a. `oxide-web/src/management.rs:90,154,162`, `server.rs:88,93`, `oxide-observability/src/otel.rs:184`, `oxide-kafka/src/listener.rs:168,249,429,435`, `producer.rs:39,128`, `oxide-mongo/src/config.rs:80,87`, `oxide-client/src/lib.rs:35,87`, `oxide-boot/src/app.rs:149` |

### A.2 Robustheit

| # | Finding | Ort |
|---|---|---|
| R1 | Kafka-Shutdown wartet nicht auf Worker-JoinHandles; letzte Commits nicht garantiert | `crates/oxide-kafka/src/listener.rs:233,355,440` |
| R2 | Rebalance ignoriert (kein `post_rebalance`); Worker pro Partition leben ewig | `listener.rs:174-358` |
| R3 | Deserialize-Fehler werden retried statt direkt DLT; Backoff ohne Cap/Jitter | `oxide-kafka-macros/src/listeners.rs:177-181`, `retry.rs:17-22` |
| R4 | `subscribe`-Fehler → `continue` statt Boot-Fehler | `listener.rs:406-409` |
| R5 | Producer: `message.send.max.retries=MAX` auch ohne Idempotenz; Timeouts hartkodiert 10 s | `producer.rs:37,90,111` |
| R6 | Kafka-Health baut pro Check einen neuen `BaseConsumer` | `oxide-kafka/src/health.rs:23` |
| R7 | Readiness geht erst im Drain auf DOWN, kein Karenzintervall für K8s; `handle.abort()` ohne Fehler-Rückgabe | `oxide-web/src/server.rs:71`, `serve.rs:92` |
| R8 | OTel-Shutdown ohne Timeout | `oxide-observability/src/otel.rs:169` |
| R9 | `std::env::vars()` panict bei Nicht-UTF-8 | `oxide-boot/src/app.rs:69` |
| R10 | `ContextBuilder::insert` überschreibt still bei gleicher `TypeId` | `oxide-core/src/container.rs:145` |

### A.3 Refactoring

| # | Finding | Ort |
|---|---|---|
| F1 | Drei Registrierungsmuster für Starter; `#[component]` kennt weder `destroy_order` noch Context-Hooks | `oxide-macros/src/component.rs:63-78`, `oxide-web/src/server.rs:76-97`, `management.rs:142-171`, `otel.rs:174-189` |
| F2 | Testcontainer-Harness 3× kopiert, jeweils mit `unsafe libc::atexit` | `oxide-kafka/tests/harness/mod.rs`, `oxide-mongo/tests/repository.rs:44-93`, `examples/kafka/tests/kafka_flow.rs:35-104`, `examples/rest-api/tests/api.rs` |
| F3 | Health-Indicator-Boilerplate identisch in Kafka und Mongo | `oxide-kafka/src/health.rs:37-55`, `oxide-mongo/src/health.rs:14-35` |
| F4 | `listener.rs` (448 Z.) mit 4 Verantwortlichkeiten; `RouteMessage` dupliziert `ReceivedMessage`; `ListenerInfra {}` leer | `oxide-kafka/src/listener.rs` |
| F5 | `name_to_snake` dupliziert | `oxide-client-macros/src/http_client.rs:102`, `oxide-mongo-macros/src/document.rs:95` |
| F6 | Mongo-Repository nicht als Component registriert; `DocumentMeta` ungenutzt; `save == insert`; `find_by_x` liefert `Vec` statt `Option` | `oxide-mongo/src/config.rs:155,162`, `tests/repository.rs:112` |
| F7 | Env-Binding `set_relaxed`/`set_existing` fast identisch | `oxide-core/src/config.rs:90-201` |
| F8 | `oxide-observability` greift auf `oxide_web::__private::axum`; `oxide-boot::__private` re-exportiert `oxide_core::*` pauschal | `oxide-observability/src/actuator.rs:7-10`, `oxide-boot/src/lib.rs:16` |
| F9 | `oxide-kafka` zieht `opentelemetry_sdk` mit Feature `testing` als normale Dependency | `crates/oxide-kafka/Cargo.toml:19` |
| F10 | Duplizierte Validierung/Defaults (`default_port`, `application.name`-Auflösung) | `management.rs:14-19,56-68`, `server.rs:10-15`, `routing.rs:21-34`, `otel.rs:39-41,97`, `app.rs:72` |

### A.4 Lücken zum Gesamtdesign (Kandidaten für Phase 4c)

- §4: Trait-Injection (`Arc<dyn Trait>`), `#[service]`/`#[repository]`-Aliase, `#[post_construct]` als Methoden-Attribut.
- §5: Validierung von ConfigProperties beim Start; `${…}` in Makro-Attributen; Env-Auflösung in Platzhaltern.
- §6: `Header<T>`-Extractor; `@Valid`/`validator` mit Feldfehlern im Problem Detail; Timeout/CORS/Compression-Layer per Config; Git-Info in `/actuator/info`.
- §7: Client-`connect-timeout`, `request-timeout`, `retry`, Basis-Header per Config; ProblemDetail-Parsing.
- §8: Indizes beim Start; `Page`/`Pageable`; `_or_`, `_between`, `_order_by`; Compile-Zeit-Feldvalidierung; `exists_by_id`; Command-Metriken/Spans.
- §9: DLT-Name/-Aktivierung pro Listener; Retry ohne Partition-Blockade.
- §10: Scheduler (fehlt komplett).
- §11: Runtime-Metriken (Tokio, CPU/RSS/FDs); `trace_id`/`span_id` in Logzeilen.
- §12: `TestApp::replace::<T>(mock)`; Testcontainer-Fixtures in `oxide-test`.
