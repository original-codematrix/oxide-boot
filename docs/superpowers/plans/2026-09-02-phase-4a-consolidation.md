# Phase 4a — Konsolidieren und absichern: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Die uncommitted Phase-3-Starter sauber in die Historie bringen, alle bekannten RUSTSEC-Advisories schließen, MSRV/Toolchain/Lockfile reproduzierbar machen und eine lokale Security-Pipeline (`make ci`) etablieren — ohne Framework-Logik zu ändern.

**Architecture:** Reine Repository-Hygiene: Commits werden per gezielt zusammengesetzten Dateiständen (nicht `git add -p`) erzeugt, damit jeder Zwischenstand baut. Dependency-Fixes laufen ausschließlich über `Cargo.toml`-Ranges + `cargo update`; Testharnesse werden nur an die neue testcontainers-API angepasst. Policy lebt in `deny.toml`, Einstiegspunkt ist ein `Makefile`.

**Tech Stack:** Rust 1.88+ (stable), cargo-audit 0.22, cargo-deny 0.20, testcontainers 0.28, rustfmt, clippy, GNU make, Docker (für Container-Tests).

**Spec:** `docs/superpowers/specs/2026-09-02-phase-4a-consolidation-design.md`

## Global Constraints

- Branch: `continue-next`. Kein Remote vorhanden; nichts pushen.
- `rust-version = "1.88"` im Workspace (Spec §4.1).
- Keine Major-Upgrades: opentelemetry, tracing-opentelemetry, tonic, reqwest (direkte Dep), rdkafka, metrics-exporter-prometheus, tower-http bleiben auf ihren heutigen Major-Versionen (Spec §6).
- Keine Änderung an Framework-Logik, kein Refactoring; Bugs nur in Spec-Anhang A notieren (Spec §6).
- Jeder Commit muss `cargo check --workspace --all-targets` bestehen (Spec §3).
- Testzahl darf nicht unter 92 fallen (Spec §4.6).
- Commit-Messages: Conventional Commits, Trailer:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE
  ```
- Scratch-Verzeichnis für Zwischenkopien: `a scratch directory outside the repository` (im Folgenden `$SCRATCH`).
- Lizenz-Allowlist (Spec §5) erweitert um die im Graphen tatsächlich vorkommenden permissiven Lizenzen `BSL-1.0` (ryu) und `CDLA-Permissive-2.0` (webpki-roots); Copyleft wird nicht aufgenommen.
- Abweichung zur Spec, bewusst: Der Code wurde nie mit rustfmt formatiert (317 Dateien mit Diff). Task 6 führt `rustfmt.toml` (`max_width = 120`, `use_small_heuristics = "Max"`) ein und formatiert einmalig; ohne diesen Schritt wäre das `fmt`-Ziel aus Spec §5 sofort rot.

---

## Ausgangszustand (verifiziert 2026-09-02)

```
git status --short
 M Cargo.toml                      # Workspace-Einträge für alle 8 neuen Crates + rdkafka/mongodb/testcontainers
 M README.md                       # +253 Zeilen (4 Starter-Abschnitte, Z. 118-370) + 5 Zeilen Cheat-Sheet (Z. 407-411)
 M examples/rest-api/Cargo.toml    # + oxide-kafka, tracing, testcontainers, libc
 M examples/rest-api/application.yml
 M examples/rest-api/src/lib.rs    # Kafka-Producer + Listener
 M examples/rest-api/tests/api.rs  # Redpanda-Container-Test
?? .idea/
?? crates/oxide-client-macros/ crates/oxide-client/
?? crates/oxide-kafka-macros/  crates/oxide-kafka/
?? crates/oxide-mongo-macros/  crates/oxide-mongo/
?? crates/oxide-test-macros/   crates/oxide-test/
?? examples/http-client/ examples/kafka/
```

README-Abschnitte im **vollständigen** (modifizierten) README:

| Zeilen | Abschnitt | Gehört zu Commit |
|---|---|---|
| 7-8 | `**Status: Phase 1 (foundation).** …` | Task 5 |
| 118-213 | `## Kafka (oxide-kafka)` | Task 1 |
| 214-278 | `## MongoDB (oxide-mongo)` | Task 2 |
| 279-323 | `## HTTP client (oxide-client)` | Task 3 |
| 324-370 | `## Tests (oxide-test)` | Task 4 |
| 407-411 | 5 Kafka-Zeilen im Cheat-Sheet | Task 1 |

Crate-Abhängigkeiten (nur Phase-3-Crates): kafka → {core, boot, web, observability, rdkafka, testcontainers}; mongo → {core, boot, web, mongodb, testcontainers}; client → {core, boot, reqwest}; test → {core, boot, web, reqwest}. `examples/rest-api` → oxide-kafka. `examples/kafka` → oxide-kafka. `examples/http-client` → oxide-client, oxide-web. Kein Example hängt von oxide-test ab. Damit ist die Reihenfolge kafka → mongo → client → test baubar.

---

### Task 0: Vollständige Stände sichern

**Files:**
- Read: `Cargo.toml`, `README.md`

- [ ] **Step 1: Vollkopien anlegen**

```bash
cp Cargo.toml "$SCRATCH/Cargo.full.toml"
cp README.md  "$SCRATCH/README.full.md"
```

- [ ] **Step 2: Zeilenangaben verifizieren**

Run:
```bash
grep -n '^## ' "$SCRATCH/README.full.md"
sed -n '407,411p' "$SCRATCH/README.full.md" | cut -c1-40
```
Expected: `118:## Kafka`, `214:## MongoDB`, `279:## HTTP client`, `324:## Tests`, `371:## Environment overrides`; die fünf Zeilen beginnen mit `| \`@KafkaListener`, `| \`KafkaTemplate`, `| \`DeadLetterPublishingRecoverer`, `| \`spring.kafka.bootstrap-servers`, `| Spring-Kafka consumer lag`.

Falls die Zeilen abweichen: Zahlen in Tasks 1-4 entsprechend anpassen, **bevor** weitergearbeitet wird.

---

### Task 1: Commit `feat(kafka)`

**Files:**
- Modify: `Cargo.toml` (Teilstand), `README.md` (Teilstand)
- Add: `crates/oxide-kafka/`, `crates/oxide-kafka-macros/`, `examples/kafka/`, `examples/rest-api/{Cargo.toml,application.yml,src/lib.rs,tests/api.rs}`

**Interfaces:**
- Produces: Workspace mit den Membern `crates/oxide-kafka`, `crates/oxide-kafka-macros`, `examples/kafka`; Workspace-Deps `rdkafka`, `testcontainers`.

- [ ] **Step 1: Root-Cargo.toml auf Kafka-Stand bringen**

Ausgehend von `$SCRATCH/Cargo.full.toml` folgende Zeilen **entfernen** (alle anderen bleiben):

```
    "crates/oxide-mongo",
    "crates/oxide-mongo-macros",
    "crates/oxide-client",
    "crates/oxide-client-macros",
    "crates/oxide-test",
    "crates/oxide-test-macros",
    "examples/http-client",
oxide-mongo = { path = "crates/oxide-mongo" }
oxide-mongo-macros = { path = "crates/oxide-mongo-macros" }
oxide-client = { path = "crates/oxide-client" }
oxide-client-macros = { path = "crates/oxide-client-macros" }
oxide-test = { path = "crates/oxide-test" }
oxide-test-macros = { path = "crates/oxide-test-macros" }
mongodb = "3.8"
```

```bash
grep -v -E '^\s*"crates/oxide-(mongo|client|test)(-macros)?",|^\s*"examples/http-client",|^oxide-(mongo|client|test)(-macros)? = |^mongodb = ' "$SCRATCH/Cargo.full.toml" > Cargo.toml
grep -c 'oxide-kafka' Cargo.toml   # Expected: 4 (2 members + 2 deps)
grep -c 'mongo\|client\|oxide-test' Cargo.toml   # Expected: 0
```

- [ ] **Step 2: README auf Kafka-Stand bringen**

```bash
sed '214,370d' "$SCRATCH/README.full.md" > README.md
grep -n '^## ' README.md
```
Expected: `## Kafka` bei 118, danach direkt `## Environment overrides` bei 214, `## Spring-to-Oxide cheat sheet` bei 230; `grep -c 'KafkaTemplate' README.md` → 1.

- [ ] **Step 3: Build-Check des Zwischenstands**

Run: `cargo check --workspace --all-targets 2>&1 | tail -3`
Expected: `Finished` ohne `error`. (Die untracked Mongo/Client/Test-Crates sind keine Member und werden nicht gebaut.)

- [ ] **Step 4: Stage und Commit**

```bash
git add Cargo.toml README.md crates/oxide-kafka crates/oxide-kafka-macros examples/kafka examples/rest-api
git status --short | grep -v '^??'   # Expected: nur A/M-Zeilen der obigen Pfade
git commit -m "feat(kafka): oxide-kafka starter with listeners, producer, retry/DLT and health

Adds crates/oxide-kafka (+ macros): #[kafka_listeners]/#[kafka_listener],
KafkaProducer, per-partition workers with retry + <topic>.DLT, consumer lag
and processing-time metrics, traceparent propagation over headers and a
kafka HealthIndicator. examples/kafka and the kafka wiring in
examples/rest-api exercise it end to end against a Redpanda container.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 2: Commit `feat(mongo)`

**Files:**
- Modify: `Cargo.toml` (Teilstand), `README.md` (Teilstand)
- Add: `crates/oxide-mongo/`, `crates/oxide-mongo-macros/`

- [ ] **Step 1: Root-Cargo.toml auf Mongo-Stand bringen**

```bash
grep -v -E '^\s*"crates/oxide-(client|test)(-macros)?",|^\s*"examples/http-client",|^oxide-(client|test)(-macros)? = ' "$SCRATCH/Cargo.full.toml" > Cargo.toml
grep -c 'mongo' Cargo.toml    # Expected: 5 (2 members + 2 deps + mongodb)
grep -c 'client\|oxide-test' Cargo.toml   # Expected: 0
```

- [ ] **Step 2: README auf Mongo-Stand bringen**

```bash
sed '279,370d' "$SCRATCH/README.full.md" > README.md
grep -n '^## ' README.md
```
Expected: `## MongoDB` bei 214, `## Environment overrides` bei 279.

- [ ] **Step 3: Build-Check**

Run: `cargo check --workspace --all-targets 2>&1 | tail -3`
Expected: `Finished` ohne `error`.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml README.md crates/oxide-mongo crates/oxide-mongo-macros
git commit -m "feat(mongo): oxide-mongo starter with Document derive and repository macro

Adds crates/oxide-mongo (+ macros): MongoClient/MongoProps under oxide.mongo,
#[derive(Document)] with #[collection]/#[id], #[mongo_repository] with CRUD,
derived queries (find/count/exists/delete_by + operators) and the #[query]
escape hatch, plus a mongo HealthIndicator. Integration test runs against a
mongo:7 container.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 3: Commit `feat(client)`

**Files:**
- Modify: `Cargo.toml` (Teilstand), `README.md` (Teilstand)
- Add: `crates/oxide-client/`, `crates/oxide-client-macros/`, `examples/http-client/`

- [ ] **Step 1: Root-Cargo.toml auf Client-Stand bringen**

```bash
grep -v -E '^\s*"crates/oxide-test(-macros)?",|^oxide-test(-macros)? = ' "$SCRATCH/Cargo.full.toml" > Cargo.toml
grep -c 'oxide-test' Cargo.toml   # Expected: 0
grep -c 'http-client' Cargo.toml  # Expected: 1
```

- [ ] **Step 2: README auf Client-Stand bringen**

```bash
sed '324,370d' "$SCRATCH/README.full.md" > README.md
grep -n '^## ' README.md
```
Expected: `## HTTP client` bei 279, `## Environment overrides` bei 324.

- [ ] **Step 3: Build-Check**

Run: `cargo check --workspace --all-targets 2>&1 | tail -3`
Expected: `Finished` ohne `error`.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml README.md crates/oxide-client crates/oxide-client-macros examples/http-client
git commit -m "feat(client): oxide-client declarative HTTP client (FeignClient-style)

Adds crates/oxide-client (+ macros): #[http_client(base_url = \"\${...}\")] on a
trait generates a reqwest-backed component with Path/Json parameters,
traceparent injection, http_client_requests_seconds metrics and a typed
ClientError. examples/http-client calls the rest-api example in-process.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 4: Commit `feat(test)`

**Files:**
- Modify: `Cargo.toml` (Vollstand), `README.md` (Vollstand)
- Add: `crates/oxide-test/`, `crates/oxide-test-macros/`

- [ ] **Step 1: Vollstände wiederherstellen**

```bash
cp "$SCRATCH/Cargo.full.toml" Cargo.toml
cp "$SCRATCH/README.full.md"  README.md
git diff --stat   # Expected: Cargo.toml (+4 Zeilen: 2 members, 2 deps) und README.md (+47 Zeilen) — nur die Test-Anteile
```

- [ ] **Step 2: Build-Check**

Run: `cargo check --workspace --all-targets 2>&1 | tail -3`
Expected: `Finished` ohne `error`.

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml README.md crates/oxide-test crates/oxide-test-macros
git commit -m "feat(test): oxide-test harness with #[oxide_test] and TestApp builder

Adds crates/oxide-test (+ macros): boot the application under test on
ephemeral ports (OXIDE_SERVER_PORT=0 / OXIDE_MANAGEMENT_PORT=0), optional
test profile, TestWebClient for main and management servers, and the
#[oxide_test] attribute as the @SpringBootTest equivalent.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
git status --short   # Expected: nur "?? .idea/"
```

---

### Task 5: Commit `chore` — .idea, README-Status, Worktree

**Files:**
- Modify: `.gitignore`, `README.md:7-8`
- Remove: `.claude/worktrees/phase-3a` (Worktree), Branch `phase-3a-kafka`

- [ ] **Step 1: .gitignore erweitern**

Anhängen an `.gitignore`:
```
.idea/
```
Run: `git status --short` → Expected: `.idea/` erscheint nicht mehr, nur ` M .gitignore`.

- [ ] **Step 2: README-Status aktualisieren**

Ersetze in `README.md` die Zeilen 7-8

```
**Status: Phase 1 (foundation).** Web, MongoDB, Kafka, HTTP client and
observability starters are planned — see `docs/superpowers/specs/`.
```
durch
```
**Status: Phase 3 (integrations) complete; Phase 4 (hardening) in progress.**
Web, observability, Kafka, MongoDB, HTTP client and test starters are
implemented — see `docs/superpowers/specs/` for the design and the phase-4
hardening backlog.
```

- [ ] **Step 3: Worktree und Branch entfernen**

```bash
git worktree remove --force .claude/worktrees/phase-3a
git branch -D phase-3a-kafka
git worktree list        # Expected: nur the repository root
git branch               # Expected: continue-next, main
```

- [ ] **Step 4: Commit**

```bash
git add .gitignore README.md
git commit -m "chore: ignore .idea, mark phase 3 complete, drop stale phase-3a worktree

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 6: rustfmt-Baseline

**Files:**
- Create: `rustfmt.toml`
- Modify: alle `*.rs` (mechanisch)

- [ ] **Step 1: rustfmt.toml anlegen**

```toml
# Closest match to the existing hand-written style (fewest diffs).
max_width = 120
use_small_heuristics = "Max"
edition = "2024"
```

- [ ] **Step 2: Formatieren und prüfen**

```bash
cargo fmt --all
cargo fmt --all -- --check && echo FMT_CLEAN
cargo check --workspace --all-targets 2>&1 | tail -1
```
Expected: `FMT_CLEAN`, `Finished`.

- [ ] **Step 3: Commit**

```bash
git add rustfmt.toml .
git commit -m "style: adopt rustfmt (max_width 120) and format the workspace

Mechanical change only; rustfmt.toml pins the style closest to the existing
code so `cargo fmt --check` can gate future changes.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 7: MSRV 1.88, rust-toolchain.toml, Cargo.lock einchecken

**Files:**
- Modify: `Cargo.toml` (`rust-version`), `.gitignore`
- Create: `rust-toolchain.toml`
- Add: `Cargo.lock`

- [ ] **Step 1: MSRV setzen**

In `Cargo.toml` unter `[workspace.package]`: `rust-version = "1.85"` → `rust-version = "1.88"`.

- [ ] **Step 2: rust-toolchain.toml anlegen**

```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 3: Cargo.lock aus .gitignore entfernen**

In `.gitignore` die Zeile `Cargo.lock` löschen. Run: `git status --short Cargo.lock` → Expected: `?? Cargo.lock`.

- [ ] **Step 4: Build-Check und Commit**

```bash
cargo check --workspace --all-targets 2>&1 | tail -1     # Expected: Finished
git add Cargo.toml rust-toolchain.toml .gitignore Cargo.lock
git commit -m "build: MSRV 1.88, pin stable toolchain, track Cargo.lock

mongodb 3.8 already requires Rust 1.88, so the declared 1.85 floor was
wrong. Tracking the lockfile makes builds and security scans reproducible.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 8: `cargo update` (Minor/Patch) — schließt h2 und time

**Files:**
- Modify: `Cargo.lock`

- [ ] **Step 1: Ausgangszustand des Scans festhalten**

Run: `cargo audit 2>&1 | grep -E '^(Crate|ID|error)'`
Expected (vor dem Update): `h2`, `time`, `tokio-tar` mit `RUSTSEC-2026-0258`, `RUSTSEC-2026-0009`, `RUSTSEC-2025-0111`; `error: 3 vulnerabilities found!`.

- [ ] **Step 2: Update ausführen**

```bash
cargo update 2>&1 | grep -E 'Updating (h2|time|hyper) ' 
cargo tree -i time --depth 0     # Expected: time v0.3.47 oder höher
cargo tree -i h2 --depth 0       # Expected: h2 v0.4.19 oder höher
```

Falls `time` auf 0.3.45 bleibt: `cargo update -p time` ausführen und erneut prüfen. (Mit `rust-version = "1.88"` ist der MSRV-aware Resolver nicht mehr die Bremse.)

- [ ] **Step 3: Scan prüfen**

Run: `cargo audit 2>&1 | grep -E '^(Crate|ID|error)'`
Expected: nur noch `tokio-tar` / `RUSTSEC-2025-0111`; `error: 1 vulnerability found!`.

- [ ] **Step 4: Tests**

Run: `cargo test --workspace 2>&1 | grep -E '^test result' | awk '{p+=$4; f+=$6} END {print "passed="p" failed="f}'`
Expected: `passed=92 failed=0` (oder passed > 92).

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock
git commit -m "build(deps): cargo update — fixes RUSTSEC-2026-0258 (h2) and RUSTSEC-2026-0009 (time)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 9: testcontainers 0.23 → 0.28 — schließt tokio-tar, async-std, rustls-pemfile

**Files:**
- Modify: `Cargo.toml` (`testcontainers = "0.28"`), `Cargo.lock`
- Möglicherweise: `crates/oxide-kafka/tests/harness/mod.rs`, `crates/oxide-mongo/tests/repository.rs`, `examples/kafka/tests/kafka_flow.rs`, `examples/rest-api/tests/api.rs`

**Interfaces:**
- Consumes (heute genutzte testcontainers-API, laut Changelog seit 0.20 stabil): `GenericImage::new(name, tag)`, `ImageExt::with_mapped_port(host, port.tcp())`, `ImageExt::with_cmd([...])`, `AsyncRunner::start().await -> Result<ContainerAsync<GenericImage>>`, `ContainerAsync::id() -> &str`.

- [ ] **Step 1: Version anheben**

In `Cargo.toml`: `testcontainers = "0.23"` → `testcontainers = "0.28"`.

```bash
cargo update -p testcontainers 2>&1 | grep -E 'Updating|Adding|Removing' | head -30
cargo tree -i tokio-tar --depth 0 2>&1 | head -1        # Expected: "error: package ID specification ... did not match" (nicht mehr im Graph)
cargo tree -i astral-tokio-tar --depth 0                # Expected: astral-tokio-tar vX
```

- [ ] **Step 2: Kompilieren**

Run: `cargo check --workspace --all-targets 2>&1 | grep -E '^(error|warning)' | head -20`
Expected: leer. Falls Fehler in einem der vier Harness-Dateien: nur die betroffene Zeile an die neue Signatur anpassen (z. B. Trait-Import `use testcontainers::core::IntoContainerPort;` bleibt; falls `ContainerAsync::id()` jetzt `String` statt `&str` liefert, `.to_string()` entfernen). Keine strukturellen Änderungen, keine Zusammenlegung.

- [ ] **Step 3: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -1`
Expected: `Finished`.

- [ ] **Step 4: Container-Tests**

Run: `cargo test --workspace 2>&1 | grep -E '^test result' | awk '{p+=$4; f+=$6} END {print "passed="p" failed="f}'`
Expected: `passed=92 failed=0` (oder mehr passed). Bei Container-Start-Fehlern zuerst `docker ps -a | grep -E 'redpanda|mongo'` prüfen und Leichen mit `docker rm -f <id>` entfernen (Port 9092/27017 ist gepinnt).

- [ ] **Step 5: Scan**

```bash
cargo audit 2>&1 | tail -3
```
Expected: `Scanning Cargo.lock for vulnerabilities (...)` gefolgt von keiner `error:`-Zeile; keine Warnung mehr für `async-std` oder `rustls-pemfile`. Falls doch noch ein Advisory erscheint, dessen Pfad ausschließlich über dev-dependencies führt und für das es keinen Fix gibt: ID notieren, in Task 10 unter `[advisories] ignore` mit `reason` eintragen und im README-Abschnitt „Security“ (Task 11) erwähnen.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock crates examples
git commit -m "build(deps): testcontainers 0.28 — drops tokio-tar (RUSTSEC-2025-0111), async-std, rustls-pemfile

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 10: `deny.toml`

**Files:**
- Create: `deny.toml`

- [ ] **Step 1: deny.toml anlegen**

```toml
# cargo-deny policy for the oxide-boot workspace.
# Run: cargo deny check   (or `make audit`)

[graph]
all-features = true

[advisories]
version = 2
yanked = "deny"
# Every entry here needs a `reason`. Remove entries as soon as a fix exists.
ignore = []

[licenses]
version = 2
allow = [
    "MIT",
    "Apache-2.0",
    "Apache-2.0 WITH LLVM-exception",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "Zlib",
    "Unicode-3.0",
    "Unlicense",
    "MPL-2.0",
    "CC0-1.0",
    "OpenSSL",
    "BSL-1.0",              # ryu
    "CDLA-Permissive-2.0",  # webpki-roots
]
confidence-threshold = 0.8
# Workspace crates with `publish = false` and no `license` field (examples).
private = { ignore = true }

[bans]
multiple-versions = "warn"
wildcards = "deny"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
```

- [ ] **Step 2: Alle Checks laufen lassen**

```bash
cargo deny check 2>&1 | grep -E '^(error|warning)\[' | sort | uniq -c
cargo deny check 2>&1 | grep -E '^(advisories|licenses|bans|sources) '
```
Expected: keine `error[`-Zeilen; `advisories ok`, `licenses ok`, `bans ok` (oder `bans warn` wegen Duplikaten), `sources ok`.

Falls `error[rejected]` für eine Lizenz erscheint: `cargo deny list | grep '^<LIZENZ>'` zeigt das Crate. Ist die Lizenz permissiv (kein GPL/AGPL/LGPL/SSPL/EUPL), in `allow` mit Kommentar ergänzen. Hinweis: `r-efi` ist `MIT OR Apache-2.0 OR LGPL-2.1-or-later` und wird über MIT akzeptiert; `LGPL` darf **nicht** in die Allowlist.

- [ ] **Step 3: Commit**

```bash
git add deny.toml
git commit -m "build: add cargo-deny policy (advisories, license allowlist, sources)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 11: `Makefile` und README „Development“

**Files:**
- Create: `Makefile`
- Modify: `README.md` (neuer Abschnitt nach „Quick start“, vor „Web (oxide-web)“)

- [ ] **Step 1: Makefile anlegen**

```make
# Local CI entry point. Requires: rustup (stable), Docker (container tests),
# `cargo install cargo-audit cargo-deny --locked`.

.PHONY: fmt clippy test check audit ci

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

check: fmt clippy test

audit:
	cargo audit
	cargo deny check

ci: check audit
```

(Rezeptzeilen müssen mit **Tab** eingerückt sein, sonst `*** missing separator`.)

- [ ] **Step 2: Makefile ausführen**

Run: `make ci 2>&1 | tail -5`
Expected: endet ohne `make: ***`-Fehlerzeile; letzte Zeilen enthalten `advisories ok` … `sources ok`.

- [ ] **Step 3: README-Abschnitt einfügen**

Direkt vor der Zeile `## Web (oxide-web)` einfügen:

````markdown
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
````

- [ ] **Step 4: Commit**

```bash
git add Makefile README.md
git commit -m "build: Makefile with check/audit/ci targets; document the dev workflow

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01KCbCYDWYust7oN81JcesaE"
```

---

### Task 12: Definition of Done nachweisen (Spec §7)

**Files:** keine Änderungen (nur Verifikation). Falls etwas rot ist: zurück zum betroffenen Task, **nicht** hier flicken.

- [ ] **Step 1: Historie und Arbeitsbaum**

```bash
git status --short            # Expected: leer
git log --oneline -12         # Expected (neueste zuerst): build: Makefile…, build: add cargo-deny…, build(deps): testcontainers…, build(deps): cargo update…, build: MSRV…, style: adopt rustfmt…, chore: ignore .idea…, feat(test)…, feat(client)…, feat(mongo)…, feat(kafka)…, docs: design spec for phase 4a…
```

- [ ] **Step 2: `make check`**

Run: `make check 2>&1 | grep -E '^test result' | awk '{p+=$4; f+=$6} END {print "passed="p" failed="f}'`
Expected: `passed=92 failed=0` oder mehr passed; kein `make: ***`.

- [ ] **Step 3: `make audit`**

Run: `make audit 2>&1 | grep -E 'vulnerabilit|advisories|licenses|bans|sources'`
Expected: keine Zeile mit `vulnerabilities found`; `advisories ok`, `licenses ok`, `sources ok`.

- [ ] **Step 4: Graph-Belege**

```bash
cargo tree -d 2>/dev/null | grep -c '^tokio-tar'      # Expected: 0
cargo tree -i time --depth 0                            # Expected: >= 0.3.47
git worktree list                                       # Expected: nur Hauptverzeichnis
git branch --list phase-3a-kafka                        # Expected: leer
```

- [ ] **Step 5: Abschlussbericht**

Kurz zusammenfassen: Commit-Liste, Scan-Ergebnis vorher/nachher (3 → 0 Advisories), Testzahl, und alle während der Umsetzung aufgefallenen Bugs als Ergänzung in `docs/superpowers/specs/2026-09-02-phase-4a-consolidation-design.md` Anhang A eintragen (eigener `docs:`-Commit).
