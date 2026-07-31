# Oxide Boot Phase 2a (Web-Kern) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `oxide-web`-Starter mit `#[controller]`-Macro, RFC-7807-Fehlern, automatisch gemountetem Axum-Server im Boot-Lifecycle (Graceful Shutdown, Basis-Health-Endpoints) — plus die beschlossene Env-Binding-Erweiterung (Fallback-Stripping) in `oxide-core`.

**Architecture:** Neues Crate-Paar `oxide-web` (Runtime: `ServerProps`, `WebServer`-Component, `RouteDescriptor`-Registry, `ApiError`/`ProblemDetail`, Health-Routen) + `oxide-web-macros` (`#[controller]`). Controller-Methoden werden zu Axum-Handler-Closures expandiert, die beim Router-Bau ihre Controller-`Inject<T>`-Handles EINMAL aus dem `Context` auflösen und in die Closure moven — kein State-Plumbing, keine Core-Änderungen. Der `WebServer` registriert sich per handgeschriebenem `ComponentDescriptor` (inventory), baut den Router synchron im `post_construct`-Hook-Closure (dort ist `&Context` verfügbar) und serviert in einem gespawnten Task mit `with_graceful_shutdown`.

**Tech Stack:** axum 0.8 (Pfad-Syntax `{id}`), tower, tokio, reqwest (nur dev-dependency für Tests), plus der bestehende Stack (serde, inventory, thiserror, syn/quote, trybuild).

## Global Constraints

- Alles aus Phase 1 gilt weiter: Edition `2024`, `rust-version = "1.85"`, Versionen NUR in `[workspace.dependencies]`, `thiserror`/kein `anyhow`, kein `unwrap()`/`expect()` in Library-Code (Ausnahmen nur unreachable-by-construction, im Report geflaggt), Code/Kommentare Englisch, Conventional Commits, TDD mit RED/GREEN-Evidenz.
- Spec: `docs/superpowers/specs/2026-07-31-oxide-boot-design.md` (§5 Env-Binding-Regel, §6 Web-Layer); bei Widerspruch gewinnt die Spec.
- Macro-generierter Code aus `oxide-web-macros` referenziert NUR `::oxide_web::__private::*`-Pfade (User-Crates deps: `oxide-boot` + `oxide-web`, Spring-Starter-Modell).
- Etablierte Testkonventionen: Boot-Tests im selben Binary serialisieren über eine `BOOT_LOCK`-`tokio::sync::Mutex` und delta-checken Counter (inventory ist prozess-global); Komponenten, die in einem Test-Binary registriert sind, booten in JEDEM Test dieses Binaries — die geteilte Config-Helper-Funktion muss alle Keys aller registrierten Components liefern; Tests binden Server IMMER an Port 0 und lesen die echte Adresse über `WebServer::local_addr()`.
- Deterministik: Env-Overrides werden sortiert angewendet (seit d58c7be); Routen werden sortiert gemountet.
- HTTP-Fehler-Payloads sind IMMER `application/problem+json` (RFC 7807) mit Feldern `type` (Default `about:blank`), `title`, `status`, optional `detail`/`instance`.
- Nicht-Ziele 2a (kommen in 2b oder später — NICHT bauen): Metriken/OTEL/Prometheus, Management-Port, Health-Indicators aus Integrationen, Request-Validierung, CORS/Compression/Timeout-Middleware, `Header<T>`-Extractor, `#[component(binds = Trait)]`.

## File Structure (Endzustand 2a, neu/geändert)

```
oxide-boot/
├── Cargo.toml                          # + Members oxide-web, oxide-web-macros, examples/rest-api; + Deps axum/tower/serde_json/reqwest
├── README.md                           # Env-Binding-Abschnitt aktualisiert; Web-Quickstart ergänzt
├── crates/
│   ├── oxide-core/src/config.rs        # apply_env_overrides: Fallback-Stripping (Task 1)
│   ├── oxide-web/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # Module, prelude, __private
│   │       ├── error.rs                # ProblemDetail, ApiError, IntoProblemDetail, WebError
│   │       ├── routing.rs              # RouteDescriptor, build_router
│   │       ├── server.rs               # ServerProps, WebServer, Descriptor, Lifecycle
│   │       └── health.rs               # /actuator/health(+liveness/readiness)
│   │   └── tests/
│   │       ├── server.rs               # Boot→Health→Shutdown, Routing, Controller-E2E, Drain
│   │       ├── ui.rs + ui/             # trybuild für #[controller]-Fehler
│   ├── oxide-web-macros/
│   │   ├── Cargo.toml                  # proc-macro
│   │   └── src/
│   │       ├── lib.rs
│   │       └── controller.rs           # #[controller] inkl. join_paths + Unit-Tests
│   └── oxide-boot/tests/boot_failure.rs# post_construct-Fehler → BootError (eigenes Binary!)
└── examples/rest-api/                  # Notes-API: lib+bin+yml+test
```

---

### Task 1: Env-Binding Fallback-Stripping (oxide-core)

**Files:**
- Modify: `crates/oxide-core/src/config.rs`
- Modify: `README.md` (Abschnitt "Environment overrides")

**Interfaces:**
- Consumes: bestehendes `apply_env_overrides`/`set_relaxed` (sortierte Anwendung seit d58c7be)
- Produces: Binding-Reihenfolge pro Var `OXIDE_A_B` gemäß Spec §5: (1) existierende Keys unter `oxide.a.b`, (2) existierende Keys unter `a.b` (gestrippt), (3) Neuanlage unter `oxide.a.b`. Neue freie Funktion `set_existing(node: &mut Value, segments: &[String], value: Value, depth: usize) -> bool` (privat). Öffentliche API unverändert.

- [ ] **Step 1: Failing Tests schreiben**

Ins Test-Modul von `config.rs`:

```rust
    #[test]
    fn env_var_reaches_app_config_outside_oxide_namespace() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "greeter:\n  name: World\n");
        let env = HashMap::from([("OXIDE_GREETER_NAME".into(), "Rust".into())]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();
        assert_eq!(cfg.get::<String>("greeter.name").unwrap(), "Rust");
        // Nothing was created in the framework namespace:
        assert!(cfg.get_opt::<String>("oxide.greeter.name").unwrap().is_none());
    }

    #[test]
    fn framework_namespace_wins_over_stripped_binding() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml",
            "oxide:\n  greeter:\n    name: framework\ngreeter:\n  name: app\n");
        let env = HashMap::from([("OXIDE_GREETER_NAME".into(), "X".into())]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();
        assert_eq!(cfg.get::<String>("oxide.greeter.name").unwrap(), "X");
        assert_eq!(cfg.get::<String>("greeter.name").unwrap(), "app"); // untouched
    }

    #[test]
    fn stripped_binding_may_create_leaf_in_existing_mapping() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "greeter:\n  name: World\n");
        let env = HashMap::from([("OXIDE_GREETER_EXCITED".into(), "true".into())]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();
        assert_eq!(cfg.get::<bool>("greeter.excited").unwrap(), true);
    }
```

Regressionsanker (bleiben unverändert grün): `env_var_for_unknown_key_creates_one_level_per_segment` (nichts existiert → Neuanlage unter `oxide.*`), `env_var_replaces_scalar_when_descending_through_it_env_wins_by_design`, `env_var_binds_to_hyphenated_key_via_relaxed_matching`.

- [ ] **Step 2: Tests laufen lassen — die 3 neuen müssen fehlschlagen**

Run: `cargo test -p oxide-core`
Expected: 3 neue FAIL (Werte landen heute in `oxide.*`-Totzone), Rest grün.

- [ ] **Step 3: Implementierung**

`apply_env_overrides` umbauen (sortierte Reihenfolge beibehalten):

```rust
fn apply_env_overrides(root: &mut Value, env: &HashMap<String, String>) {
    let mut vars: Vec<(&String, &String)> = env
        .iter()
        .filter(|(name, _)| name.starts_with("OXIDE_") && name.as_str() != "OXIDE_PROFILES_ACTIVE")
        .collect();
    vars.sort_by(|a, b| a.0.cmp(b.0));
    for (name, raw) in vars {
        let segments: Vec<String> = name.split('_').map(|s| s.to_ascii_lowercase()).collect();
        let value: Value = serde_yaml_ng::from_str(raw).unwrap_or_else(|_| Value::String(raw.clone()));
        // 1. Framework namespace against existing keys (`oxide` is the first segment).
        if set_existing(root, &segments, value.clone(), 0) {
            continue;
        }
        // 2. Stripped prefix against existing keys — makes app-level config env-overridable.
        if segments.len() > 1 && set_existing(root, &segments[1..], value.clone(), 0) {
            continue;
        }
        // 3. Nothing matches: create under the framework namespace (pre-2a behavior).
        set_relaxed(root, &segments, value);
    }
}

/// Like `set_relaxed`, but binds only along EXISTING keys (greedy longest match
/// per level). Intermediate levels must already exist; only the final leaf may
/// be newly inserted, and only once at least one existing level was descended
/// (`depth > 0`). Descending through an existing scalar key replaces it with a
/// mapping — env overrides win by design (pinned in Phase 1). Returns whether
/// the value was bound.
fn set_existing(node: &mut Value, segments: &[String], value: Value, depth: usize) -> bool {
    if segments.is_empty() {
        return false;
    }
    let Value::Mapping(map) = node else {
        // Reached via an existing key that holds a scalar: env wins, convert.
        if depth == 0 {
            return false;
        }
        *node = Value::Mapping(Default::default());
        let Value::Mapping(map) = node else { return false };
        if segments.len() == 1 {
            map.insert(Value::String(segments[0].clone()), value);
            return true;
        }
        // Deeper segments below a converted scalar: create the rest.
        let mut current = map;
        for seg in &segments[..segments.len() - 1] {
            let key = Value::String(seg.clone());
            if current.get(&key).is_none() {
                current.insert(key.clone(), Value::Mapping(Default::default()));
            }
            let Some(Value::Mapping(next)) = current.get_mut(&key) else { return false };
            current = next;
        }
        current.insert(Value::String(segments[segments.len() - 1].clone()), value);
        return true;
    };

    for take in (1..=segments.len()).rev() {
        let candidate = normalize(&segments[..take].join(""));
        let existing = map
            .keys()
            .find(|k| matches!(k, Value::String(s) if normalize(s) == candidate))
            .cloned();
        if let Some(key) = existing {
            if take == segments.len() {
                map.insert(key, value);
                return true;
            }
            let child = map.get_mut(&key).expect("key just found");
            return set_existing(child, &segments[take..], value, depth + 1);
        }
    }

    // No existing key matched at this level: the leaf may be created here,
    // but only if we already descended through at least one existing level.
    if segments.len() == 1 && depth > 0 {
        map.insert(Value::String(segments[0].clone()), value);
        return true;
    }
    false
}
```

Hinweis: das `expect("key just found")` folgt dem in Phase 1 etablierten unreachable-by-construction-Muster von `set_relaxed`. Die verschachtelte Skalar-Konvertierung oben ist bewusst konservativ ausgeschrieben; wenn beim Implementieren eine einfachere äquivalente Form entsteht (z. B. Wiederverwendung von `set_relaxed` für den Rest-Pfad nach der Konvertierung), ist das ok — die Tests entscheiden.

README: den "Environment overrides"-Abschnitt ersetzen — neue Aussage: Vars binden zuerst `oxide.*`, dann mit gestripptem Prefix App-Keys (`OXIDE_GREETER_NAME` → `greeter.name`, wenn `greeter:` in der YAML existiert); unbekannte Pfade entstehen unter `oxide.*`.

- [ ] **Step 4: Tests laufen lassen — alle grün**

Run: `cargo test -p oxide-core && cargo test --workspace`
Expected: alle PASS (auch hello-world — dessen `greeter.name` ist jetzt env-overridebar, kein Test hängt an der alten Totzone).

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-core README.md
git commit -m "feat(core): env-binding fallback-stripping reaches app config outside oxide namespace"
```

---

### Task 2: Crate-Scaffolds oxide-web + oxide-web-macros

**Files:**
- Modify: `Cargo.toml` (Members + workspace.dependencies)
- Create: `crates/oxide-web/Cargo.toml`, `crates/oxide-web/src/lib.rs`
- Create: `crates/oxide-web-macros/Cargo.toml`, `crates/oxide-web-macros/src/lib.rs`

**Interfaces:**
- Consumes: Workspace aus Phase 1
- Produces: buildbare Crates; `oxide_web::__private` re-exportiert `axum`, `inventory` und die eigenen Typen (wächst in Tasks 3–5); `oxide-web` re-exportiert später `controller` aus `oxide-web-macros`.

- [ ] **Step 1: Workspace erweitern**

`Cargo.toml` — zu `members` hinzufügen: `"crates/oxide-web"`, `"crates/oxide-web-macros"`. Zu `[workspace.dependencies]`:

```toml
oxide-web = { path = "crates/oxide-web" }
oxide-web-macros = { path = "crates/oxide-web-macros" }
axum = "0.8"
tower = "0.5"
serde_json = "1"
reqwest = { version = "0.12", default-features = false, features = ["json"] }
```

- [ ] **Step 2: Manifeste + leere libs**

`crates/oxide-web/Cargo.toml`:

```toml
[package]
name = "oxide-web"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
oxide-core.workspace = true
oxide-boot.workspace = true
oxide-web-macros.workspace = true
axum.workspace = true
tower.workspace = true
tokio.workspace = true
serde.workspace = true
serde_json.workspace = true
inventory.workspace = true
thiserror.workspace = true
futures.workspace = true
tracing.workspace = true

[dev-dependencies]
reqwest.workspace = true
tempfile.workspace = true
trybuild.workspace = true
```

`crates/oxide-web/src/lib.rs`:

```rust
//! Oxide Boot web starter: annotated controllers on top of axum, RFC 7807
//! error responses, and an HTTP server wired into the boot lifecycle.

/// Internal re-exports used by generated macro code. Not part of the
/// public API; do not use directly.
#[doc(hidden)]
pub mod __private {
    pub use axum;
    pub use inventory;
}
```

`crates/oxide-web-macros/Cargo.toml`: wie `oxide-macros` (proc-macro = true, syn/quote/proc-macro2), Name `oxide-web-macros`.

`crates/oxide-web-macros/src/lib.rs`:

```rust
//! Procedural macros for the Oxide Boot web starter. Generated code only
//! references paths under `::oxide_web::__private`.
```

- [ ] **Step 3: Build prüfen**

Run: `cargo build --workspace`
Expected: kompiliert.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml crates/oxide-web crates/oxide-web-macros
git commit -m "chore(web): scaffold oxide-web and oxide-web-macros crates"
```

---

### Task 3: ProblemDetail + ApiError (RFC 7807)

**Files:**
- Create: `crates/oxide-web/src/error.rs`
- Modify: `crates/oxide-web/src/lib.rs`

**Interfaces:**
- Consumes: axum (`StatusCode`, `IntoResponse`), serde
- Produces (`pub` aus `oxide_web`):
  - `pub struct ProblemDetail { pub problem_type: String /* serde rename "type", default "about:blank" */, pub title: String, pub status: u16, pub detail: Option<String>, pub instance: Option<String> }` (Serialize; `detail`/`instance` mit `skip_serializing_if`)
  - `pub struct ApiError` mit `new(status: StatusCode, title: impl Into<String>)`, Konstruktoren `not_found()`, `bad_request(detail)`, `conflict(detail)`, `internal_server_error()`, Buildern `with_detail(..)`, `with_type(..)`, `with_instance(..)`, Getter `status()`; `impl IntoResponse` → Status + `Content-Type: application/problem+json` + JSON-Body
  - `pub trait IntoProblemDetail { fn into_problem(self) -> ApiError; }` + Blanket `impl<E: IntoProblemDetail> From<E> for ApiError` (damit `?` in Handlern funktioniert; `ApiError` selbst implementiert das Trait NICHT)
  - `pub enum WebError { Bind { port: u16, source: std::io::Error }, ControllerNotRegistered { type_name: &'static str, path: &'static str }, }` (thiserror; wächst bei Bedarf)

- [ ] **Step 1: Failing Tests schreiben**

Test-Modul in `error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    #[test]
    fn problem_detail_serializes_rfc7807_shape_and_skips_empty_fields() {
        let err = ApiError::not_found();
        let json = serde_json::to_value(&err.problem()).unwrap();
        assert_eq!(json["type"], "about:blank");
        assert_eq!(json["title"], "Not Found");
        assert_eq!(json["status"], 404);
        assert!(json.get("detail").is_none());
        assert!(json.get("instance").is_none());
    }

    #[test]
    fn builders_fill_optional_fields() {
        let err = ApiError::bad_request("email is malformed")
            .with_type("https://errors.example.com/validation")
            .with_instance("/api/users");
        let json = serde_json::to_value(&err.problem()).unwrap();
        assert_eq!(json["status"], 400);
        assert_eq!(json["detail"], "email is malformed");
        assert_eq!(json["type"], "https://errors.example.com/validation");
        assert_eq!(json["instance"], "/api/users");
    }

    #[tokio::test]
    async fn into_response_sets_status_and_problem_json_content_type() {
        let res = ApiError::conflict("already exists").into_response();
        assert_eq!(res.status(), StatusCode::CONFLICT);
        assert_eq!(res.headers()["content-type"], "application/problem+json");
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["status"], 409);
        assert_eq!(json["detail"], "already exists");
    }

    #[test]
    fn custom_errors_convert_via_into_problem_detail() {
        enum DomainError { NotFound }
        impl IntoProblemDetail for DomainError {
            fn into_problem(self) -> ApiError {
                match self { DomainError::NotFound => ApiError::not_found() }
            }
        }
        fn fallible() -> Result<(), ApiError> { Err(DomainError::NotFound)? }
        assert_eq!(fallible().unwrap_err().status(), StatusCode::NOT_FOUND);
    }
}
```

Dafür braucht `ApiError` einen Test-sichtbaren Zugriff auf das Problem: `pub fn problem(&self) -> &ProblemDetail`.

- [ ] **Step 2: RED**

Run: `cargo test -p oxide-web`
Expected: Compile-Fehler (Typen existieren nicht). (`tokio`-Dev-Test braucht `tokio`-Macros — ist Workspace-Dep mit `macros`-Feature.)

- [ ] **Step 3: Implementierung**

```rust
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProblemDetail {
    #[serde(rename = "type")]
    pub problem_type: String,
    pub title: String,
    pub status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
}

/// An HTTP error that renders as an RFC 7807 `application/problem+json` body.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    problem: ProblemDetail,
}

impl ApiError {
    pub fn new(status: StatusCode, title: impl Into<String>) -> Self {
        Self {
            status,
            problem: ProblemDetail {
                problem_type: "about:blank".to_string(),
                title: title.into(),
                status: status.as_u16(),
                detail: None,
                instance: None,
            },
        }
    }

    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "Not Found")
    }

    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "Bad Request").with_detail(detail)
    }

    pub fn conflict(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "Conflict").with_detail(detail)
    }

    /// Deliberately carries no detail: internal causes must not leak to clients.
    pub fn internal_server_error() -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.problem.detail = Some(detail.into());
        self
    }

    pub fn with_type(mut self, problem_type: impl Into<String>) -> Self {
        self.problem.problem_type = problem_type.into();
        self
    }

    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.problem.instance = Some(instance.into());
        self
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn problem(&self) -> &ProblemDetail {
        &self.problem
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            [(header::CONTENT_TYPE, "application/problem+json")],
            axum::Json(self.problem),
        )
            .into_response()
    }
}

/// Implement on domain error types to use them with `?` in controller methods.
pub trait IntoProblemDetail {
    fn into_problem(self) -> ApiError;
}

impl<E: IntoProblemDetail> From<E> for ApiError {
    fn from(e: E) -> Self {
        e.into_problem()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WebError {
    #[error("cannot bind HTTP server to port {port}: {source}")]
    Bind { port: u16, #[source] source: std::io::Error },
    #[error("controller '{type_name}' (route '{path}') is not registered as a component — add #[component] to the struct")]
    ControllerNotRegistered { type_name: &'static str, path: &'static str },
}
```

`lib.rs`: `pub mod error; pub use error::{ApiError, IntoProblemDetail, ProblemDetail, WebError};`

Falls der Blanket-`From` mit einer Std-Impl kollidiert (Compiler entscheidet): auf `impl IntoProblemDetail`-Konvertierung per explizitem `map_err` im Macro ausweichen und die Signaturänderung im Report flaggen — NICHT stillschweigend eine andere öffentliche API bauen.

- [ ] **Step 4: GREEN**

Run: `cargo test -p oxide-web`
Expected: 4 Tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-web
git commit -m "feat(web): RFC 7807 ProblemDetail and ApiError with IntoProblemDetail conversion"
```

---

### Task 4: ServerProps + WebServer-Lifecycle + Basis-Health

**Files:**
- Create: `crates/oxide-web/src/server.rs`, `crates/oxide-web/src/health.rs`
- Modify: `crates/oxide-web/src/lib.rs`
- Test: `crates/oxide-web/tests/server.rs`

**Interfaces:**
- Consumes: `ComponentDescriptor`/`ContextBuilder`/`Context`/`Inject` aus oxide-core, `OxideApplication` aus oxide-boot (Tests), `WebError`
- Produces:
  - `pub struct ServerProps { pub port: u16 /* default 8080 */, pub shutdown_grace_seconds: u64 /* default 30 */ }` (Deserialize + Default), gebunden an `oxide.server` — handgeschriebener Descriptor mit `config.get_opt("oxide.server")?.unwrap_or_default()` (Abschnitt darf fehlen; das kann der `ConfigProperties`-Derive nicht — als bekannte Lücke im Report nennen)
  - `pub struct WebServer` (Component, handgeschriebener Descriptor) mit `pub fn local_addr(&self) -> Option<std::net::SocketAddr>` und intern `async fn start(&self, router: axum::Router) -> ComponentResult<()>`, `async fn stop_server(&self) -> ComponentResult<()>`; Readiness-Flag `Arc<AtomicBool>`, Zugriff für health: `pub(crate) fn readiness(&self) -> Arc<AtomicBool>`
  - Lifecycle: `post_construct` baut den Router SYNCHRON im Hook-Closure (`crate::routing::build_router(ctx)` — Task 5; in DIESEM Task noch `Router::new()` + Health-Routen direkt), bindet `0.0.0.0:port`, spawnt `axum::serve(...).with_graceful_shutdown(oneshot)`, setzt Readiness. `pre_destroy`: Readiness runter, Shutdown-Signal, `tokio::time::timeout(grace, handle)` und Ausgang loggen.
  - `health.rs`: `pub(crate) fn health_routes(readiness: Arc<AtomicBool>) -> axum::Router` — `/actuator/health` und `/actuator/health/readiness`: `{"status":"UP"}`/200 wenn ready, sonst `{"status":"OUT_OF_SERVICE"}`/503; `/actuator/health/liveness`: immer `{"status":"UP"}`/200.

- [ ] **Step 1: Failing Test schreiben**

`crates/oxide-web/tests/server.rs`:

```rust
use oxide_boot::prelude::*;
use oxide_web::WebServer;
use std::collections::HashMap;

static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: web-test\n  server:\n    port: 0\n",
    )
    .unwrap();
    dir
}

#[tokio::test]
async fn boots_serves_health_and_shuts_down() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    let server = app.context().get::<WebServer>().unwrap();
    let addr = server.local_addr().expect("server started");
    let base = format!("http://{addr}");

    let live: serde_json::Value =
        reqwest::get(format!("{base}/actuator/health/liveness")).await.unwrap().json().await.unwrap();
    assert_eq!(live["status"], "UP");

    let ready = reqwest::get(format!("{base}/actuator/health")).await.unwrap();
    assert_eq!(ready.status(), 200);

    app.stop().await.unwrap();
    assert!(reqwest::get(format!("{base}/actuator/health")).await.is_err(), "server must be down after stop");
}
```

Dev-Dependencies von oxide-web um `serde_json.workspace = true` ergänzen, falls über die normalen Deps nicht sichtbar.

- [ ] **Step 2: RED**

Run: `cargo test -p oxide-web --test server`
Expected: Compile-Fehler (`WebServer` existiert nicht).

- [ ] **Step 3: Implementierung**

`server.rs` (Kernstücke — vollständig ausschreiben):

```rust
use oxide_core::{ComponentDescriptor, ComponentResult, Context};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

fn default_port() -> u16 { 8080 }
fn default_grace() -> u64 { 30 }

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ServerProps {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(rename = "shutdown-grace-seconds", default = "default_grace")]
    pub shutdown_grace_seconds: u64,
}

impl Default for ServerProps {
    fn default() -> Self {
        Self { port: default_port(), shutdown_grace_seconds: default_grace() }
    }
}

struct ServerRuntime {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
    local_addr: Option<SocketAddr>,
}

pub struct WebServer {
    props: ServerProps,
    readiness: Arc<AtomicBool>,
    runtime: Mutex<ServerRuntime>,
    // local_addr readable without awaiting the runtime lock:
    addr: std::sync::Mutex<Option<SocketAddr>>,
}

impl WebServer {
    fn new(props: ServerProps) -> Self {
        Self {
            props,
            readiness: Arc::new(AtomicBool::new(false)),
            runtime: Mutex::new(ServerRuntime { shutdown: None, handle: None, local_addr: None }),
            addr: std::sync::Mutex::new(None),
        }
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        *self.addr.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn readiness(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.readiness)
    }

    async fn start(&self, router: axum::Router) -> ComponentResult<()> {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", self.props.port))
            .await
            .map_err(|source| crate::WebError::Bind { port: self.props.port, source })?;
        let local_addr = listener.local_addr().map_err(|source| crate::WebError::Bind { port: self.props.port, source })?;
        let (tx, rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = rx.await;
                })
                .await
        });
        {
            let mut rt = self.runtime.lock().await;
            rt.shutdown = Some(tx);
            rt.handle = Some(handle);
            rt.local_addr = Some(local_addr);
        }
        *self.addr.lock().unwrap_or_else(|p| p.into_inner()) = Some(local_addr);
        self.readiness.store(true, Ordering::SeqCst);
        tracing::info!(port = local_addr.port(), "HTTP server listening");
        Ok(())
    }

    async fn stop_server(&self) -> ComponentResult<()> {
        self.readiness.store(false, Ordering::SeqCst);
        let (shutdown, handle) = {
            let mut rt = self.runtime.lock().await;
            (rt.shutdown.take(), rt.handle.take())
        };
        if let Some(tx) = shutdown {
            let _ = tx.send(());
        }
        if let Some(handle) = handle {
            let grace = std::time::Duration::from_secs(self.props.shutdown_grace_seconds);
            match tokio::time::timeout(grace, handle).await {
                Ok(join) => {
                    join.map_err(|e| format!("server task panicked: {e}"))??;
                    tracing::info!("HTTP server stopped");
                }
                Err(_) => {
                    tracing::warn!(grace_seconds = self.props.shutdown_grace_seconds, "HTTP server drain timed out");
                }
            }
        }
        Ok(())
    }
}

inventory::submit! {
    ComponentDescriptor {
        type_name: "ServerProps",
        type_id: || std::any::TypeId::of::<ServerProps>(),
        deps: || vec![(std::any::TypeId::of::<oxide_core::AppConfig>(), "AppConfig")],
        construct: |b| {
            let config = b.resolve::<oxide_core::AppConfig>()?;
            let props: ServerProps = config
                .get_opt("oxide.server")
                .map_err(|e| oxide_core::ContextError::Construct {
                    type_name: "ServerProps",
                    message: format!("binding prefix 'oxide.server': {e}"),
                })?
                .unwrap_or_default();
            b.insert(props);
            Ok(())
        },
        post_construct: None,
        pre_destroy: None,
    }
}

inventory::submit! {
    ComponentDescriptor {
        type_name: "WebServer",
        type_id: || std::any::TypeId::of::<WebServer>(),
        deps: || vec![(std::any::TypeId::of::<ServerProps>(), "ServerProps")],
        construct: |b| {
            let props = b.resolve::<ServerProps>()?;
            b.insert(WebServer::new((*props).clone()));
            Ok(())
        },
        post_construct: Some(|ctx: &Context| {
            let router = crate::routing::build_router(ctx); // Task 5; in Task 4 noch: Ok(Router::new()) + health
            let me = ctx.get::<WebServer>().expect("component was just constructed");
            Box::pin(async move { me.start(router?).await })
        }),
        pre_destroy: Some(|ctx: &Context| {
            let me = ctx.get::<WebServer>().expect("component exists during shutdown");
            Box::pin(async move { me.stop_server().await })
        }),
    }
}
```

In DIESEM Task existiert `routing.rs` noch nicht — der Hook baut den Router direkt: `let router = crate::health::health_routes(me_readiness)` (Reihenfolge im Closure: erst `me` holen, dann Router mit `me.readiness()` bauen, dann async-Block). In Task 5 wird das auf `build_router(ctx)` umgestellt.

`health.rs`:

```rust
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn status_body(up: bool) -> (StatusCode, Json<serde_json::Value>) {
    if up {
        (StatusCode::OK, Json(serde_json::json!({ "status": "UP" })))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({ "status": "OUT_OF_SERVICE" })))
    }
}

pub(crate) fn health_routes(readiness: Arc<AtomicBool>) -> Router {
    let r1 = Arc::clone(&readiness);
    Router::new()
        .route("/actuator/health", get(move || {
            let r = Arc::clone(&r1);
            async move { status_body(r.load(Ordering::SeqCst)) }
        }))
        .route("/actuator/health/liveness", get(|| async { status_body(true) }))
        .route("/actuator/health/readiness", get(move || {
            let r = Arc::clone(&readiness);
            async move { status_body(r.load(Ordering::SeqCst)) }
        }))
}
```

`lib.rs`: Module + `pub use server::{ServerProps, WebServer};` + im `__private` zusätzlich `pub use crate::{...}` nach Bedarf der Macros (Task 6).

- [ ] **Step 4: GREEN**

Run: `cargo test -p oxide-web --test server && cargo test --workspace`
Expected: PASS, keine Regressionen.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-web
git commit -m "feat(web): WebServer lifecycle component with graceful shutdown and health endpoints"
```

---

### Task 5: RouteDescriptor + Router-Assembly

**Files:**
- Create: `crates/oxide-web/src/routing.rs`
- Modify: `crates/oxide-web/src/server.rs` (Hook nutzt `build_router`), `crates/oxide-web/src/lib.rs`
- Test: `crates/oxide-web/tests/server.rs` (erweitern)

**Interfaces:**
- Consumes: `Context`, `WebServer::readiness`, `health_routes`, `WebError`
- Produces:
  - `pub struct RouteDescriptor { pub method: &'static str, pub path: &'static str, pub register: fn(&Context, axum::Router) -> Result<axum::Router, WebError> }` + `inventory::collect!(RouteDescriptor)`
  - `pub(crate) fn build_router(ctx: &Context) -> Result<axum::Router, WebError>` — sammelt alle Descriptors, sortiert nach `(path, method)` (deterministisch), registriert sie, merged Health-Routen (Readiness aus `ctx.get::<WebServer>()`), loggt jede Route mit `tracing::info!(method, path, "route mounted")`

- [ ] **Step 1: Failing Test schreiben**

In `tests/server.rs` (handgeschriebener Descriptor, Muster wie Phase-1-Task 6 — Macro kommt erst in Task 6):

```rust
use oxide_web::{RouteDescriptor, WebError};
use oxide_boot::__private::ComponentDescriptor;

struct EchoService { prefix: String }

inventory::submit! {
    ComponentDescriptor {
        type_name: "EchoService",
        type_id: || std::any::TypeId::of::<EchoService>(),
        deps: || vec![],
        construct: |b| { b.insert(EchoService { prefix: "echo:".into() }); Ok(()) },
        post_construct: None,
        pre_destroy: None,
    }
}

inventory::submit! {
    RouteDescriptor {
        method: "GET",
        path: "/echo/{msg}",
        register: |ctx, router| {
            let svc = ctx.get::<EchoService>().ok_or(WebError::ControllerNotRegistered {
                type_name: "EchoService",
                path: "/echo/{msg}",
            })?;
            Ok(router.route("/echo/{msg}", axum::routing::get(move |axum::extract::Path(msg): axum::extract::Path<String>| {
                let svc = svc.clone();
                async move { format!("{}{}", svc.prefix, msg) }
            })))
        },
    }
}

#[tokio::test]
async fn hand_written_route_descriptor_is_mounted_and_resolves_components() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();
    let body = reqwest::get(format!("http://{addr}/echo/hi")).await.unwrap().text().await.unwrap();
    assert_eq!(body, "echo:hi");
    app.stop().await.unwrap();
}
```

Dazu `inventory.workspace = true` in die Dev-Dependencies von oxide-web.

- [ ] **Step 2: RED**

Run: `cargo test -p oxide-web --test server`
Expected: Compile-Fehler (`RouteDescriptor` existiert nicht).

- [ ] **Step 3: Implementierung**

`routing.rs`:

```rust
use crate::{health, server::WebServer, WebError};
use axum::Router;
use oxide_core::Context;

/// One HTTP route contributed by a controller (usually macro-generated).
/// `register` resolves the owning controller from the context ONCE at router
/// build time and moves the cloned handle into the axum handler closure.
pub struct RouteDescriptor {
    pub method: &'static str,
    pub path: &'static str,
    pub register: fn(&Context, Router) -> Result<Router, WebError>,
}

inventory::collect!(RouteDescriptor);

pub(crate) fn build_router(ctx: &Context) -> Result<Router, WebError> {
    let mut routes: Vec<&RouteDescriptor> = inventory::iter::<RouteDescriptor>().collect();
    routes.sort_by_key(|r| (r.path, r.method));
    let mut router = Router::new();
    for route in routes {
        router = (route.register)(ctx, router)?;
        tracing::info!(method = route.method, path = route.path, "route mounted");
    }
    if let Some(server) = ctx.get::<WebServer>() {
        router = router.merge(health::health_routes(server.readiness()));
    }
    Ok(router)
}
```

`server.rs`-Hook auf `crate::routing::build_router(ctx)` umstellen (die Task-4-Direktvariante ersetzen). `lib.rs`: `pub mod routing; pub use routing::RouteDescriptor;`

- [ ] **Step 4: GREEN**

Run: `cargo test -p oxide-web --test server && cargo test --workspace`
Expected: PASS (beide server-Tests; Health weiter grün, jetzt über build_router gemountet).

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-web
git commit -m "feat(web): RouteDescriptor registry with deterministic router assembly"
```

---

### Task 6: `#[controller]`-Macro

**Files:**
- Create: `crates/oxide-web-macros/src/controller.rs`
- Modify: `crates/oxide-web-macros/src/lib.rs`, `crates/oxide-web/src/lib.rs` (Re-Export + `__private`-Erweiterung)
- Test: `crates/oxide-web/tests/server.rs` (erweitern), `crates/oxide-web/tests/ui.rs` + `tests/ui/*`

**Interfaces:**
- Consumes: `RouteDescriptor`, `WebError`, `Context::get`, axum via `::oxide_web::__private::axum`
- Produces:
  - `#[controller("/base/path")]` (Arg optional, Default `""`) auf einem `impl`-Block. Methoden mit GENAU EINEM Routen-Attribut `#[get("...")]`/`#[post]`/`#[put]`/`#[delete]`/`#[patch]` werden zu Routen; Methoden ohne Routen-Attribut bleiben unberührte Helfer. Routen-Attribute werden aus dem Output gestrippt.
  - Methoden-Signaturvertrag: `async fn name(&self, param: ExtractorType, ...) -> R` — Parameter sind PLAIN `ident: Type` (Axum-Extractors wie `Path<T>`, `Query<T>`, `Json<T>`; Body-Extractor zuletzt, das erzwingt Axum selbst); Destructuring-Patterns → Compile-Fehler „controller method parameters must be plain `name: Type` bindings". Rückgabetyp muss `IntoResponse` erfüllen (typisch `Result<Json<T>, ApiError>` — Axum akzeptiert `Result` mit beiden Seiten `IntoResponse`).
  - Pro Routen-Methode ein `inventory::submit!` eines `RouteDescriptor` mit `register`-Closure: `ctx.get::<SelfTy>()` → `WebError::ControllerNotRegistered` bei Fehlen; Handler-Closure cloned das `Inject`-Handle und ruft die Methode.
  - Pfad-Join: `join_paths("/api/users", "/{id}") == "/api/users/{id}"`, `join_paths("/api/users", "") == "/api/users"`, doppelte Slashes normalisiert — pure Funktion mit Unit-Tests IM Macro-Crate.
  - `oxide-web` re-exportiert: `pub use oxide_web_macros::controller;` (+ prelude). `__private` erweitert um `pub use crate::{RouteDescriptor, WebError}; pub use oxide_core::Context;`
  - trybuild: (1) Destructuring-Pattern-Param → erwarteter Fehler, (2) `#[controller]` auf einer fn statt impl → „#[controller] must be applied to an impl block".

- [ ] **Step 1: Failing Integrationstest schreiben**

In `tests/server.rs`:

```rust
use oxide_boot::component;
use oxide_web::{controller, ApiError};
use axum::extract::{Json, Path};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct Note { id: u32, text: String }

#[component]
struct NotesController {
    #[default]
    store: std::sync::Mutex<Vec<Note>>,
}

#[controller("/api/notes")]
impl NotesController {
    #[post("")]
    async fn create(&self, body: Json<Note>) -> Result<Json<Note>, ApiError> {
        let note = body.0.clone();
        self.store.lock().unwrap_or_else(|p| p.into_inner()).push(body.0);
        Ok(Json(note))
    }

    #[get("/{id}")]
    async fn get_by_id(&self, id: Path<u32>) -> Result<Json<Note>, ApiError> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .find(|n| n.id == *id)
            .cloned()
            .map(Json)
            .ok_or_else(ApiError::not_found)
    }
}

#[tokio::test]
async fn controller_macro_mounts_routes_and_returns_problem_json_on_error() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();
    let base = format!("http://{addr}/api/notes");
    let client = reqwest::Client::new();

    let created: serde_json::Value = client
        .post(&base)
        .json(&serde_json::json!({ "id": 1, "text": "hello" }))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(created["text"], "hello");

    let fetched: serde_json::Value = client.get(format!("{base}/1")).send().await.unwrap().json().await.unwrap();
    assert_eq!(fetched["id"], 1);

    let missing = client.get(format!("{base}/999")).send().await.unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(missing.headers()["content-type"], "application/problem+json");
    let problem: serde_json::Value = missing.json().await.unwrap();
    assert_eq!(problem["title"], "Not Found");
    assert_eq!(problem["status"], 404);

    app.stop().await.unwrap();
}
```

WICHTIG (prozess-globales inventory): `NotesController` bootet ab jetzt in ALLEN Tests dieses Binaries — er hat aber nur `#[default]`-State und braucht keine Config-Keys, die geteilte `test_config_dir()` bleibt unverändert gültig. Der Mutex-Zugriff im Store nutzt `unwrap_or_else(|p| p.into_inner())` statt `unwrap()` — Testcode, aber konsistent mit dem Library-Stil.

- [ ] **Step 2: RED**

Run: `cargo test -p oxide-web --test server`
Expected: Compile-Fehler (`controller` existiert nicht).

- [ ] **Step 3: Macro implementieren**

`crates/oxide-web-macros/src/controller.rs` — Struktur:

```rust
use proc_macro2::TokenStream;
use quote::quote;
use syn::{FnArg, ImplItem, ItemImpl, LitStr, Pat};

const ROUTE_ATTRS: [(&str, &str); 5] =
    [("get", "GET"), ("post", "POST"), ("put", "PUT"), ("delete", "DELETE"), ("patch", "PATCH")];

pub fn join_paths(base: &str, path: &str) -> String {
    let joined = format!("{}/{}", base.trim_end_matches('/'), path.trim_start_matches('/'));
    let joined = joined.trim_end_matches('/').to_string();
    if joined.is_empty() { "/".to_string() } else { joined }
}

pub fn expand(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let base_path = if args.is_empty() {
        String::new()
    } else {
        syn::parse2::<LitStr>(args)?.value()
    };
    let mut item: ItemImpl = syn::parse2(item).map_err(|e| {
        syn::Error::new(e.span(), "#[controller] must be applied to an impl block")
    })?;
    let self_ty = item.self_ty.clone();
    let mut registrations = Vec::new();

    for impl_item in item.items.iter_mut() {
        let ImplItem::Fn(method) = impl_item else { continue };
        // find exactly one route attribute
        let mut route: Option<(usize, &'static str, String)> = None;
        for (idx, attr) in method.attrs.iter().enumerate() {
            for (name, verb) in ROUTE_ATTRS {
                if attr.path().is_ident(name) {
                    if route.is_some() {
                        return Err(syn::Error::new_spanned(attr, "only one route attribute per method"));
                    }
                    let sub_path: String = match &attr.meta {
                        syn::Meta::Path(_) => String::new(),
                        _ => attr.parse_args::<LitStr>()?.value(),
                    };
                    route = Some((idx, verb, sub_path));
                }
            }
        }
        let Some((attr_idx, verb, sub_path)) = route else { continue };
        method.attrs.remove(attr_idx);

        // collect plain `ident: Type` params (skip &self)
        let mut params = Vec::new();
        for arg in method.sig.inputs.iter().skip(1) {
            let FnArg::Typed(pat_ty) = arg else { continue };
            let Pat::Ident(ident) = &*pat_ty.pat else {
                return Err(syn::Error::new_spanned(
                    &pat_ty.pat,
                    "controller method parameters must be plain `name: Type` bindings",
                ));
            };
            params.push((ident.ident.clone(), (*pat_ty.ty).clone()));
        }

        let full_path = join_paths(&base_path, &sub_path);
        let method_ident = method.sig.ident.clone();
        let axum_verb = syn::Ident::new(&verb.to_lowercase(), proc_macro2::Span::call_site());
        let param_names: Vec<_> = params.iter().map(|(n, _)| n).collect();
        let param_decls: Vec<_> = params.iter().map(|(n, t)| quote! { #n: #t }).collect();

        registrations.push(quote! {
            ::oxide_web::__private::inventory::submit! {
                ::oxide_web::RouteDescriptor {
                    method: #verb,
                    path: #full_path,
                    register: |__ctx, __router| {
                        let __this = __ctx.get::<#self_ty>().ok_or(
                            ::oxide_web::WebError::ControllerNotRegistered {
                                type_name: stringify!(#self_ty),
                                path: #full_path,
                            },
                        )?;
                        ::core::result::Result::Ok(__router.route(
                            #full_path,
                            ::oxide_web::__private::axum::routing::#axum_verb(
                                move |#(#param_decls),*| {
                                    let __this = __this.clone();
                                    async move { __this.#method_ident(#(#param_names),*).await }
                                },
                            ),
                        ))
                    },
                }
            }
        });
    }

    Ok(quote! {
        #item
        #(#registrations)*
    })
}

#[cfg(test)]
mod tests {
    use super::join_paths;

    #[test]
    fn joins_base_and_subpath() {
        assert_eq!(join_paths("/api/users", "/{id}"), "/api/users/{id}");
        assert_eq!(join_paths("/api/users", ""), "/api/users");
        assert_eq!(join_paths("/api/users/", "/{id}"), "/api/users/{id}");
        assert_eq!(join_paths("", ""), "/");
    }
}
```

`oxide-web-macros/src/lib.rs`:

```rust
mod controller;

use proc_macro::TokenStream;

/// Turns an impl block into HTTP routes: methods annotated with
/// #[get]/#[post]/#[put]/#[delete]/#[patch] are mounted on the web server.
#[proc_macro_attribute]
pub fn controller(args: TokenStream, item: TokenStream) -> TokenStream {
    controller::expand(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
```

`oxide-web/src/lib.rs`: `pub use oxide_web_macros::controller;`, `__private` ergänzen:

```rust
#[doc(hidden)]
pub mod __private {
    pub use axum;
    pub use inventory;
    pub use oxide_core::Context;
}
```

(`RouteDescriptor`/`WebError` sind bereits `pub` auf Crate-Ebene — das Macro referenziert sie als `::oxide_web::RouteDescriptor`/`::oxide_web::WebError`.)

trybuild-Fälle (`tests/ui.rs` analog Phase 1; Runner `t.compile_fail("tests/ui/*.rs")`):

`tests/ui/destructured_param.rs`:

```rust
use oxide_boot::component;
use oxide_web::controller;
use axum::extract::Path;

#[component]
struct C {}

#[controller("/x")]
impl C {
    #[get("/{id}")]
    async fn broken(&self, Path(id): Path<String>) -> String { id }
}

fn main() {}
```

`tests/ui/controller_on_fn.rs`:

```rust
use oxide_web::controller;

#[controller("/x")]
async fn not_an_impl() {}

fn main() {}
```

`TRYBUILD=overwrite cargo test -p oxide-web --test ui` → .stderr generieren, MANUELL prüfen (enthalten „plain `name: Type` bindings" bzw. „must be applied to an impl block"), committen.

- [ ] **Step 4: GREEN**

Run: `cargo test -p oxide-web && cargo test --workspace`
Expected: alle PASS (server-Tests inkl. Controller-E2E, ui-Tests, keine Regressionen).

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-web crates/oxide-web-macros
git commit -m "feat(web): #[controller] macro mounting annotated impl methods as axum routes"
```

---

### Task 7: Drain-Verhalten + BootError-Pfad-Tests

**Files:**
- Test: `crates/oxide-web/tests/server.rs` (Drain-Test)
- Test: `crates/oxide-boot/tests/boot_failure.rs` (NEUES eigenes Binary — der absichtlich fehlschlagende Component darf NICHT im boot.rs-Binary registriert sein, sonst bricht er dort jeden Boot)

**Interfaces:**
- Consumes: alles Bisherige
- Produces: Nachweis (deferred-Item aus Phase 1), dass (a) laufende Requests beim Shutdown zu Ende bedient werden, (b) ein Fehler in `post_construct` als `BootError::PostConstruct { component, .. }` mit Component-Namen ankommt.

- [ ] **Step 1: Drain-Test (failing nur, falls Verhalten kaputt — Verifikationstest)**

In `tests/server.rs` einen Slow-Controller + Test ergänzen:

```rust
#[component]
struct SlowController {}

#[controller("/slow")]
impl SlowController {
    #[get("")]
    async fn slow(&self) -> &'static str {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        "done"
    }
}

#[tokio::test]
async fn graceful_shutdown_drains_in_flight_requests() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();

    let in_flight = tokio::spawn(reqwest::get(format!("http://{addr}/slow")));
    tokio::time::sleep(std::time::Duration::from_millis(100)).await; // request is on the wire
    app.stop().await.unwrap(); // must wait for the drain

    let response = in_flight.await.unwrap().unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "done");
}
```

- [ ] **Step 2: BootError-Test (eigenes Binary)**

`crates/oxide-boot/tests/boot_failure.rs`:

```rust
use oxide_boot::prelude::*;
use oxide_boot::component;
use std::collections::HashMap;

#[component(post_construct = explode)]
struct FailingComponent {}

impl FailingComponent {
    async fn explode(&self) -> ComponentResult<()> {
        Err("intentional startup failure".into())
    }
}

#[tokio::test]
async fn failing_post_construct_surfaces_as_boot_error_with_component_name() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), "oxide: {}\n").unwrap();
    let err = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
        .start()
        .await
        .unwrap_err();
    match err {
        BootError::PostConstruct { component, message } => {
            assert_eq!(component, "FailingComponent");
            assert!(message.contains("intentional startup failure"), "was: {message}");
        }
        other => panic!("expected PostConstruct error, got: {other}"),
    }
}
```

- [ ] **Step 3: Tests laufen lassen**

Run: `cargo test -p oxide-web --test server && cargo test -p oxide-boot --test boot_failure && cargo test --workspace`
Expected: alle PASS. Schlägt der Drain-Test fehl, ist das ein ECHTER Befund am Task-4-Lifecycle — dann dort fixen, nicht den Test aufweichen.

- [ ] **Step 4: Commit**

```bash
git add crates/oxide-web crates/oxide-boot
git commit -m "test(web,boot): graceful-shutdown drain and post_construct failure surfacing"
```

---

### Task 8: Beispiel `rest-api` + README-Update

**Files:**
- Modify: `Cargo.toml` (Member `examples/rest-api`)
- Create: `examples/rest-api/Cargo.toml`, `examples/rest-api/application.yml`, `examples/rest-api/src/lib.rs`, `examples/rest-api/src/main.rs`, `examples/rest-api/tests/api.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: gesamte 2a-API
- Produces: lauffähiges Referenzbeispiel einer REST-API; README-Abschnitt „Web" mit Controller-Snippet und aktualisierter Cheat-Sheet-Tabelle (+ Zeilen `@RestController` → `#[controller]`, `@GetMapping` → `#[get]`, `ResponseEntity`-Fehler → `ApiError`, Actuator-Health → `/actuator/health`).

- [ ] **Step 1: Beispiel anlegen**

`examples/rest-api/Cargo.toml` (analog hello-world plus `oxide-web.workspace = true`, `axum.workspace = true`, `serde.workspace = true`; dev: `tokio`, `reqwest`, `serde_json`).

`examples/rest-api/application.yml`:

```yaml
oxide:
  application:
    name: rest-api
  server:
    port: 8080
  logging:
    level:
      root: info
```

`examples/rest-api/src/lib.rs`:

```rust
use oxide_boot::prelude::*;
use oxide_boot::component;
use oxide_web::{controller, ApiError};
use axum::extract::{Json, Path};
use std::sync::Mutex;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Note {
    pub id: u32,
    pub text: String,
}

#[component]
pub struct NoteStore {
    #[default]
    notes: Mutex<Vec<Note>>,
}

impl NoteStore {
    pub fn add(&self, note: Note) -> Note {
        let mut notes = self.notes.lock().unwrap_or_else(|p| p.into_inner());
        notes.push(note.clone());
        note
    }

    pub fn find(&self, id: u32) -> Option<Note> {
        self.notes.lock().unwrap_or_else(|p| p.into_inner()).iter().find(|n| n.id == id).cloned()
    }

    pub fn all(&self) -> Vec<Note> {
        self.notes.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

#[component]
pub struct NotesController {
    store: NoteStore,
}

#[controller("/api/notes")]
impl NotesController {
    #[get("")]
    async fn list(&self) -> Json<Vec<Note>> {
        Json(self.store.all())
    }

    #[post("")]
    async fn create(&self, body: Json<Note>) -> Json<Note> {
        Json(self.store.add(body.0))
    }

    #[get("/{id}")]
    async fn get_by_id(&self, id: Path<u32>) -> Result<Json<Note>, ApiError> {
        self.store.find(*id).map(Json).ok_or_else(ApiError::not_found)
    }
}
```

`examples/rest-api/src/main.rs` (wie hello-world: load-bearing Import mit Begründungskommentar, `#[oxide_boot::main]`, `OxideApplication::run()`, eprintln+exit(1) bei Fehler).

`examples/rest-api/tests/api.rs`:

```rust
use oxide_boot::prelude::*;
use oxide_web::WebServer;
use rest_api::Note;
use std::collections::HashMap;

#[tokio::test]
async fn create_fetch_and_missing_note_flow() {
    let env = HashMap::from([("OXIDE_SERVER_PORT".to_string(), "0".to_string())]); // env override, dogfooded
    let app = OxideApplication::builder().base_dir(env!("CARGO_MANIFEST_DIR")).env(env).start().await.unwrap();
    let addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();
    let base = format!("http://{addr}/api/notes");
    let client = reqwest::Client::new();

    client.post(&base).json(&Note { id: 7, text: "first".into() }).send().await.unwrap();
    let fetched: Note = client.get(format!("{base}/7")).send().await.unwrap().json().await.unwrap();
    assert_eq!(fetched.text, "first");

    let missing = client.get(format!("{base}/8")).send().await.unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(missing.headers()["content-type"], "application/problem+json");

    app.stop().await.unwrap();
}
```

- [ ] **Step 2: Test + manueller Lauf**

Run: `cargo test -p rest-api`
Expected: PASS.

Run (manuell, macOS ohne timeout-Binary): `cargo run -p` geht nicht vom Root (yml-Discovery per cwd) — also `cd examples/rest-api && cargo run` im Hintergrund starten, mit `curl http://localhost:8080/actuator/health` und `curl http://localhost:8080/api/notes` prüfen, dann `kill -TERM`, Shutdown-Logs verifizieren. Output ins Report.

- [ ] **Step 3: README aktualisieren**

Neuer Abschnitt „## Web (oxide-web)" nach dem Quick start: 3-Satz-Intro (Starter-Prinzip: `oxide-web` als Dependency = Server, Health-Endpoints und Controller-Mounting sind da), das `NotesController`-Snippet aus dem Beispiel (gekürzt auf `#[controller]` + eine `#[get]`-Methode), Hinweis auf `cd examples/rest-api && cargo run`. Cheat-Sheet-Tabelle um die vier Web-Zeilen ergänzen (siehe Interfaces).

- [ ] **Step 4: Workspace-Gate**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: alles grün/clean (Clippy-Funde ordentlich fixen; gezielte `#[allow]` nur mit Begründungskommentar).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml examples/rest-api README.md
git commit -m "feat: rest-api example with notes controller and web README section"
```

---

## Abschluss-Check Phase 2a

- [ ] `cargo test --workspace` grün, `cargo clippy --workspace --all-targets -- -D warnings` sauber
- [ ] `cd examples/rest-api && cargo run` → Health 200, POST/GET Notes funktionieren, 404 liefert `application/problem+json`, SIGTERM drained und stoppt sauber
- [ ] Spec-Abgleich §6: abgedeckt sind Controller/Extractors/RFC 7807/Graceful Shutdown/Basis-Health; BEWUSST auf 2b/3 verschoben: Prometheus-/Info-Actuator, Management-Port, Health-Indicators aus Integrationen, Request-Validierung, CORS/Compression/Timeout-Middleware, `Header<T>`-Extractor
- [ ] Spec-Abgleich §5: Env-Binding-Regel (Fallback-Stripping) implementiert und README-dokumentiert
- [ ] Bekannte bewusste Lücken dokumentieren: `ConfigProperties` kann keine optionalen Prefixe (ServerProps ist handverdrahtet — Kandidat für `#[config(prefix, default)]` in 2b); Readiness wird beim Server-Start `true` (nicht erst nach ALLEN post_constructs — 2b-Thema zusammen mit Hook-Ordnung)
- [ ] Danach: Plan für Phase 2b (Observability: Metriken, OTEL-Tracing, Prometheus-Actuator, Management-Port) mit den Erkenntnissen aus 2a
