# Oxide Boot Phase 2b (Observability) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `oxide-observability`-Starter (automatische HTTP-Metriken mit Micrometer-Namen, OTEL-Tracing mit W3C-Propagation, JSON-Logs) + Management-Server mit `/actuator/prometheus`, `/actuator/info` und dem Health-Split — plus die aus dem 2a-Final-Review vorgemerkten Fundament-Härtungen (Descriptor-Builder, `#[config(default)]`, Macro-Guards).

**Architecture:** Drei Erweiterungs-Seams machen die Starter komponierbar, alle nach dem etablierten inventory-Muster: (1) `LoggingInitializer` in oxide-boot (höchste Priorität gewinnt; oxide-observability ersetzt damit das Basis-Logging durch EnvFilter + JSON/Pretty + OTEL-Layer), (2) `RouterLayer` in oxide-web (build_router wendet gesammelte Middleware an; Observability hängt hier HTTP-Metriken + Request-Spans ein), (3) `ManagementRoute` in oxide-web (Management-Router sammelt Actuator-Routen; Observability steuert `/actuator/prometheus` bei). Der `ComponentDescriptor` wird `#[non_exhaustive]` mit Builder — künftige Felder brechen keine User-Crates mehr. Health zieht vom Haupt- auf den Management-Port (Spec §6: separater Port, Default 8081).

**Tech Stack:** `metrics` + `metrics-exporter-prometheus` (Handle-Render, kein eigener Listener), `opentelemetry`/`opentelemetry_sdk`/`opentelemetry-otlp` (grpc-tonic) + `tracing-opentelemetry`, bestehender Stack. Versions-Vorgaben unten sind Startpunkte — bei API-Drift der OTEL-Crates gilt: an die resolved Version anpassen, die Tests sind der Vertrag; NIEMALS die Semantik (Namen/Labels/Propagation) ändern.

## Global Constraints

- Alles aus Phase 1/2a gilt weiter (Edition 2024, Versionen nur in `[workspace.dependencies]`, thiserror/kein anyhow, kein `unwrap()`/`expect()` in Library-Code außer geflaggten unreachable-by-construction-Stellen und `unwrap_or_else(|p| p.into_inner())` bei Mutex-Poisoning, Englisch, Conventional Commits, TDD mit ROHEM RED/GREEN-Output im Report).
- Spec §11: Metriken heißen und labeln Micrometer-kompatibel — `http_server_requests_seconds` als Histogram mit Labels `method`, `uri` (Routen-TEMPLATE wie `/api/notes/{id}`, nie der konkrete Pfad), `status`. Eigene Metriken über `metrics::counter!/gauge!/histogram!`.
- Spec §11: Trace-Propagation ist W3C `traceparent`; Sampling parent-based, Default 100 %; OTLP-Export nur wenn `oxide.otel.endpoint` gesetzt ist (kein Endpoint = lokale Spans ohne Export, kein Fehler).
- Logs: JSON wenn Profil `prod` aktiv ODER `oxide.logging.format: json`; sonst menschenlesbar. Log-Level-Config (`oxide.logging.level.*`) verhält sich exakt wie bisher.
- Management-Server: `oxide.management.port` (Default 8081), `oxide.management.enabled` (Default true). Actuator-Endpoints (`/actuator/health` inkl. `/liveness` + `/readiness`, `/actuator/info`, `/actuator/prometheus`) liegen NUR auf dem Management-Port; der Haupt-Port serviert keine `/actuator/*`-Routen mehr (der Reserved-Path-Guard für User-Routen bleibt).
- inventory ist prozess-global: neue Test-Components booten in jedem Test ihres Binaries; BOOT_LOCK + Delta-Counter + geteilte Config-Helper wie etabliert; Ports in Tests immer 0.
- Der Prometheus-Recorder ist prozess-global installierbar (einmal pro Prozess) — Zugriff über `OnceLock`, mehrfache Boots in Test-Binaries dürfen nicht panicen.
- Nicht-Ziele 2b (Phase 3+): Health-Indicators aus Mongo/Kafka (nur der Registry-Mechanismus entsteht), Request-Validierung, CORS/Compression, `Header<T>`, verteiltes Scheduling, Tokio-Runtime-Vollmetriken (nur stabile Basis-Gauges).

## File Structure (Endzustand 2b, neu/geändert)

```
oxide-boot/
├── Cargo.toml                                # + oxide-observability; + metrics/otel-Deps
├── README.md                                 # Observability-Abschnitt, Actuator-Umzug, Cheat-Sheet-Zeilen
├── crates/
│   ├── oxide-core/src/container.rs           # ComponentDescriptor: #[non_exhaustive] + Builder; deterministische Topo-Tie-Breaks
│   ├── oxide-macros/src/{component,config_properties}.rs  # Builder-Codegen; #[config(prefix, default)]
│   ├── oxide-boot/src/{app.rs,logging.rs}    # LoggingInitializer-Seam (inventory), Fallback-Init
│   ├── oxide-web/src/
│   │   ├── serve.rs                          # interner HttpServing-Helper (bind/spawn/drain) für beide Server
│   │   ├── server.rs                         # WebServer nutzt serve.rs; ServerProps via #[config(default)]
│   │   ├── management.rs                     # ManagementProps, ManagementServer, ManagementRoute-Seam, /actuator/info
│   │   ├── routing.rs                        # RouterLayer-Seam; Health raus aus build_router
│   │   └── health.rs                         # unverändert intern, jetzt vom Management-Router gemountet
│   ├── oxide-web-macros/src/controller.rs    # Guards: generic/trait impls abgelehnt
│   └── oxide-observability/
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs                        # __private, Re-Exports
│           ├── logging.rs                    # LoggingInitializer (EnvFilter + fmt json/pretty + OTEL-Layer)
│           ├── metrics.rs                    # Prometheus-Recorder (OnceLock), MetricsHandle-Component
│           ├── otel.rs                       # OtelProps, Tracer-Provider, Flush-Component
│           └── http.rs                       # RouterLayer: http_server_requests_seconds + Request-Span + W3C-Extract
└── examples/rest-api/                        # Management-Port in yml, Custom-Metric, erweiterter Test
```

Neue `[workspace.dependencies]` (Task 5/7; Startpunkte, bei Resolve-Konflikten harmonisieren und im Report nennen):

```toml
oxide-observability = { path = "crates/oxide-observability" }
metrics = "0.24"
metrics-exporter-prometheus = { version = "0.16", default-features = false }
opentelemetry = "0.27"
opentelemetry_sdk = { version = "0.27", features = ["rt-tokio"] }
opentelemetry-otlp = { version = "0.27", features = ["grpc-tonic"] }
tracing-opentelemetry = "0.28"
```

---

### Task 1: ComponentDescriptor-Builder + deterministische Hook-Reihenfolge

**Files:**
- Modify: `crates/oxide-core/src/container.rs`
- Modify: `crates/oxide-macros/src/component.rs`, `crates/oxide-macros/src/config_properties.rs`
- Modify: alle handgeschriebenen Descriptor-Stellen: `crates/oxide-boot/tests/boot.rs`, `crates/oxide-boot/tests/boot_failure.rs`, `crates/oxide-web/src/server.rs`, `crates/oxide-web/tests/server.rs`

**Interfaces:**
- Consumes: bestehender `ComponentDescriptor` (pub-Felder, Struct-Literale)
- Produces:
  - `ComponentDescriptor` wird `#[non_exhaustive]` (Felder bleiben `pub` lesbar, Literal-Konstruktion außerhalb von oxide-core unmöglich)
  - `pub fn ComponentDescriptor::builder(type_name: &'static str, type_id: fn() -> TypeId, construct: fn(&mut ContextBuilder) -> Result<(), ContextError>) -> ComponentDescriptorBuilder` mit Methoden `.deps(fn() -> Vec<(TypeId, &'static str)>)`, `.post_construct(HookFn)`, `.pre_destroy(HookFn)`, `.build() -> ComponentDescriptor` — künftige Felder sind damit additive Builder-Methoden, kein Breaking Change mehr
  - `assemble` sortiert die Descriptors VOR der Topo-DFS stabil nach `type_name` → die Reihenfolge unabhängiger Components ist deterministisch (alphabetisch) statt Linker-Reihenfolge; Abhängigkeitsordnung gewinnt weiterhin
  - Alle Macros generieren Builder-Aufrufe statt Struct-Literalen; alle handgeschriebenen Descriptors umgestellt

- [ ] **Step 1: Failing Tests schreiben**

In `container.rs`-Testmodul:

```rust
    #[test]
    fn builder_produces_equivalent_descriptor() {
        let d = ComponentDescriptor::builder(
            "Built",
            || TypeId::of::<Repo>(),
            |b| {
                let cfg = b.resolve::<Config>()?;
                b.insert(Repo { prefix: cfg.greeting.clone() });
                Ok(())
            },
        )
        .deps(|| vec![(TypeId::of::<Config>(), "Config")])
        .build();
        assert_eq!(d.type_name, "Built");
        assert_eq!((d.deps)().len(), 1);
        assert!(d.post_construct.is_none() && d.pre_destroy.is_none());
    }

    #[test]
    fn independent_components_construct_in_type_name_order() {
        struct Zebra;
        struct Alpha;
        // Registered Zebra first — order must still be alphabetical (deterministic).
        let zebra: &'static ComponentDescriptor = Box::leak(Box::new(
            ComponentDescriptor::builder("Zebra", || TypeId::of::<Zebra>(), |b| { b.insert(Zebra); Ok(()) }).build(),
        ));
        let alpha: &'static ComponentDescriptor = Box::leak(Box::new(
            ComponentDescriptor::builder("Alpha", || TypeId::of::<Alpha>(), |b| { b.insert(Alpha); Ok(()) }).build(),
        ));
        let (_ctx, order) = assemble(vec![zebra, alpha], ContextBuilder::new()).unwrap();
        let names: Vec<_> = order.iter().map(|d| d.type_name).collect();
        assert_eq!(names, vec!["Alpha", "Zebra"]);
    }
```

(Bestehende Tests im Modul auf den Builder umstellen, wo sie Literale nutzen — gleiche Semantik.)

- [ ] **Step 2: RED**

Run: `cargo test -p oxide-core`
Expected: Compile-Fehler (`builder` existiert nicht). ROHEN Output ins Report.

- [ ] **Step 3: Implementierung**

`container.rs`:

```rust
#[derive(Debug)]
#[non_exhaustive]
pub struct ComponentDescriptor { /* Felder unverändert, bleiben pub */ }

pub struct ComponentDescriptorBuilder {
    inner: ComponentDescriptor,
}

impl ComponentDescriptor {
    pub fn builder(
        type_name: &'static str,
        type_id: fn() -> TypeId,
        construct: fn(&mut ContextBuilder) -> Result<(), ContextError>,
    ) -> ComponentDescriptorBuilder {
        ComponentDescriptorBuilder {
            inner: ComponentDescriptor {
                type_name,
                type_id,
                deps: Vec::new,
                construct,
                post_construct: None,
                pre_destroy: None,
            },
        }
    }
}

impl ComponentDescriptorBuilder {
    pub fn deps(mut self, deps: fn() -> Vec<(TypeId, &'static str)>) -> Self {
        self.inner.deps = deps;
        self
    }
    pub fn post_construct(mut self, hook: HookFn) -> Self {
        self.inner.post_construct = Some(hook);
        self
    }
    pub fn pre_destroy(mut self, hook: HookFn) -> Self {
        self.inner.pre_destroy = Some(hook);
        self
    }
    pub fn build(self) -> ComponentDescriptor {
        self.inner
    }
}
```

(`deps: Vec::new` — die argumentlose fn-Item-Referenz `Vec::new` coerct zu `fn() -> Vec<(TypeId, &'static str)>`; falls die Inferenz zickt, expliziten leeren fn-Pointer `|| Vec::new()` als benannte fn `fn no_deps() -> ...` verwenden.)

In `assemble` direkt nach dem Duplicate-Check einfügen:

```rust
    let mut descriptors = descriptors;
    descriptors.sort_by_key(|d| d.type_name); // deterministic order for independents
```

Macros umstellen — `component.rs` generiert jetzt:

```rust
::oxide_boot::__private::inventory::submit! {
    ::oxide_boot::__private::ComponentDescriptor::builder(
        stringify!(#ident),
        || ::core::any::TypeId::of::<#ident>(),
        |__builder| { /* wie bisher */ },
    )
    .deps(|| <[_]>::into_vec(::std::boxed::Box::new([#(#deps),*])))
    #maybe_post_construct   // `.post_construct(|__ctx| { ... })` nur wenn Arg gesetzt
    #maybe_pre_destroy
    .build()
}
```

`config_properties.rs` analog. Alle handgeschriebenen Descriptors (boot.rs Probe, boot_failure entfällt — nutzt Macro, oxide-web server.rs ServerProps + WebServer, tests/server.rs EchoService) auf Builder umstellen — Verhalten identisch.

- [ ] **Step 4: GREEN**

Run: `cargo test --workspace`
Expected: alles grün. WICHTIG: Falls ein Test von der alten Linker-Reihenfolge abhing, ist das ein ECHTER Befund — im Report dokumentieren, Test-Erwartung nur anpassen, wenn die neue alphabetische Ordnung semantisch korrekt ist.

- [ ] **Step 5: Commit**

```bash
git add crates/
git commit -m "feat(core)!: non_exhaustive ComponentDescriptor with builder and deterministic assembly order"
```

---

### Task 2: `#[config(prefix, default)]` + ServerProps auf Derive

**Files:**
- Modify: `crates/oxide-macros/src/config_properties.rs`
- Modify: `crates/oxide-web/src/server.rs` (handgeschriebener ServerProps-Descriptor entfällt)
- Test: `crates/oxide-boot/tests/boot.rs` (erweitern)

**Interfaces:**
- Consumes: ConfigProperties-Derive aus Phase 1 (Builder-Codegen aus Task 1)
- Produces: `#[config(prefix = "...", default)]` — mit `default`-Flag bindet der generierte Descriptor via `config.get_opt(prefix)?` und fällt bei fehlendem Abschnitt auf `Default::default()` zurück (Struct braucht dann `Default`). Ohne Flag: bisheriges Fail-Fast-Verhalten unverändert. `ServerProps` in oxide-web nutzt das Derive statt Handverdrahtung (Verhalten identisch: Abschnitt darf fehlen).

- [ ] **Step 1: Failing Test schreiben**

`crates/oxide-boot/tests/boot.rs` ergänzen (Config-Helper NICHT erweitern — genau das ist der Test):

```rust
#[derive(oxide_boot::ConfigProperties, serde::Deserialize, Default)]
#[config(prefix = "optional-section", default)]
struct OptionalProps {
    #[serde(default)]
    flag: bool,
}

#[component]
struct OptionalConsumer {
    props: OptionalProps,
}

#[tokio::test]
async fn config_properties_with_default_tolerate_missing_section() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir(); // contains NO `optional-section:` key — that is the point
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let consumer = app.context().get::<OptionalConsumer>().unwrap();
    assert!(!consumer.props.flag);
    app.stop().await.unwrap();
}
```

- [ ] **Step 2: RED**

Run: `cargo test -p oxide-boot --test boot`
Expected: FAIL — je nach bisherigem Parser Compile-Fehler („expected `prefix = ...`") oder Boot-Fehler (Missing key). ROHEN Output ins Report.

- [ ] **Step 3: Implementierung**

`config_properties.rs`: im `parse_nested_meta`-Callback zusätzlich `default` als flag akzeptieren (`meta.path.is_ident("default")` ohne Wert). Codegen-Unterschied nur im construct-Body:

```rust
// mit default-Flag:
let __props: #ident = __config
    .get_opt(#prefix)
    .map_err(|e| ::oxide_boot::__private::ContextError::Construct {
        type_name: stringify!(#ident),
        message: ::std::format!("binding prefix '{}': {}", #prefix, e),
    })?
    .unwrap_or_default();
```

`server.rs`: handgeschriebenen ServerProps-Descriptor löschen, stattdessen:

```rust
#[derive(Debug, Clone, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.server", default)]
pub struct ServerProps { /* Felder + Default-Impl unverändert */ }
```

Achtung Zyklus-Check: oxide-web hängt bereits von oxide-boot ab (seit 2a), das Derive ist nutzbar. Der WebServer-Descriptor referenziert ServerProps weiter über `deps` — unverändert.

- [ ] **Step 4: GREEN**

Run: `cargo test --workspace`
Expected: alles grün (insbesondere die bestehenden oxide-web-Server-Tests: ServerProps bindet weiter `oxide.server.port: 0` aus deren Config).

- [ ] **Step 5: Commit**

```bash
git add crates/
git commit -m "feat(macros): #[config(prefix, default)] tolerates missing sections; ServerProps uses derive"
```

---

### Task 3: Controller-Macro-Guards + ControllerNotRegistered-E2E

**Files:**
- Modify: `crates/oxide-web-macros/src/controller.rs`
- Test: `crates/oxide-web/tests/ui/generic_impl.rs` (+ .stderr), `crates/oxide-web/tests/ui/trait_impl.rs` (+ .stderr)
- Test: `crates/oxide-web/tests/unregistered_controller.rs` (NEUES eigenes Binary)

**Interfaces:**
- Consumes: `#[controller]`-Macro aus 2a
- Produces: `#[controller]` auf `impl<T> ...` → Compile-Fehler „#[controller] does not support generic impl blocks"; auf `impl Trait for Type` → „#[controller] must be applied to an inherent impl, not a trait impl". E2E-Beweis, dass ein Controller OHNE `#[component]` beim Boot `BootError::PostConstruct` mit `ControllerNotRegistered`-Text liefert (eigenes Binary — der kaputte Controller darf die anderen Tests nicht infizieren).

- [ ] **Step 1: Guards implementieren + trybuild-Cases**

In `expand` nach dem `ItemImpl`-Parse:

```rust
    if !item.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(&item.generics, "#[controller] does not support generic impl blocks"));
    }
    if let Some((_, trait_path, _)) = &item.trait_ {
        return Err(syn::Error::new_spanned(trait_path, "#[controller] must be applied to an inherent impl, not a trait impl"));
    }
```

trybuild-Cases analog zu den bestehenden (`generic_impl.rs`: `#[controller("/x")] impl<T> C<T> { #[get("")] async fn f(&self) -> String { ... } }`; `trait_impl.rs`: `#[controller("/x")] impl Clone for C { ... }` — minimal kompilierbare Formen). `TRYBUILD=overwrite`, .stderr MANUELL prüfen (beide Meldungen enthalten), committen.

- [ ] **Step 2: E2E-Test (eigenes Binary)**

`crates/oxide-web/tests/unregistered_controller.rs`:

```rust
use oxide_boot::prelude::*;
use oxide_web::controller;
use std::collections::HashMap;

// Deliberately NOT #[component]: boot must fail with ControllerNotRegistered.
struct GhostController;

#[controller("/ghost")]
impl GhostController {
    #[get("")]
    async fn boo(&self) -> String {
        "boo".to_string()
    }
}

#[tokio::test]
async fn unregistered_controller_fails_boot_with_typed_error() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), "oxide:\n  server:\n    port: 0\n").unwrap();
    let err = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("GhostController") && msg.contains("not registered"), "was: {msg}");
}
```

- [ ] **Step 3: Alles laufen lassen**

Run: `cargo test -p oxide-web && cargo test --workspace`
Expected: grün (5 trybuild-Cases, neues Binary PASS). RAW-Outputs ins Report.

- [ ] **Step 4: Commit**

```bash
git add crates/oxide-web crates/oxide-web-macros
git commit -m "fix(web): reject generic/trait impls in #[controller]; e2e-test unregistered controllers"
```

---

### Task 4: LoggingInitializer-Seam in oxide-boot

**Files:**
- Create: `crates/oxide-boot/src/logging.rs`
- Modify: `crates/oxide-boot/src/app.rs`, `crates/oxide-boot/src/lib.rs`

**Interfaces:**
- Consumes: bisheriges `init_logging` (privat in app.rs)
- Produces:
  - `pub struct LoggingInitializer { pub priority: i32, pub init: fn(&AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> }` + `inventory::collect!(LoggingInitializer)` in oxide-boot; Re-Export über `__private`
  - Boot-Ablauf: höchste `priority` gewinnt; genau EIN Initializer läuft. Der bisherige fmt-Init wird als eingebauter Fallback mit `priority: 0` registriert (gleiche Semantik: EnvFilter aus `oxide.logging.level.*`, try_init-tolerant für Mehrfach-Boots)
  - Fehler eines Initializers → `BootError::Logging { message }` (neue Variante — additive Erweiterung, kein Breaking Change)

- [ ] **Step 1: Failing Test schreiben**

In `crates/oxide-boot/tests/boot.rs`:

```rust
static CUSTOM_LOGGING_RAN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

inventory::submit! {
    oxide_boot::LoggingInitializer {
        priority: 100,
        init: |_config| {
            CUSTOM_LOGGING_RAN.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Deliberately no subscriber install: the built-in fallback must NOT run
            // as well, so log output setup stays with whoever wins.
            Ok(())
        },
    }
}

#[tokio::test]
async fn highest_priority_logging_initializer_wins() {
    let _guard = BOOT_LOCK.lock().await;
    let before = CUSTOM_LOGGING_RAN.load(std::sync::atomic::Ordering::SeqCst);
    let dir = test_config_dir();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    assert_eq!(CUSTOM_LOGGING_RAN.load(std::sync::atomic::Ordering::SeqCst), before + 1);
    app.stop().await.unwrap();
}
```

Hinweis: der Custom-Initializer läuft ab jetzt in JEDEM Boot dieses Binaries — er ist bewusst nebenwirkungsarm (Zähler + Ok). Delta-Check wie etabliert.

- [ ] **Step 2: RED**

Run: `cargo test -p oxide-boot --test boot`
Expected: Compile-Fehler (`LoggingInitializer` existiert nicht). RAW ins Report.

- [ ] **Step 3: Implementierung**

`logging.rs`:

```rust
use oxide_core::AppConfig;

/// Extension seam: starter crates register one of these to take over logging
/// setup. The single highest-priority initializer runs during boot, before the
/// DI container is assembled. The built-in plain-fmt setup registers itself at
/// priority 0.
pub struct LoggingInitializer {
    pub priority: i32,
    pub init: fn(&AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
}

inventory::collect!(LoggingInitializer);

pub(crate) fn run_highest_priority(config: &AppConfig) -> Result<(), crate::BootError> {
    let winner = inventory::iter::<LoggingInitializer>()
        .max_by_key(|i| i.priority)
        .expect("built-in fallback initializer is always registered");
    (winner.init)(config).map_err(|e| crate::BootError::Logging { message: e.to_string() })
}

inventory::submit! {
    LoggingInitializer { priority: 0, init: crate::app::fallback_fmt_init }
}
```

`app.rs`: `init_logging` in `pub(crate) fn fallback_fmt_init(config: &AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>>` umbenennen (Body unverändert plus `Ok(())`), Aufrufstelle in `start()` durch `crate::logging::run_highest_priority(&config)?` ersetzen. `BootError` um `#[error("logging initialization failed: {message}")] Logging { message: String }` ergänzen. `lib.rs`: `mod logging; pub use logging::LoggingInitializer;` (+ im `__private`). Das `expect` im `max_by_key`-Pfad ist unreachable-by-construction (Fallback immer registriert) — im Report flaggen.

- [ ] **Step 4: GREEN**

Run: `cargo test --workspace`
Expected: grün (auch hello-world/rest-api — dort läuft weiter der Fallback).

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-boot
git commit -m "feat(boot): LoggingInitializer seam with priority selection and built-in fallback"
```

---

### Task 5: oxide-observability-Scaffold + Logging (JSON/Pretty) + Metrics-Recorder

**Files:**
- Modify: `Cargo.toml` (Member + Deps aus dem Kopfblock)
- Create: `crates/oxide-observability/Cargo.toml`, `src/lib.rs`, `src/logging.rs`, `src/metrics.rs`

**Interfaces:**
- Consumes: `LoggingInitializer`, `AppConfig`, ComponentDescriptor-Builder
- Produces:
  - Crate `oxide-observability` (deps: oxide-core, oxide-boot, metrics, metrics-exporter-prometheus, tracing, tracing-subscriber, opentelemetry*-Stack, inventory, thiserror, serde; dev: tempfile, tokio)
  - `logging.rs`: `LoggingInitializer { priority: 100 }` — EnvFilter wie Fallback (`oxide.logging.level.*`); Format: JSON wenn Profil `prod` ODER `oxide.logging.format: json`, sonst pretty; try_init-tolerant. (Der OTEL-Layer kommt in Task 8 dazu.)
  - `metrics.rs`: Prometheus-Recorder via `PrometheusBuilder::new().install_recorder()` hinter `OnceLock<PrometheusHandle>` (`fn prometheus_handle() -> &'static PrometheusHandle`); Component `MetricsEndpoint` (leer, dient in Task 7 als Anker) — Recorder-Installation passiert im LoggingInitializer-Lauf (einmal pro Prozess, Mehrfach-Boot-sicher)
  - Custom-Metrics-Vertrag: User-Code ruft `metrics::counter!("name", "label" => "value").increment(1)` — landet im Handle-Render

- [ ] **Step 1: Failing Tests schreiben**

`crates/oxide-observability/tests/observability.rs`:

```rust
use oxide_boot::prelude::*;
use std::collections::HashMap;

static BOOT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_config_dir(extra: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        format!("oxide:\n  application:\n    name: obs-test\n{extra}"),
    )
    .unwrap();
    dir
}

#[tokio::test]
async fn boot_installs_prometheus_recorder_and_custom_metrics_render() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir("");
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    metrics::counter!("obs_test_counter_total", "kind" => "unit").increment(3);
    let rendered = oxide_observability::prometheus_handle().render();
    assert!(rendered.contains("obs_test_counter_total"), "render was: {rendered}");
    assert!(rendered.contains("kind=\"unit\""), "render was: {rendered}");

    app.stop().await.unwrap();
}

#[tokio::test]
async fn second_boot_in_same_process_does_not_panic_on_recorder() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir("");
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    app.stop().await.unwrap(); // reaching here twice across tests proves OnceLock guarding
}
```

- [ ] **Step 2: RED**

Run: `cargo test -p oxide-observability`
Expected: Crate existiert nicht / Compile-Fehler. RAW ins Report.

- [ ] **Step 3: Implementierung**

`metrics.rs` (Kern):

```rust
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Installs the process-global Prometheus recorder on first call; later calls
/// (further boots in the same test binary) reuse the existing handle.
pub fn prometheus_handle() -> &'static PrometheusHandle {
    HANDLE.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("no other metrics recorder may be installed before oxide-observability")
    })
}
```

(Das `expect` feuert nur, wenn ein FREMDER Recorder zuvor installiert wurde — Fehlkonfiguration, fail-fast beim Boot; im Report flaggen.)

`logging.rs`: Initializer registrieren —

```rust
inventory::submit! {
    oxide_boot::LoggingInitializer { priority: 100, init: crate::logging::init }
}

pub(crate) fn init(config: &oxide_core::AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    crate::metrics::prometheus_handle(); // recorder first, so early code can record
    let filter = build_env_filter(config); // gleiche Direktiven-Logik wie oxide-boot-Fallback
    let json = config.profiles().iter().any(|p| p == "prod")
        || matches!(config.get_opt::<String>("oxide.logging.format")?, Some(f) if f == "json");
    if json {
        let _ = tracing_subscriber::fmt().with_env_filter(filter).json().try_init();
    } else {
        let _ = tracing_subscriber::fmt().with_env_filter(filter).pretty().try_init();
    }
    Ok(())
}
```

`build_env_filter`: Direktiven-Aufbau aus `oxide.logging.level.root` + Targets — Logik aus `oxide-boot::app::fallback_fmt_init` EXTRAHIEREN statt duplizieren: die Direktiven-Berechnung als `pub fn logging_directives(config: &AppConfig) -> String` nach `oxide-boot::logging` ziehen, Fallback + Observability nutzen sie beide (kleiner Refactor in oxide-boot, gehört zu diesem Task).

`lib.rs`: Module + `pub use metrics::prometheus_handle;` + `#[doc(hidden)] pub mod __private { pub use inventory; }`.

- [ ] **Step 4: GREEN**

Run: `cargo test -p oxide-observability && cargo test --workspace`
Expected: grün.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/oxide-observability crates/oxide-boot
git commit -m "feat(observability): starter crate with json/pretty logging and prometheus recorder"
```

---

### Task 6: Management-Server + Health-Split + `/actuator/info`

**Files:**
- Create: `crates/oxide-web/src/serve.rs`, `crates/oxide-web/src/management.rs`
- Modify: `crates/oxide-web/src/server.rs`, `src/routing.rs`, `src/lib.rs`
- Test: `crates/oxide-web/tests/server.rs` (anpassen + erweitern)

**Interfaces:**
- Consumes: WebServer/health aus 2a, ComponentDescriptor-Builder, `#[config(default)]`
- Produces:
  - `serve.rs` (intern): `pub(crate) struct HttpServing` kapselt bind/spawn/graceful-shutdown/drain-timeout/abort — WebServer UND ManagementServer nutzen ihn (Extraktion aus WebServer, Verhalten identisch inkl. `local_addr`-Clearing)
  - `management.rs`: `#[config(prefix = "oxide.management", default)] pub struct ManagementProps { pub port: u16 /* default 8081 */, pub enabled: bool /* default true */ }`; `pub struct ManagementServer` (Component; bei `enabled: false` startet nichts, `local_addr()` bleibt `None`); Management-Router = Health-Routen (Readiness-Arc von `ctx.get::<WebServer>()`, falls vorhanden — sonst nur Liveness) + `/actuator/info` (JSON: application name, active profiles, framework version aus `env!("CARGO_PKG_VERSION")`) + alle `ManagementRoute`-Descriptors
  - `pub struct ManagementRoute { pub path: &'static str, pub register: fn(&Context, Router) -> Result<Router, WebError> }` + `inventory::collect!` — Seam für Task 7
  - `routing.rs`: `build_router` mountet KEINE Health-Routen mehr (Reserved-Path-Guard für User-Routen bleibt); `health.rs` unverändert, wird von management.rs gemountet
  - 503-Test: `health_routes` mit einem `AtomicBool(false)` direkt gebaut → `/actuator/health/readiness` liefert 503 + `OUT_OF_SERVICE` (Unit-Test in health.rs, dafür `pub(crate)`-Zugriff nutzen)

- [ ] **Step 1: Failing Tests schreiben**

`tests/server.rs` — bestehenden Health-Test umbauen + neue Tests:

```rust
#[tokio::test]
async fn actuator_lives_on_management_port_not_main_port() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir(); // yml erweitern: "  management:\n    port: 0\n" unter oxide:
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let main_addr = app.context().get::<WebServer>().unwrap().local_addr().unwrap();
    let mgmt_addr = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();
    assert_ne!(main_addr.port(), mgmt_addr.port());

    let health = reqwest::get(format!("http://{mgmt_addr}/actuator/health")).await.unwrap();
    assert_eq!(health.status(), 200);

    let on_main = reqwest::get(format!("http://{main_addr}/actuator/health")).await.unwrap();
    assert_eq!(on_main.status(), 404, "actuator must be gone from the main port");

    let info: serde_json::Value = reqwest::get(format!("http://{mgmt_addr}/actuator/info")).await.unwrap().json().await.unwrap();
    assert_eq!(info["application"]["name"], "web-test");

    app.stop().await.unwrap();
}
```

`health.rs`-Unit-Test (503-Zweig):

```rust
    #[tokio::test]
    async fn readiness_false_yields_503_out_of_service() {
        let readiness = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let router = health_routes(readiness);
        let server = axum_test_helper_or_oneshot(router).await; // per tower::ServiceExt::oneshot, kein echter Port
        // GET /actuator/health/readiness → 503, body {"status":"OUT_OF_SERVICE"}
    }
```

Konkret mit `tower::ServiceExt::oneshot` (dafür `tower` als dev-dependency wieder rein — nur dev!): Request bauen, `router.oneshot(req).await`, Status + Body asserten. Vollständigen Test ausschreiben.

Der bestehende `boots_serves_health_and_shuts_down`-Test wird auf den Management-Port umgestellt (Health-Asserts gegen `mgmt_addr`), sein Shutdown-/`local_addr`-Teil bleibt gegen den Haupt-Server.

- [ ] **Step 2: RED**

Run: `cargo test -p oxide-web --test server`
Expected: Compile-Fehler (`ManagementServer` existiert nicht). RAW ins Report.

- [ ] **Step 3: Implementierung**

Reihenfolge: (a) `serve.rs` aus WebServer extrahieren (reiner Move + Parametrisierung: bind-addr, name fürs Logging, grace); (b) WebServer auf `HttpServing` umstellen — alle 2a-Tests müssen davor/danach grün bleiben; (c) `management.rs` mit ManagementProps/ManagementServer (Descriptor via Builder; post_construct baut Management-Router: `health::health_routes(readiness_or_default)` + info-Route + `ManagementRoute`-Iteration mit gleicher Duplicate-Prüfung wie build_router) ; (d) build_router: Health-Merge entfernen; (e) lib.rs: `pub mod management; pub use management::{ManagementProps, ManagementRoute, ManagementServer};`.

Info-Handler:

```rust
async fn info(config: Inject<AppConfig>) -> Json<serde_json::Value> { /* via closure-captured Inject wie Route-Handler */ }
// {"application": {"name": ..., "profiles": [...]}, "framework": {"name": "oxide-boot", "version": env!("CARGO_PKG_VERSION")}}
```

- [ ] **Step 4: GREEN**

Run: `cargo test -p oxide-web && cargo test --workspace`
Expected: grün; rest-api-Integrationstest bleibt grün (er prüft keine Actuator-Routen auf dem Hauptport — verifizieren, sonst dort mit anpassen und im Report nennen).

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-web
git commit -m "feat(web): management server on separate port with health split and /actuator/info"
```

---

### Task 7: `/actuator/prometheus` via ManagementRoute

**Files:**
- Modify: `crates/oxide-observability/Cargo.toml` (dep oxide-web), `src/lib.rs`
- Create: `crates/oxide-observability/src/actuator.rs`
- Test: `crates/oxide-observability/tests/observability.rs` (erweitern)

**Interfaces:**
- Consumes: `ManagementRoute`-Seam, `prometheus_handle()`
- Produces: oxide-observability registriert `ManagementRoute { path: "/actuator/prometheus", register }` — Handler rendert `prometheus_handle().render()` als `text/plain; version=0.0.4` (Prometheus-Exposition-Format). Wer oxide-observability UND oxide-web einbindet, bekommt den Scrape-Endpoint automatisch; ohne oxide-web kompiliert oxide-observability weiter (dep ist regulär, kein Feature-Gate in 2b — dokumentieren).

- [ ] **Step 1: Failing Test schreiben**

```rust
use oxide_web::ManagementServer;

#[tokio::test]
async fn prometheus_endpoint_scrapes_custom_metrics() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir("  server:\n    port: 0\n  management:\n    port: 0\n");
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let mgmt = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();

    metrics::counter!("scrape_probe_total").increment(1);
    let res = reqwest::get(format!("http://{mgmt}/actuator/prometheus")).await.unwrap();
    assert_eq!(res.status(), 200);
    assert!(res.headers()["content-type"].to_str().unwrap().starts_with("text/plain"));
    assert!(res.text().await.unwrap().contains("scrape_probe_total"));

    app.stop().await.unwrap();
}
```

(oxide-web + reqwest als deps/dev-deps des Test-Crates ergänzen; das Test-Binary bootet jetzt Web- + Management-Server mit — `test_config_dir` bekommt die Ports als Parameter, bestehende Tests des Binaries entsprechend mitziehen.)

- [ ] **Step 2: RED** — `cargo test -p oxide-observability`, RAW ins Report.

- [ ] **Step 3: Implementierung**

`actuator.rs`:

```rust
inventory::submit! {
    oxide_web::ManagementRoute {
        path: "/actuator/prometheus",
        register: |_ctx, router| {
            Ok(router.route(
                "/actuator/prometheus",
                oxide_web::__private::axum::routing::get(|| async {
                    (
                        [(oxide_web::__private::axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                        crate::metrics::prometheus_handle().render(),
                    )
                }),
            ))
        },
    }
}
```

(`axum::http` über `__private` verfügbar machen, falls noch nicht re-exportiert.)

- [ ] **Step 4: GREEN** — `cargo test -p oxide-observability && cargo test --workspace`.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-observability crates/oxide-web
git commit -m "feat(observability): /actuator/prometheus management route with exposition format"
```

---

### Task 8: RouterLayer-Seam + automatische HTTP-Metriken + Request-Spans

**Files:**
- Modify: `crates/oxide-web/src/routing.rs`, `src/lib.rs`
- Create: `crates/oxide-observability/src/http.rs`
- Test: `crates/oxide-observability/tests/observability.rs` (erweitern)

**Interfaces:**
- Consumes: `build_router`, MatchedPath (axum), metrics-Recorder
- Produces:
  - oxide-web: `pub struct RouterLayer { pub name: &'static str, pub apply: fn(axum::Router) -> axum::Router }` + `inventory::collect!`; `build_router` wendet alle Layer nach dem Mounten an, sortiert nach `name` (deterministisch), mit `tracing::debug!(name, "router layer applied")`
  - oxide-observability `http.rs`: registriert einen RouterLayer mit `axum::middleware::from_fn`:
    - misst pro Request die Dauer und recorded `metrics::histogram!("http_server_requests_seconds", "method" => .., "uri" => <MatchedPath-Template oder "UNMATCHED">, "status" => ..)` 
    - erzeugt pro Request einen `tracing::info_span!("http.request", http.method, http.route, http.status_code)` — Statuscode nach dem Handler via `span.record`
    - W3C: liest `traceparent`/`tracestate` über den OTEL-Propagator (HeaderMap-Extractor handgerollt) und setzt den Parent-Context auf den Span (`tracing_opentelemetry::OpenTelemetrySpanExt::set_parent`) — funktioniert auch ohne konfigurierten Exporter (noop-Provider)
  - Micrometer-Kompatibilität ist Vertrag: Metrikname exakt `http_server_requests_seconds`, Label-Keys exakt `method`/`uri`/`status`

- [ ] **Step 1: Failing Test schreiben**

Im Observability-Test-Binary (bootet schon Web+Management; ein simpler Controller kommt dazu):

```rust
#[oxide_boot::component]
struct PingController {}

#[oxide_web::controller("/ping")]
impl PingController {
    #[get("/{name}")]
    async fn ping(&self, name: axum::extract::Path<String>) -> String {
        format!("pong {}", *name)
    }
}

#[tokio::test]
async fn http_requests_are_measured_with_route_template_labels() {
    let _guard = BOOT_LOCK.lock().await;
    let dir = test_config_dir("  server:\n    port: 0\n  management:\n    port: 0\n");
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();
    let main = app.context().get::<oxide_web::WebServer>().unwrap().local_addr().unwrap();
    let mgmt = app.context().get::<ManagementServer>().unwrap().local_addr().unwrap();

    reqwest::get(format!("http://{main}/ping/world")).await.unwrap();
    let scrape = reqwest::get(format!("http://{mgmt}/actuator/prometheus")).await.unwrap().text().await.unwrap();
    assert!(scrape.contains("http_server_requests_seconds"), "scrape: {scrape}");
    assert!(scrape.contains("uri=\"/ping/{name}\""), "route template label missing: {scrape}");
    assert!(scrape.contains("status=\"200\""), "scrape: {scrape}");

    app.stop().await.unwrap();
}
```

- [ ] **Step 2: RED** — RAW ins Report.

- [ ] **Step 3: Implementierung**

`routing.rs`-Seam (~10 Zeilen, analog ManagementRoute). `http.rs`-Middleware als Referenz:

```rust
pub(crate) async fn observe(req: axum::extract::Request, next: axum::middleware::Next) -> axum::response::Response {
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let method = req.method().as_str().to_owned();
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "UNMATCHED".to_owned());
    let span = tracing::info_span!("http.request", http.method = %method, http.route = %route, http.status_code = tracing::field::Empty);
    let parent = opentelemetry::global::get_text_map_propagator(|prop| prop.extract(&HeaderCarrier(req.headers())));
    span.set_parent(parent);

    let started = std::time::Instant::now();
    let response = {
        let _enter = span.enter();
        next.run(req).await
    };
    let status = response.status().as_u16().to_string();
    span.record("http.status_code", &status);
    metrics::histogram!(
        "http_server_requests_seconds",
        "method" => method, "uri" => route, "status" => status
    )
    .record(started.elapsed().as_secs_f64());
    response
}

struct HeaderCarrier<'a>(&'a axum::http::HeaderMap);
impl opentelemetry::propagation::Extractor for HeaderCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}
```

(Async-Spans sauber: falls `_enter` über den await problematisch ist — Clippy `await_holding_span_guard`/tracing-Doku — stattdessen `next.run(req).instrument(span.clone()).await` mit `tracing::Instrument`. Der Implementer wählt die idiomatische Variante; der Span muss den Handler-Lauf umschließen.)

RouterLayer-Registrierung:

```rust
inventory::submit! {
    oxide_web::RouterLayer {
        name: "oxide-observability/http",
        apply: |router| router.layer(oxide_web::__private::axum::middleware::from_fn(crate::http::observe)),
    }
}
```

- [ ] **Step 4: GREEN** — `cargo test -p oxide-observability && cargo test --workspace`.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-web crates/oxide-observability
git commit -m "feat(observability): automatic http_server_requests_seconds metrics and request spans via RouterLayer"
```

---

### Task 9: OTLP-Export + Sampler + Flush beim Shutdown

**Files:**
- Create: `crates/oxide-observability/src/otel.rs`
- Modify: `crates/oxide-observability/src/logging.rs` (OTEL-Layer in den Subscriber), `src/lib.rs`
- Test: `crates/oxide-observability/tests/otel.rs` (NEUES eigenes Binary — globaler Tracer-Provider!)

**Interfaces:**
- Consumes: `oxide.otel.*`-Config, LoggingInitializer, tracing-opentelemetry
- Produces:
  - `#[config(prefix = "oxide.otel", default)] OtelProps { pub endpoint: Option<String>, pub sample_ratio: f64 /* default 1.0 */ }`
  - Bei gesetztem `endpoint`: OTLP-Tracer-Provider (grpc-tonic, Batch-Exporter auf Tokio-Runtime), `service.name` = `oxide.application.name`, Sampler `ParentBased(TraceIdRatioBased(sample_ratio))`; global registriert; W3C `TraceContextPropagator` als globaler Propagator (auch OHNE Endpoint gesetzt — Extraction in Task 8 braucht ihn)
  - `tracing-opentelemetry`-Layer hängt im Subscriber (logging.rs), sodass `http.request`-Spans als OTEL-Spans exportiert werden
  - Flush/Shutdown: Component `OtelLifecycle` mit pre_destroy → `provider.shutdown()` (Batch-Flush); ohne Endpoint ist die Component ein No-Op
  - In-Memory-Beweis statt echtem Collector: Test nutzt den `opentelemetry_sdk`-InMemory-Span-Exporter über einen Test-Konstruktor `otel::init_with_exporter(exporter, props)` (pub(crate)-Test-Seam bzw. `#[doc(hidden)] pub`), asserted: Request mit `traceparent`-Header erzeugt exportierten Span mit passender Trace-ID

- [ ] **Step 1: Failing Test schreiben (eigenes Binary `tests/otel.rs`)**

```rust
// Kern-Assertions (vollständiges Gerüst analog observability.rs, eigenes Binary,
// KEIN zweiter Test daneben — globaler Provider):
#[tokio::test]
async fn traceparent_header_becomes_parent_of_exported_request_span() {
    // 1. InMemory-Exporter + Provider via oxide_observability::otel::testing::install_in_memory()
    //    (liefert Arc<InMemorySpanExporter>-Klon zum Auslesen)
    // 2. App booten (server port 0), GET /ping/x mit Header
    //    traceparent: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
    // 3. app.stop() (flusht), dann exporter.get_finished_spans():
    //    genau ein Span namens "http.request" mit trace_id == 0af7651916cd43dd8448eb211c80319c
}
```

Der Task schreibt den Test VOLLSTÄNDIG aus (Boot-Gerüst wie gehabt); die exakten InMemory-APIs (`InMemorySpanExporterBuilder` o. ä.) an die resolved opentelemetry_sdk-Version anpassen — die Assertions sind der Vertrag.

- [ ] **Step 2: RED** — RAW ins Report.

- [ ] **Step 3: Implementierung**

`otel.rs`-Struktur: `OtelProps` (Derive mit default); `pub(crate) fn build_layer(config) -> Option<Layer>` für logging.rs; globaler `TraceContextPropagator` IMMER (`opentelemetry::global::set_text_map_propagator`); Provider-Bau bei endpoint (Batch/tonic) ODER via Test-Seam mit InMemory-Exporter; `OtelLifecycle`-Component (Builder-Descriptor) mit pre_destroy → shutdown/flush. logging.rs: `.with(otel_layer)` in den Subscriber, wenn vorhanden (Layer-Komposition via `tracing_subscriber::registry()` statt `fmt()`-Shortcut, damit optionale Layer sauber stapeln — Umbau der init-Funktion gehört zum Task).

- [ ] **Step 4: GREEN** — `cargo test -p oxide-observability && cargo test --workspace`.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-observability
git commit -m "feat(observability): otlp export with parent-based sampling and shutdown flush"
```

---

### Task 10: Beispiel, README, Spec-Note, Workspace-Gate

**Files:**
- Modify: `examples/rest-api/` (Cargo.toml + application.yml + lib.rs + tests/api.rs), `README.md`, `docs/design/specs/2026-07-31-oxide-boot-design.md` (§5-Note)

**Interfaces:**
- Consumes: alles aus 2b
- Produces: rest-api mit Observability (Dependency genügt + `management.port` in yml); Custom-Metric im Controller (`metrics::counter!("notes_created_total")` im create-Handler); Test erweitert um Prometheus-Scrape-Assertion (Custom-Metric + http_server_requests_seconds sichtbar); README-Abschnitt „Observability" (Auto-Metriken, JSON-Logs, OTEL via `oxide.otel.endpoint`, Actuator jetzt auf Management-Port — Quickstart-Hinweis aktualisieren!) + Cheat-Sheet-Zeilen: Micrometer `Counter.builder(...)` → `metrics::counter!`, `management.server.port` → `oxide.management.port`, Sleuth/OTEL-Auto-Instrumentierung → automatisch via oxide-observability, `/actuator/prometheus` → gleich; Spec §5 bekommt die Reserved-Keys-Note (Top-Level-Keys, die mit Framework-Segmenten kollidieren — z. B. eigenes `server:` — können `OXIDE_SERVER_*`-Vars vom Framework wegfangen; Empfehlung: App-Keys nicht `server`/`management`/`otel`/`logging`/`application` nennen)
- Workspace-Gate + manueller Lauf: `cd examples/rest-api && cargo run`, curl auf `:8080/api/notes` und `:8081/actuator/prometheus` (http_server_requests_seconds + notes_created_total sichtbar), SIGTERM-Drain — RAW-Output ins Report

- [ ] **Step 1: Beispiel + Test erweitern** (yml: `management: { port: 8081 }`; Test-Env-Overrides `OXIDE_MANAGEMENT_PORT=0` zusätzlich zu `OXIDE_SERVER_PORT=0` — dogfoodet das Env-Binding weiter)
- [ ] **Step 2: README + Spec-Note**
- [ ] **Step 3: Gate**: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings` — Findings ordentlich fixen
- [ ] **Step 4: Manueller Lauf** (Output ins Report)
- [ ] **Step 5: Commit**

```bash
git add examples/rest-api README.md docs/
git commit -m "feat: rest-api observability wiring, docs and reserved-keys spec note"
```

---

## Abschluss-Check Phase 2b

- [ ] `cargo test --workspace` grün, Clippy `-D warnings` sauber
- [ ] Manuell verifiziert: `:8081/actuator/prometheus` liefert `http_server_requests_seconds` mit Routen-Template-Labels + Custom-Metric; JSON-Logs unter `OXIDE_PROFILES_ACTIVE=prod`; SIGTERM flusht OTEL und drained
- [ ] Spec-Abgleich §11: Metriken/Tracing/Logs abgedeckt; §6-Actuator: health/info/prometheus auf Management-Port. BEWUSST offen für Phase 3+: Health-Indicators aus Integrationen (Seam existiert via ManagementRoute + geplante HealthIndicator-Registry), Kafka-/Mongo-/Client-Metriken (kommen mit den Integrations-Crates), Tokio-Runtime-Vollmetriken, Log-`trace_id`-Feld-Extraktion in JSON (aktuell über Span-Kontext — echtes Top-Level-Feld als 3er-Kandidat)
- [ ] Ledger-Reste aus 2a geschlossen: `#[config(default)]` ✓ (T2), 503-Test ✓ (T6), ControllerNotRegistered-E2E ✓ (T3), Macro-Guards ✓ (T3), Descriptor-Evolution ✓ (T1), Hook-Determinismus ✓ (T1), Reserved-Keys-Spec-Note ✓ (T10); WEITER OFFEN: oxide-web-prelude-Entscheidung, Bind-Address-Config, Readiness-erst-nach-allen-Hooks (bewusst vertagt: aktuelle Semantik ist durch deterministische Ordnung + Management-Health akzeptabel — Phase-3-Kandidat)
- [ ] Danach: Phase 3 (Integrationen: oxide-mongo, oxide-kafka, oxide-client, Scheduler) planen
