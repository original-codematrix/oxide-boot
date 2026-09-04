# Oxide Boot Phase 1 (Fundament) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cargo-Workspace mit `oxide-core`, `oxide-macros`, `oxide-boot` und einem Hello-World-Beispiel, das `application.yml` (Profile + Env-Overrides) liest, Components per DI verdrahtet und sauber bootet/herunterfährt.

**Architecture:** `oxide-core` enthält Config-Loading und den DI-Container (Registry aus `inventory`-gesammelten `ComponentDescriptor`n, topologische Sortierung, `Arc`-Singletons). `oxide-macros` liefert `#[component]`, `#[derive(ConfigProperties)]` und `#[oxide_boot::main]`; generierter Code referenziert ausschließlich `::oxide_boot::__private::*`. `oxide-boot` ist die Umbrella-Crate mit `OxideApplication` (Boot-Lifecycle, Logging-Init, Signal-Handling).

**Tech Stack:** Rust (Edition 2024), Tokio (multi_thread), serde + serde_yaml_ng, inventory, thiserror, syn/quote (Proc-Macros), trybuild, tracing/tracing-subscriber, tempfile (Tests).

## Global Constraints

- Edition `2024`, `rust-version = "1.85"`, Workspace-Resolver `"3"`; alle Versionen NUR in `[workspace.dependencies]` pflegen.
- Spec: `docs/design/specs/2026-07-31-oxide-boot-design.md` — bei Widerspruch gewinnt die Spec.
- Alle Framework-APIs async-first; Components sind `Send + Sync`-Singletons in `Arc`.
- Generierter Macro-Code referenziert nur `::oxide_boot::__private::*` (nie `::oxide_core::*` direkt), damit User-Crates nur `oxide-boot` als Dependency brauchen.
- Fail-Fast: Fehler beim Boot (Config fehlt, Dependency fehlt, Zyklus) brechen mit sprechender Fehlermeldung ab, die den betroffenen Component-/Key-Namen nennt.
- Fehlertypen mit `thiserror`; kein `anyhow` in Library-Code. `unwrap()`/`expect()` nur in Tests und Beispielen.
- Falls `serde_yaml_ng` nicht verfügbar/inkompatibel: auf `serde_yaml = "0.9"` (archiviert, funktional) ausweichen und im Root-`Cargo.toml` einen Kommentar mit TODO-Ticket hinterlassen.
- Code, Kommentare und Doku im Repo auf Englisch (internationale Codebasis); dieses Planungsdokument ist Deutsch.
- Commits: Conventional Commits (`feat:`, `test:`, `chore:` …).

## File Structure (Endzustand Phase 1)

```
oxide-boot/
├── Cargo.toml                      # Workspace-Root, workspace.dependencies
├── .gitignore
├── crates/
│   ├── oxide-core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs              # Re-Exports
│   │       ├── config.rs           # AppConfig, ConfigError, deep_merge, env-Overrides, Platzhalter
│   │       └── container.rs        # Inject, ContextBuilder, Context, ComponentDescriptor, assemble
│   ├── oxide-macros/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs              # proc-macro Einstiegspunkte
│   │       ├── component.rs        # #[component]
│   │       ├── config_properties.rs# #[derive(ConfigProperties)]
│   │       └── main_macro.rs       # #[oxide_boot::main]
│   └── oxide-boot/
│       ├── Cargo.toml
│       ├── src/
│       │   ├── lib.rs              # prelude, __private, Re-Export der Macros
│       │   └── app.rs              # OxideApplication, AppBuilder, Application, BootError, Logging
│       └── tests/
│           ├── boot.rs             # Integrationstests: Macros + Boot end-to-end
│           ├── ui.rs               # trybuild-Runner
│           └── ui/                 # trybuild-Fälle (.rs + .stderr)
└── examples/
    └── hello-world/
        ├── Cargo.toml
        ├── application.yml
        ├── src/main.rs
        └── tests/hello.rs
```

---

### Task 1: Workspace-Scaffold

**Files:**
- Create: `Cargo.toml`, `.gitignore`
- Create: `crates/oxide-core/Cargo.toml`, `crates/oxide-core/src/lib.rs`
- Create: `crates/oxide-macros/Cargo.toml`, `crates/oxide-macros/src/lib.rs`
- Create: `crates/oxide-boot/Cargo.toml`, `crates/oxide-boot/src/lib.rs`

**Interfaces:**
- Consumes: —
- Produces: buildbarer Workspace; `oxide-boot` re-exportiert `oxide-core` als `pub mod __private` (spätere Tasks erweitern das).

- [ ] **Step 1: Workspace-Root anlegen**

`Cargo.toml`:

```toml
[workspace]
resolver = "3"
members = [
    "crates/oxide-core",
    "crates/oxide-macros",
    "crates/oxide-boot",
]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
license = "MIT OR Apache-2.0"

[workspace.dependencies]
oxide-core = { path = "crates/oxide-core" }
oxide-macros = { path = "crates/oxide-macros" }
oxide-boot = { path = "crates/oxide-boot" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal", "time", "sync"] }
serde = { version = "1", features = ["derive"] }
serde_yaml_ng = "0.10"
inventory = "0.3"
thiserror = "2"
futures = "0.3"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
syn = { version = "2", features = ["full"] }
quote = "1"
proc-macro2 = "1"
trybuild = "1"
tempfile = "3"
```

`.gitignore`:

```
/target
Cargo.lock
.DS_Store
```

(`Cargo.lock` bleibt in Phase 1 unversioniert; sobald das erste Binary/Beispiel produktiv wird, nehmen wir es rein.)

- [ ] **Step 2: Crate-Manifeste + leere libs**

`crates/oxide-core/Cargo.toml`:

```toml
[package]
name = "oxide-core"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
serde.workspace = true
serde_yaml_ng.workspace = true
thiserror.workspace = true
futures.workspace = true
inventory.workspace = true

[dev-dependencies]
tempfile.workspace = true
tokio = { workspace = true }
```

`crates/oxide-core/src/lib.rs`:

```rust
//! Core building blocks of Oxide Boot: configuration loading and the
//! dependency-injection container. Users normally depend on `oxide-boot`,
//! not on this crate directly.
```

`crates/oxide-macros/Cargo.toml`:

```toml
[package]
name = "oxide-macros"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[lib]
proc-macro = true

[dependencies]
syn.workspace = true
quote.workspace = true
proc-macro2.workspace = true
```

`crates/oxide-macros/src/lib.rs`:

```rust
//! Procedural macros for Oxide Boot. Generated code only references
//! paths under `::oxide_boot::__private`.
```

`crates/oxide-boot/Cargo.toml`:

```toml
[package]
name = "oxide-boot"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
oxide-core.workspace = true
oxide-macros.workspace = true
tokio.workspace = true
inventory.workspace = true
thiserror.workspace = true
futures.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
serde.workspace = true

[dev-dependencies]
trybuild.workspace = true
tempfile.workspace = true
```

`crates/oxide-boot/src/lib.rs`:

```rust
//! Oxide Boot — a Spring-Boot-like application framework for Rust.

/// Internal re-exports used by generated macro code. Not part of the
/// public API; do not use directly.
#[doc(hidden)]
pub mod __private {
    pub use inventory;
    pub use oxide_core::*;
    pub use tokio;
}
```

- [ ] **Step 3: Build prüfen**

Run: `cargo build --workspace`
Expected: kompiliert ohne Fehler (Warnungen über leere Crates sind ok).

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml .gitignore crates/
git commit -m "chore: scaffold oxide-boot workspace with core, macros and umbrella crates"
```

---

### Task 2: Config-Dateien laden + Deep-Merge + Profile

**Files:**
- Create: `crates/oxide-core/src/config.rs`
- Modify: `crates/oxide-core/src/lib.rs`

**Interfaces:**
- Consumes: —
- Produces:
  - `pub struct AppConfig` mit `pub fn load(base_dir: &Path, env: &HashMap<String, String>) -> Result<AppConfig, ConfigError>` und `pub fn profiles(&self) -> &[String]`
  - `pub enum ConfigError { Io{..}, Parse{..}, Missing{..}, Deserialize{..}, Placeholder{..} }`
  - Ladereihenfolge: `application.yml`/`.yaml` in `base_dir`, dann `base_dir/config/`, danach je aktivem Profil `application-{profile}.yml` in derselben Reihenfolge; spätere Quellen überschreiben per Deep-Merge. Profile aus Env `OXIDE_PROFILES_ACTIVE` (kommagetrennt).

- [ ] **Step 1: Failing Tests schreiben**

Ans Ende von `crates/oxide-core/src/config.rs` (Datei neu anlegen, oben vorerst nur `use`-Zeilen, Implementierung folgt in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn write(dir: &std::path::Path, name: &str, content: &str) {
        std::fs::write(dir.join(name), content).unwrap();
    }

    #[test]
    fn loads_base_file_and_reads_nested_values() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "oxide:\n  server:\n    port: 8080\n");
        let cfg = AppConfig::load(dir.path(), &HashMap::new()).unwrap();
        assert_eq!(cfg.get::<u16>("oxide.server.port").unwrap(), 8080);
    }

    #[test]
    fn profile_file_overrides_base_deep_merge_keeps_siblings() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "oxide:\n  server:\n    port: 8080\n  app:\n    name: demo\n");
        write(dir.path(), "application-prod.yml", "oxide:\n  server:\n    port: 9090\n");
        let env = HashMap::from([("OXIDE_PROFILES_ACTIVE".into(), "prod".into())]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();
        assert_eq!(cfg.get::<u16>("oxide.server.port").unwrap(), 9090);
        assert_eq!(cfg.get::<String>("oxide.app.name").unwrap(), "demo"); // sibling survives merge
        assert_eq!(cfg.profiles(), &["prod".to_string()]);
    }

    #[test]
    fn config_subdir_overrides_base_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("config")).unwrap();
        write(dir.path(), "application.yml", "a: 1\n");
        write(&dir.path().join("config"), "application.yml", "a: 2\n");
        let cfg = AppConfig::load(dir.path(), &HashMap::new()).unwrap();
        assert_eq!(cfg.get::<i64>("a").unwrap(), 2);
    }

    #[test]
    fn missing_files_yield_empty_config_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = AppConfig::load(dir.path(), &HashMap::new()).unwrap();
        assert!(matches!(cfg.get::<u16>("nope"), Err(ConfigError::Missing { .. })));
    }

    #[test]
    fn invalid_yaml_reports_file_path() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "a: [unclosed\n");
        let err = AppConfig::load(dir.path(), &HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("application.yml"), "was: {err}");
    }
}
```

Hinweis: `get::<T>` wird hier schon benutzt und in diesem Task minimal mitimplementiert (ohne Platzhalter — die kommen in Task 4).

- [ ] **Step 2: Tests laufen lassen — müssen fehlschlagen**

Run: `cargo test -p oxide-core`
Expected: FAIL (Compile-Fehler: `AppConfig` existiert nicht).

- [ ] **Step 3: Implementierung**

`crates/oxide-core/src/config.rs` (oberhalb des Test-Moduls):

```rust
use serde::de::DeserializeOwned;
use serde_yaml_ng::Value;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config file {path}: {source}")]
    Io { path: String, #[source] source: std::io::Error },
    #[error("invalid YAML in {path}: {source}")]
    Parse { path: String, #[source] source: serde_yaml_ng::Error },
    #[error("missing configuration key '{path}'")]
    Missing { path: String },
    #[error("configuration key '{path}' has an unexpected shape: {source}")]
    Deserialize { path: String, #[source] source: serde_yaml_ng::Error },
    #[error("unresolvable placeholder '${{{placeholder}}}' (no such key, no default)")]
    Placeholder { placeholder: String },
}

/// Layered application configuration, Spring-Boot style:
/// `application.yml` -> `config/application.yml` -> per-profile files -> env overrides.
#[derive(Debug, Clone)]
pub struct AppConfig {
    root: Value,
    profiles: Vec<String>,
}

impl AppConfig {
    pub fn load(base_dir: &Path, env: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let profiles: Vec<String> = env
            .get("OXIDE_PROFILES_ACTIVE")
            .map(|p| p.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
            .unwrap_or_default();

        let mut root = Value::Mapping(Default::default());
        let file_stems =
            std::iter::once("application".to_string()).chain(profiles.iter().map(|p| format!("application-{p}")));
        for stem in file_stems {
            for dir in [base_dir.to_path_buf(), base_dir.join("config")] {
                for ext in ["yml", "yaml"] {
                    let path = dir.join(format!("{stem}.{ext}"));
                    if !path.exists() {
                        continue;
                    }
                    let display = path.display().to_string();
                    let text = std::fs::read_to_string(&path)
                        .map_err(|source| ConfigError::Io { path: display.clone(), source })?;
                    let value: Value = serde_yaml_ng::from_str(&text)
                        .map_err(|source| ConfigError::Parse { path: display, source })?;
                    deep_merge(&mut root, value);
                }
            }
        }
        Ok(Self { root, profiles })
    }

    pub fn profiles(&self) -> &[String] {
        &self.profiles
    }

    pub fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ConfigError> {
        let node = self.lookup(path).ok_or_else(|| ConfigError::Missing { path: path.to_string() })?;
        serde_yaml_ng::from_value(node.clone())
            .map_err(|source| ConfigError::Deserialize { path: path.to_string(), source })
    }

    pub fn get_opt<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>, ConfigError> {
        match self.get(path) {
            Ok(v) => Ok(Some(v)),
            Err(ConfigError::Missing { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn lookup(&self, path: &str) -> Option<&Value> {
        path.split('.').try_fold(&self.root, |node, segment| match node {
            Value::Mapping(map) => map.get(Value::String(segment.to_string())),
            _ => None,
        })
    }
}

/// Recursively merge `overlay` into `base`: mappings merge key-wise,
/// everything else (scalars, sequences) is replaced by the overlay value.
fn deep_merge(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Mapping(base_map), Value::Mapping(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(existing) => deep_merge(existing, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_slot, overlay) => *base_slot = overlay,
    }
}
```

`crates/oxide-core/src/lib.rs` ergänzen:

```rust
pub mod config;
pub use config::{AppConfig, ConfigError};
```

API-Hinweis: falls `Mapping::get`/`get_mut` in `serde_yaml_ng` eine Referenz (`&Value`) als Index erwarten, entsprechend `map.get(&Value::String(...))` schreiben — Compiler-Fehlermeldung entscheidet.

- [ ] **Step 4: Tests laufen lassen — müssen grün sein**

Run: `cargo test -p oxide-core`
Expected: alle 5 Tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-core
git commit -m "feat(core): layered application.yml loading with profiles and deep merge"
```

---

### Task 3: Env-Var-Overrides mit Relaxed Binding

**Files:**
- Modify: `crates/oxide-core/src/config.rs`

**Interfaces:**
- Consumes: `AppConfig::load`, `deep_merge` aus Task 2
- Produces: Env-Vars mit Prefix `OXIDE_` überschreiben Config-Keys. Mapping: Segmente per `_` getrennt, Groß-/Kleinschreibung und `-` egal; mehrteilige Keys (z. B. `bootstrap-servers`) werden per Greedy-Longest-Match gegen **existierende** Keys gebunden. Werte werden als YAML-Skalar geparst (Zahlen/Bools typisiert). `OXIDE_PROFILES_ACTIVE` ist reserviert.

- [ ] **Step 1: Failing Tests schreiben**

Ins Test-Modul von `config.rs`:

```rust
    #[test]
    fn env_var_overrides_existing_key_with_typed_value() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "oxide:\n  server:\n    port: 8080\n");
        let env = HashMap::from([("OXIDE_SERVER_PORT".into(), "9090".into())]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();
        assert_eq!(cfg.get::<u16>("oxide.server.port").unwrap(), 9090);
    }

    #[test]
    fn env_var_binds_to_hyphenated_key_via_relaxed_matching() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "oxide:\n  kafka:\n    bootstrap-servers: \"localhost:9092\"\n");
        let env = HashMap::from([("OXIDE_KAFKA_BOOTSTRAP_SERVERS".into(), "kafka:9092".into())]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();
        assert_eq!(cfg.get::<String>("oxide.kafka.bootstrap-servers").unwrap(), "kafka:9092");
    }

    #[test]
    fn env_var_for_unknown_key_creates_one_level_per_segment() {
        let dir = tempfile::tempdir().unwrap();
        let env = HashMap::from([("OXIDE_FEATURE_ENABLED".into(), "true".into())]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();
        assert_eq!(cfg.get::<bool>("oxide.feature.enabled").unwrap(), true);
    }

    #[test]
    fn non_oxide_env_vars_and_profiles_var_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let env = HashMap::from([
            ("PATH".into(), "/usr/bin".into()),
            ("OXIDE_PROFILES_ACTIVE".into(), "prod".into()),
        ]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();
        assert!(cfg.get_opt::<String>("path").unwrap().is_none());
        assert!(cfg.get_opt::<String>("oxide.profiles.active").unwrap().is_none());
    }
```

- [ ] **Step 2: Tests laufen lassen — die neuen müssen fehlschlagen**

Run: `cargo test -p oxide-core`
Expected: die 4 neuen Tests FAIL (Override wird noch nicht angewendet), alte bleiben grün.

- [ ] **Step 3: Implementierung**

In `config.rs` in `AppConfig::load` direkt vor `Ok(Self { .. })` einfügen:

```rust
        apply_env_overrides(&mut root, env);
```

Und die freien Funktionen ergänzen:

```rust
/// Case-insensitive comparison key: lowercased, `-` and `_` stripped.
fn normalize(key: &str) -> String {
    key.chars()
        .filter(|c| *c != '-' && *c != '_')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn apply_env_overrides(root: &mut Value, env: &HashMap<String, String>) {
    for (name, raw) in env {
        if !name.starts_with("OXIDE_") || name == "OXIDE_PROFILES_ACTIVE" {
            continue;
        }
        let segments: Vec<String> = name.split('_').map(|s| s.to_ascii_lowercase()).collect();
        // Parse the value as a YAML scalar so "9090" becomes a number, "true" a bool.
        let value: Value = serde_yaml_ng::from_str(raw).unwrap_or_else(|_| Value::String(raw.clone()));
        set_relaxed(root, &segments, value);
    }
}

/// Walks/creates the tree for the given env-var segments. At each level it
/// greedily tries to match the longest run of segments against an existing
/// key (normalized), so `KAFKA_BOOTSTRAP_SERVERS` binds to `bootstrap-servers`.
/// Unknown paths are created with one mapping level per segment.
fn set_relaxed(node: &mut Value, segments: &[String], value: Value) {
    if segments.is_empty() {
        return;
    }
    if !matches!(node, Value::Mapping(_)) {
        *node = Value::Mapping(Default::default());
    }
    let Value::Mapping(map) = node else { unreachable!() };

    for take in (1..=segments.len()).rev() {
        let candidate = normalize(&segments[..take].join(""));
        let existing = map
            .keys()
            .find(|k| matches!(k, Value::String(s) if normalize(s) == candidate))
            .cloned();
        if let Some(key) = existing {
            if take == segments.len() {
                map.insert(key, value);
            } else {
                set_relaxed(map.get_mut(&key).expect("key just found"), &segments[take..], value);
            }
            return;
        }
    }

    let head = Value::String(segments[0].clone());
    if segments.len() == 1 {
        map.insert(head, value);
    } else {
        if map.get(&head).is_none() {
            map.insert(head.clone(), Value::Mapping(Default::default()));
        }
        set_relaxed(map.get_mut(&head).expect("just inserted"), &segments[1..], value);
    }
}
```

- [ ] **Step 4: Tests laufen lassen — müssen grün sein**

Run: `cargo test -p oxide-core`
Expected: alle Tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-core
git commit -m "feat(core): env-var overrides with relaxed binding (OXIDE_* -> config keys)"
```

---

### Task 4: `${...}`-Platzhalter-Auflösung

**Files:**
- Modify: `crates/oxide-core/src/config.rs`

**Interfaces:**
- Consumes: `AppConfig` aus Task 2/3
- Produces: String-Werte mit `${key.path}` bzw. `${key.path:default}` werden nach dem Laden gegen den Config-Baum aufgelöst (max. 5 Iterationen für verkettete Platzhalter). Unauflösbar ohne Default → `ConfigError::Placeholder`.

- [ ] **Step 1: Failing Tests schreiben**

```rust
    #[test]
    fn placeholder_resolves_against_other_key() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml",
            "oxide:\n  application:\n    name: user-service\nkafka-group: \"${oxide.application.name}-group\"\n");
        let cfg = AppConfig::load(dir.path(), &HashMap::new()).unwrap();
        assert_eq!(cfg.get::<String>("kafka-group").unwrap(), "user-service-group");
    }

    #[test]
    fn placeholder_uses_default_when_key_missing() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "url: \"${billing.url:http://localhost:8081}\"\n");
        let cfg = AppConfig::load(dir.path(), &HashMap::new()).unwrap();
        assert_eq!(cfg.get::<String>("url").unwrap(), "http://localhost:8081");
    }

    #[test]
    fn unresolvable_placeholder_without_default_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "url: \"${nope.nope}\"\n");
        let err = AppConfig::load(dir.path(), &HashMap::new()).unwrap_err();
        assert!(matches!(err, ConfigError::Placeholder { .. }), "was: {err}");
    }
```

- [ ] **Step 2: Tests laufen lassen — die neuen müssen fehlschlagen**

Run: `cargo test -p oxide-core`
Expected: 3 neue Tests FAIL.

- [ ] **Step 3: Implementierung**

In `AppConfig::load` nach `apply_env_overrides(...)`:

```rust
        resolve_placeholders(&mut root)?;
```

Freie Funktionen:

```rust
/// Resolves `${key.path}` / `${key.path:default}` inside all string scalars.
/// Runs up to 5 passes so placeholders may reference values that themselves
/// contain placeholders.
fn resolve_placeholders(root: &mut Value) -> Result<(), ConfigError> {
    for _ in 0..5 {
        let snapshot = root.clone();
        let mut changed = false;
        substitute_strings(root, &snapshot, &mut changed)?;
        if !changed {
            return Ok(());
        }
    }
    Ok(())
}

fn substitute_strings(node: &mut Value, snapshot: &Value, changed: &mut bool) -> Result<(), ConfigError> {
    match node {
        Value::Mapping(map) => {
            for (_, v) in map.iter_mut() {
                substitute_strings(v, snapshot, changed)?;
            }
        }
        Value::Sequence(seq) => {
            for v in seq.iter_mut() {
                substitute_strings(v, snapshot, changed)?;
            }
        }
        Value::String(s) if s.contains("${") => {
            let mut result = String::with_capacity(s.len());
            let mut rest = s.as_str();
            while let Some(start) = rest.find("${") {
                result.push_str(&rest[..start]);
                let after = &rest[start + 2..];
                let end = after.find('}').ok_or_else(|| ConfigError::Placeholder { placeholder: after.to_string() })?;
                let inner = &after[..end];
                let (key, default) = match inner.split_once(':') {
                    Some((k, d)) => (k, Some(d)),
                    None => (inner, None),
                };
                let looked_up = lookup_path(snapshot, key).map(value_to_string);
                match looked_up.or_else(|| default.map(str::to_string)) {
                    Some(replacement) => result.push_str(&replacement),
                    None => return Err(ConfigError::Placeholder { placeholder: inner.to_string() }),
                }
                rest = &after[end + 1..];
            }
            result.push_str(rest);
            if &result != s {
                *changed = true;
                *s = result;
            }
        }
        _ => {}
    }
    Ok(())
}

fn lookup_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(root, |node, segment| match node {
        Value::Mapping(map) => map.get(Value::String(segment.to_string())),
        _ => None,
    })
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => serde_yaml_ng::to_string(other).unwrap_or_default().trim_end().to_string(),
    }
}
```

`AppConfig::lookup` auf `lookup_path(&self.root, path)` umstellen (eine Implementierung statt zwei).

- [ ] **Step 4: Tests laufen lassen — müssen grün sein**

Run: `cargo test -p oxide-core`
Expected: alle Tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-core
git commit -m "feat(core): ${key:default} placeholder resolution in config values"
```

---

### Task 5: DI-Container — Context, Descriptor, topologische Sortierung

**Files:**
- Create: `crates/oxide-core/src/container.rs`
- Modify: `crates/oxide-core/src/lib.rs`

**Interfaces:**
- Consumes: —
- Produces (alles `pub` aus `oxide_core`):
  - `pub struct Inject<T>(Arc<T>)` mit `Deref<Target = T>` + `Clone`
  - `pub type ComponentResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;`
  - `pub type HookFn = fn(&Context) -> futures::future::BoxFuture<'static, ComponentResult<()>>;`
  - `pub struct ComponentDescriptor { pub type_name: &'static str, pub type_id: fn() -> TypeId, pub deps: fn() -> Vec<(TypeId, &'static str)>, pub construct: fn(&mut ContextBuilder) -> Result<(), ContextError>, pub post_construct: Option<HookFn>, pub pre_destroy: Option<HookFn> }` + `inventory::collect!(ComponentDescriptor)`
  - `pub struct ContextBuilder` mit `new()`, `insert<T: Send + Sync + 'static>(&mut self, value: T)`, `resolve<T: Send + Sync + 'static>(&self) -> Result<Inject<T>, ContextError>`, `contains(&self, id: TypeId) -> bool`
  - `pub struct Context` mit `get<T: Send + Sync + 'static>(&self) -> Option<Inject<T>>`
  - `pub fn assemble(descriptors: Vec<&'static ComponentDescriptor>, seed: ContextBuilder) -> Result<(Context, Vec<&'static ComponentDescriptor>), ContextError>` — konstruiert in Abhängigkeitsreihenfolge, Rückgabe enthält die Reihenfolge für Lifecycle-Hooks
  - `pub enum ContextError { MissingDependency{..}, Cycle{..}, Duplicate{..}, NotFound{..}, Construct{..} }`

- [ ] **Step 1: Failing Tests schreiben**

`crates/oxide-core/src/container.rs` anlegen, Test-Modul ans Ende:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;

    struct Config { greeting: String }
    struct Repo { prefix: String }
    struct Service { repo: Inject<Repo> }

    static REPO: ComponentDescriptor = ComponentDescriptor {
        type_name: "Repo",
        type_id: || TypeId::of::<Repo>(),
        deps: || vec![(TypeId::of::<Config>(), "Config")],
        construct: |b| {
            let cfg = b.resolve::<Config>()?;
            b.insert(Repo { prefix: cfg.greeting.clone() });
            Ok(())
        },
        post_construct: None,
        pre_destroy: None,
    };

    static SERVICE: ComponentDescriptor = ComponentDescriptor {
        type_name: "Service",
        type_id: || TypeId::of::<Service>(),
        deps: || vec![(TypeId::of::<Repo>(), "Repo")],
        construct: |b| {
            let repo = b.resolve::<Repo>()?;
            b.insert(Service { repo });
            Ok(())
        },
        post_construct: None,
        pre_destroy: None,
    };

    fn seed() -> ContextBuilder {
        let mut b = ContextBuilder::new();
        b.insert(Config { greeting: "hello".into() });
        b
    }

    #[test]
    fn constructs_in_dependency_order_regardless_of_registration_order() {
        // SERVICE registered before its dependency REPO on purpose.
        let (ctx, order) = assemble(vec![&SERVICE, &REPO], seed()).unwrap();
        assert_eq!(ctx.get::<Service>().unwrap().repo.prefix, "hello");
        let names: Vec<_> = order.iter().map(|d| d.type_name).collect();
        assert_eq!(names, vec!["Repo", "Service"]);
    }

    #[test]
    fn missing_dependency_names_both_components() {
        let err = assemble(vec![&SERVICE], seed()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Service") && msg.contains("Repo"), "was: {msg}");
    }

    #[test]
    fn cycle_is_reported_with_path() {
        struct A(#[allow(dead_code)] u8);
        struct B(#[allow(dead_code)] u8);
        static CYC_A: ComponentDescriptor = ComponentDescriptor {
            type_name: "A",
            type_id: || TypeId::of::<A>(),
            deps: || vec![(TypeId::of::<B>(), "B")],
            construct: |_| Ok(()),
            post_construct: None,
            pre_destroy: None,
        };
        static CYC_B: ComponentDescriptor = ComponentDescriptor {
            type_name: "B",
            type_id: || TypeId::of::<B>(),
            deps: || vec![(TypeId::of::<A>(), "A")],
            construct: |_| Ok(()),
            post_construct: None,
            pre_destroy: None,
        };
        let err = assemble(vec![&CYC_A, &CYC_B], ContextBuilder::new()).unwrap_err();
        assert!(matches!(err, ContextError::Cycle { .. }));
        assert!(err.to_string().contains("A -> B -> A"), "was: {err}");
    }

    #[test]
    fn duplicate_registration_is_an_error() {
        let err = assemble(vec![&REPO, &REPO], seed()).unwrap_err();
        assert!(matches!(err, ContextError::Duplicate { .. }));
    }

    #[test]
    fn inject_derefs_and_clones_share_the_same_instance() {
        let (ctx, _) = assemble(vec![&REPO], seed()).unwrap();
        let a = ctx.get::<Repo>().unwrap();
        let b = a.clone();
        assert!(std::ptr::eq(&*a, &*b));
        assert_eq!(a.prefix, "hello"); // Deref
    }
}
```

- [ ] **Step 2: Tests laufen lassen — müssen fehlschlagen**

Run: `cargo test -p oxide-core container`
Expected: FAIL (Compile-Fehler: Typen existieren nicht).

- [ ] **Step 3: Implementierung**

Oberhalb des Test-Moduls in `container.rs`:

```rust
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

pub type ComponentResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type HookFn = fn(&Context) -> futures::future::BoxFuture<'static, ComponentResult<()>>;

#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("component '{component}' depends on '{dependency}', but no such component is registered")]
    MissingDependency { component: &'static str, dependency: &'static str },
    #[error("dependency cycle detected: {path}")]
    Cycle { path: String },
    #[error("component '{type_name}' is registered more than once")]
    Duplicate { type_name: &'static str },
    #[error("no component of type '{type_name}' in the context")]
    NotFound { type_name: &'static str },
    #[error("failed to construct component '{type_name}': {message}")]
    Construct { type_name: &'static str, message: String },
}

/// Shared handle to a singleton component. Cheap to clone; derefs to `T`.
pub struct Inject<T>(Arc<T>);

impl<T> Clone for Inject<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> std::ops::Deref for Inject<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

/// Static description of one component: how to identify it, what it needs,
/// how to build it. Registered via `inventory` by the `#[component]` macro.
pub struct ComponentDescriptor {
    pub type_name: &'static str,
    pub type_id: fn() -> TypeId,
    pub deps: fn() -> Vec<(TypeId, &'static str)>,
    pub construct: fn(&mut ContextBuilder) -> Result<(), ContextError>,
    pub post_construct: Option<HookFn>,
    pub pre_destroy: Option<HookFn>,
}

inventory::collect!(ComponentDescriptor);

#[derive(Default)]
pub struct ContextBuilder {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl ContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub fn resolve<T: Send + Sync + 'static>(&self) -> Result<Inject<T>, ContextError> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|any| Arc::clone(any).downcast::<T>().ok())
            .map(Inject)
            .ok_or(ContextError::NotFound { type_name: std::any::type_name::<T>() })
    }

    pub fn contains(&self, id: TypeId) -> bool {
        self.map.contains_key(&id)
    }
}

/// Immutable, fully-wired application context.
pub struct Context {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl Context {
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Inject<T>> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|any| Arc::clone(any).downcast::<T>().ok())
            .map(Inject)
    }
}

/// Builds the context: topologically sorts descriptors by their dependencies,
/// constructs every component exactly once, fails fast on missing deps,
/// duplicates and cycles. Returns the construction order for lifecycle hooks.
pub fn assemble(
    descriptors: Vec<&'static ComponentDescriptor>,
    mut seed: ContextBuilder,
) -> Result<(Context, Vec<&'static ComponentDescriptor>), ContextError> {
    let mut by_id: HashMap<TypeId, &'static ComponentDescriptor> = HashMap::new();
    for d in &descriptors {
        if by_id.insert((d.type_id)(), d).is_some() {
            return Err(ContextError::Duplicate { type_name: d.type_name });
        }
    }

    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        Visiting,
        Done,
    }

    fn visit(
        d: &'static ComponentDescriptor,
        by_id: &HashMap<TypeId, &'static ComponentDescriptor>,
        seed: &ContextBuilder,
        marks: &mut HashMap<TypeId, Mark>,
        stack: &mut Vec<&'static str>,
        order: &mut Vec<&'static ComponentDescriptor>,
    ) -> Result<(), ContextError> {
        let id = (d.type_id)();
        match marks.get(&id) {
            Some(Mark::Done) => return Ok(()),
            Some(Mark::Visiting) => {
                stack.push(d.type_name);
                return Err(ContextError::Cycle { path: stack.join(" -> ") });
            }
            None => {}
        }
        marks.insert(id, Mark::Visiting);
        stack.push(d.type_name);
        for (dep_id, dep_name) in (d.deps)() {
            if let Some(dep) = by_id.get(&dep_id) {
                visit(dep, by_id, seed, marks, stack, order)?;
            } else if !seed.contains(dep_id) {
                return Err(ContextError::MissingDependency { component: d.type_name, dependency: dep_name });
            }
        }
        stack.pop();
        marks.insert(id, Mark::Done);
        order.push(d);
        Ok(())
    }

    let mut marks = HashMap::new();
    let mut order = Vec::new();
    for d in &descriptors {
        let mut stack = Vec::new();
        visit(d, &by_id, &seed, &mut marks, &mut stack, &mut order)?;
    }

    for d in &order {
        (d.construct)(&mut seed)?;
    }

    Ok((Context { map: seed.map }, order))
}
```

`lib.rs` ergänzen:

```rust
pub mod container;
pub use container::{
    assemble, ComponentDescriptor, ComponentResult, Context, ContextBuilder, ContextError, HookFn, Inject,
};
```

- [ ] **Step 4: Tests laufen lassen — müssen grün sein**

Run: `cargo test -p oxide-core`
Expected: alle Tests PASS (Config + Container).

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-core
git commit -m "feat(core): DI container with descriptors, topological wiring and fail-fast errors"
```

---

### Task 6: `oxide-boot`-Grundgerüst — `__private`, `BootError`, `AppBuilder::start`

Hinweis zur Reihenfolge: Das Boot-Gerüst kommt VOR den Macros, damit die Macro-Tasks (7–9) ihre generierten Pfade (`::oxide_boot::__private`) und die Boot-Integrationstests sofort gegen echte APIs testen können.

**Files:**
- Modify: `crates/oxide-boot/src/lib.rs`
- Create: `crates/oxide-boot/src/app.rs`

**Interfaces:**
- Consumes: `AppConfig`, `assemble`, `ContextBuilder`, `Context`, `ComponentDescriptor`, `ContextError`, `ConfigError` aus oxide-core
- Produces:
  - `pub struct OxideApplication;` mit `pub fn builder() -> AppBuilder` und `pub async fn run() -> Result<(), BootError>` (= `builder().run().await`)
  - `pub struct AppBuilder` mit `pub fn base_dir(self, dir: impl Into<PathBuf>) -> Self`, `pub fn env(self, env: HashMap<String, String>) -> Self` (Default: Prozess-Env), `pub async fn start(self) -> Result<Application, BootError>`, `pub async fn run(self) -> Result<(), BootError>`
  - `pub struct Application` mit `pub fn context(&self) -> &Context`, `pub async fn stop(self) -> Result<(), BootError>`, `pub async fn wait_for_shutdown(self) -> Result<(), BootError>` (ctrl-c/SIGTERM, dann `stop`)
  - `pub enum BootError { Config(ConfigError), Context(ContextError), PostConstruct{..}, PreDestroy{..} }`
  - `start()` macht: Config laden → Logging initialisieren → Banner loggen → `AppConfig` in Seed-Builder legen → inventory-Descriptors einsammeln → `assemble` → `post_construct`-Hooks in Reihenfolge ausführen
  - `prelude`-Modul: `pub use` von `OxideApplication`, `Application`, `BootError`, `Inject`, `ComponentResult`, `AppConfig`

- [ ] **Step 1: Failing Integrationstest schreiben**

`crates/oxide-boot/tests/boot.rs`:

```rust
use oxide_boot::prelude::*;
use oxide_boot::__private::{ComponentDescriptor, Context};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

static STARTED: AtomicBool = AtomicBool::new(false);
static STOPPED: AtomicBool = AtomicBool::new(false);

struct Probe;

inventory::submit! {
    ComponentDescriptor {
        type_name: "Probe",
        type_id: || std::any::TypeId::of::<Probe>(),
        deps: || vec![(std::any::TypeId::of::<AppConfig>(), "AppConfig")],
        construct: |b| {
            b.resolve::<AppConfig>()?;
            b.insert(Probe);
            Ok(())
        },
        post_construct: Some(|_ctx: &Context| Box::pin(async {
            STARTED.store(true, Ordering::SeqCst);
            Ok(())
        })),
        pre_destroy: Some(|_ctx: &Context| Box::pin(async {
            STOPPED.store(true, Ordering::SeqCst);
            Ok(())
        })),
    }
}

#[tokio::test]
async fn boots_reads_config_runs_hooks_and_stops() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), "oxide:\n  application:\n    name: probe-app\n").unwrap();

    let app = OxideApplication::builder()
        .base_dir(dir.path())
        .env(HashMap::new())
        .start()
        .await
        .unwrap();

    let cfg = app.context().get::<AppConfig>().unwrap();
    assert_eq!(cfg.get::<String>("oxide.application.name").unwrap(), "probe-app");
    assert!(app.context().get::<Probe>().is_some());
    assert!(STARTED.load(Ordering::SeqCst));
    assert!(!STOPPED.load(Ordering::SeqCst));

    app.stop().await.unwrap();
    assert!(STOPPED.load(Ordering::SeqCst));
}
```

Dazu in `crates/oxide-boot/Cargo.toml` unter `[dev-dependencies]` ergänzen: `tokio = { workspace = true }` (falls nicht schon durch Task 1 vorhanden) — `inventory` steht bereits in `[dependencies]`, für den Test außerdem als Dev-Dependency nutzbar über `oxide_boot::__private::inventory`; im Test oben stattdessen direkt `inventory.workspace = true` zu den Dev-Dependencies hinzufügen, damit `inventory::submit!` ohne Umweg funktioniert.

- [ ] **Step 2: Test laufen lassen — muss fehlschlagen**

Run: `cargo test -p oxide-boot --test boot`
Expected: FAIL (Compile-Fehler: `prelude`, `OxideApplication` existieren nicht).

- [ ] **Step 3: Implementierung**

`crates/oxide-boot/src/app.rs`:

```rust
use oxide_core::{assemble, AppConfig, ComponentDescriptor, ConfigError, Context, ContextBuilder, ContextError};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum BootError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error("post_construct of component '{component}' failed: {message}")]
    PostConstruct { component: &'static str, message: String },
    #[error("pre_destroy of component '{component}' failed: {message}")]
    PreDestroy { component: &'static str, message: String },
}

pub struct OxideApplication;

impl OxideApplication {
    pub fn builder() -> AppBuilder {
        AppBuilder { base_dir: PathBuf::from("."), env: None }
    }

    /// Boots the application and blocks until SIGTERM / ctrl-c.
    pub async fn run() -> Result<(), BootError> {
        Self::builder().run().await
    }
}

pub struct AppBuilder {
    base_dir: PathBuf,
    env: Option<HashMap<String, String>>,
}

impl AppBuilder {
    pub fn base_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.base_dir = dir.into();
        self
    }

    /// Overrides the process environment (used by tests for isolation).
    pub fn env(mut self, env: HashMap<String, String>) -> Self {
        self.env = Some(env);
        self
    }

    pub async fn start(self) -> Result<Application, BootError> {
        let env = self.env.unwrap_or_else(|| std::env::vars().collect());
        let config = AppConfig::load(&self.base_dir, &env)?;
        init_logging(&config);
        let name: String = config.get_opt("oxide.application.name")?.unwrap_or_else(|| "oxide-app".into());
        tracing::info!(
            application = %name,
            profiles = ?config.profiles(),
            version = env!("CARGO_PKG_VERSION"),
            "Oxide Boot starting"
        );

        let mut seed = ContextBuilder::new();
        seed.insert(config);
        let descriptors: Vec<&'static ComponentDescriptor> = inventory::iter::<ComponentDescriptor>().collect();
        let (context, order) = assemble(descriptors, seed)?;
        let context = Arc::new(context);

        for d in &order {
            if let Some(hook) = d.post_construct {
                hook(&context).await.map_err(|e| BootError::PostConstruct {
                    component: d.type_name,
                    message: e.to_string(),
                })?;
            }
        }
        tracing::info!(components = order.len(), "Oxide Boot started");
        Ok(Application { context, order })
    }

    pub async fn run(self) -> Result<(), BootError> {
        self.start().await?.wait_for_shutdown().await
    }
}

pub struct Application {
    context: Arc<Context>,
    order: Vec<&'static ComponentDescriptor>,
}

impl Application {
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// Runs `pre_destroy` hooks in reverse construction order.
    pub async fn stop(self) -> Result<(), BootError> {
        for d in self.order.iter().rev() {
            if let Some(hook) = d.pre_destroy {
                hook(&self.context).await.map_err(|e| BootError::PreDestroy {
                    component: d.type_name,
                    message: e.to_string(),
                })?;
            }
        }
        tracing::info!("Oxide Boot stopped");
        Ok(())
    }

    /// Blocks until ctrl-c (and on Unix SIGTERM), then shuts down.
    pub async fn wait_for_shutdown(self) -> Result<(), BootError> {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("install SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = ctrl_c.await;
        }
        tracing::info!("shutdown signal received");
        self.stop().await
    }
}

/// Builds an env-filter string like "info,my_target=debug" from
/// `oxide.logging.level.root` and `oxide.logging.level.<target>` keys.
fn init_logging(config: &AppConfig) {
    let mut directives: Vec<String> =
        vec![config.get_opt::<String>("oxide.logging.level.root").ok().flatten().unwrap_or_else(|| "info".into())];
    if let Ok(Some(levels)) = config.get_opt::<HashMap<String, String>>("oxide.logging.level") {
        for (target, level) in levels {
            if target != "root" {
                directives.push(format!("{target}={level}"));
            }
        }
    }
    // try_init: keep the first subscriber if tests boot multiple apps.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(directives.join(","))
        .try_init();
}
```

`crates/oxide-boot/src/lib.rs` erweitern (bestehendes `__private` behalten):

```rust
mod app;

pub use app::{AppBuilder, Application, BootError, OxideApplication};

pub mod prelude {
    pub use crate::{Application, BootError, OxideApplication};
    pub use oxide_core::{AppConfig, ComponentResult, Inject};
}
```

- [ ] **Step 4: Test laufen lassen — muss grün sein**

Run: `cargo test -p oxide-boot --test boot`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-boot
git commit -m "feat(boot): OxideApplication lifecycle with config, logging, hooks and shutdown"
```

---

### Task 7: `#[component]`-Attribut-Macro

**Files:**
- Create: `crates/oxide-macros/src/component.rs`
- Modify: `crates/oxide-macros/src/lib.rs`
- Modify: `crates/oxide-boot/src/lib.rs` (Macro-Re-Export)
- Test: `crates/oxide-boot/tests/boot.rs` (erweitern)

**Interfaces:**
- Consumes: `ComponentDescriptor`, `Inject`, `ContextBuilder`, `Context`, `ComponentResult` über `::oxide_boot::__private`
- Produces:
  - `#[component]` auf Structs mit benannten Feldern. Jedes Feld ohne `#[default]`-Attribut wird zu `Inject<OriginalTyp>` umgeschrieben und beim Konstruieren per `resolve` injiziert; Felder mit `#[default]` behalten ihren Typ und werden mit `Default::default()` befüllt.
  - Optionale Args: `#[component(post_construct = method_name)]`, `pre_destroy = method_name`. Vertrag der Methoden: `async fn name(&self) -> oxide_boot::prelude::ComponentResult<()>`.
  - Registrierung via `inventory::submit!` eines `ComponentDescriptor`.
  - Re-Export in `oxide-boot`: `pub use oxide_macros::component;` (+ im `prelude`).

- [ ] **Step 1: Failing Test schreiben**

`crates/oxide-boot/tests/boot.rs` ergänzen:

```rust
use oxide_boot::component;
use std::sync::atomic::AtomicUsize;

static GREETER_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);

#[component]
struct Clock {
    #[default]
    ticks: std::sync::atomic::AtomicUsize,
}

#[component(post_construct = init)]
struct Greeter {
    clock: Clock,
}

impl Greeter {
    async fn init(&self) -> ComponentResult<()> {
        GREETER_INIT_COUNT.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn greet(&self) -> String {
        self.clock.ticks.fetch_add(1, Ordering::SeqCst);
        "hello".to_string()
    }
}

#[tokio::test]
async fn component_macro_wires_dependencies_and_runs_post_construct() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), "oxide: {}\n").unwrap();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    let greeter = app.context().get::<Greeter>().unwrap();
    assert_eq!(greeter.greet(), "hello");
    assert_eq!(GREETER_INIT_COUNT.load(Ordering::SeqCst), 1);
    app.stop().await.unwrap();
}
```

Wichtig: Das Feld `clock: Clock` im Quelltext wird vom Macro zu `clock: Inject<Clock>` umgeschrieben — der Test greift über Deref (`self.clock.ticks`) zu, das muss transparent funktionieren.

- [ ] **Step 2: Test laufen lassen — muss fehlschlagen**

Run: `cargo test -p oxide-boot --test boot`
Expected: FAIL (Compile-Fehler: `component` existiert nicht).

- [ ] **Step 3: Macro implementieren**

`crates/oxide-macros/src/component.rs`:

```rust
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse::Parser, Field, Fields, Ident, ItemStruct};

pub struct ComponentArgs {
    pub post_construct: Option<Ident>,
    pub pre_destroy: Option<Ident>,
}

pub fn parse_args(args: TokenStream) -> syn::Result<ComponentArgs> {
    let mut out = ComponentArgs { post_construct: None, pre_destroy: None };
    if args.is_empty() {
        return Ok(out);
    }
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("post_construct") {
            out.post_construct = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("pre_destroy") {
            out.pre_destroy = Some(meta.value()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("expected `post_construct = <method>` or `pre_destroy = <method>`"))
        }
    });
    parser.parse2(args)?;
    Ok(out)
}

fn has_default_attr(field: &Field) -> bool {
    field.attrs.iter().any(|a| a.path().is_ident("default"))
}

pub fn expand(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args = parse_args(args)?;
    let mut item: ItemStruct = syn::parse2(item)?;
    let ident = item.ident.clone();

    let Fields::Named(fields) = &mut item.fields else {
        return Err(syn::Error::new_spanned(
            &item.ident,
            "#[component] requires a struct with named fields (unit and tuple structs are not supported)",
        ));
    };

    let mut deps = Vec::new();
    let mut constructors = Vec::new();
    for field in fields.named.iter_mut() {
        let name = field.ident.clone().expect("named field");
        let ty = field.ty.clone();
        if has_default_attr(field) {
            field.attrs.retain(|a| !a.path().is_ident("default"));
            constructors.push(quote! { #name: ::core::default::Default::default() });
        } else {
            deps.push(quote! {
                (::core::any::TypeId::of::<#ty>(), ::core::any::type_name::<#ty>())
            });
            constructors.push(quote! { #name: __builder.resolve::<#ty>()? });
            field.ty = syn::parse_quote! { ::oxide_boot::__private::Inject<#ty> };
        }
    }

    let post_construct = match &args.post_construct {
        Some(method) => quote! {
            ::core::option::Option::Some(|__ctx: &::oxide_boot::__private::Context| {
                let __me = __ctx.get::<#ident>().expect("component was just constructed");
                ::std::boxed::Box::pin(async move { __me.#method().await })
            })
        },
        None => quote! { ::core::option::Option::None },
    };
    let pre_destroy = match &args.pre_destroy {
        Some(method) => quote! {
            ::core::option::Option::Some(|__ctx: &::oxide_boot::__private::Context| {
                let __me = __ctx.get::<#ident>().expect("component exists during shutdown");
                ::std::boxed::Box::pin(async move { __me.#method().await })
            })
        },
        None => quote! { ::core::option::Option::None },
    };

    Ok(quote! {
        #item

        ::oxide_boot::__private::inventory::submit! {
            ::oxide_boot::__private::ComponentDescriptor {
                type_name: stringify!(#ident),
                type_id: || ::core::any::TypeId::of::<#ident>(),
                deps: || <[_]>::into_vec(::std::boxed::Box::new([#(#deps),*])),
                construct: |__builder| {
                    let __value = #ident { #(#constructors),* };
                    __builder.insert(__value);
                    ::core::result::Result::Ok(())
                },
                post_construct: #post_construct,
                pre_destroy: #pre_destroy,
            }
        }
    })
}
```

`crates/oxide-macros/src/lib.rs`:

```rust
mod component;

use proc_macro::TokenStream;

/// Marks a struct as an injectable singleton component.
#[proc_macro_attribute]
pub fn component(args: TokenStream, item: TokenStream) -> TokenStream {
    component::expand(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
```

`crates/oxide-boot/src/lib.rs` ergänzen:

```rust
pub use oxide_macros::component;
```

und im `prelude`: `pub use oxide_macros::component;`

- [ ] **Step 4: Test laufen lassen — muss grün sein**

Run: `cargo test -p oxide-boot --test boot`
Expected: PASS (beide Tests).

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-macros crates/oxide-boot
git commit -m "feat(macros): #[component] attribute with field injection and lifecycle hooks"
```

---

### Task 8: `#[derive(ConfigProperties)]`

**Files:**
- Create: `crates/oxide-macros/src/config_properties.rs`
- Modify: `crates/oxide-macros/src/lib.rs`
- Modify: `crates/oxide-boot/src/lib.rs` (Re-Export)
- Test: `crates/oxide-boot/tests/boot.rs` (erweitern)

**Interfaces:**
- Consumes: `AppConfig`, `ComponentDescriptor`, `ContextBuilder`, `ContextError` über `__private`
- Produces:
  - `#[derive(ConfigProperties)]` + `#[config(prefix = "some.prefix")]` auf Structs, die `serde::Deserialize` implementieren. Registriert einen Descriptor mit Dependency auf `AppConfig`; `construct` deserialisiert den Subtree unter `prefix` und legt das Struct als Component ab. Fehlender Key/Shape-Fehler → `ContextError::Construct` mit Prefix und Ursache in der Message.
  - Fehlendes `#[config(prefix = ...)]` → Compile-Fehler „missing #[config(prefix = \"...\")] attribute“.
  - Re-Export: `pub use oxide_macros::ConfigProperties;` in `oxide-boot` + `prelude`.

- [ ] **Step 1: Failing Test schreiben**

`crates/oxide-boot/tests/boot.rs` ergänzen:

```rust
use oxide_boot::ConfigProperties;

#[derive(ConfigProperties, serde::Deserialize)]
#[config(prefix = "billing")]
struct BillingProps {
    url: String,
    #[serde(default)]
    retries: u32,
}

#[component]
struct BillingGateway {
    props: BillingProps,
}

#[tokio::test]
async fn config_properties_are_bound_and_injectable() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), "billing:\n  url: \"http://localhost:8081\"\n").unwrap();
    let app = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap();

    let gw = app.context().get::<BillingGateway>().unwrap();
    assert_eq!(gw.props.url, "http://localhost:8081");
    assert_eq!(gw.props.retries, 0);
    app.stop().await.unwrap();
}
```

Achtung: Die bestehenden Tests aus Task 6/7 nutzen ein tempdir OHNE `billing:`-Key — deren Boot schlägt jetzt fehl, weil `BillingProps` global registriert ist. Lösung im selben Step: alle Boot-Tests in dieser Datei nutzen eine gemeinsame Helper-Funktion, die ein tempdir mit ALLEN von Testkomponenten benötigten Keys schreibt:

```rust
fn test_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("application.yml"),
        "oxide:\n  application:\n    name: probe-app\nbilling:\n  url: \"http://localhost:8081\"\n",
    )
    .unwrap();
    dir
}
```

Bestehende Tests auf `test_config_dir()` umstellen.

- [ ] **Step 2: Test laufen lassen — muss fehlschlagen**

Run: `cargo test -p oxide-boot --test boot`
Expected: FAIL (Compile-Fehler: `ConfigProperties` existiert nicht).

- [ ] **Step 3: Macro implementieren**

`crates/oxide-macros/src/config_properties.rs`:

```rust
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitStr};

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let ident = &input.ident;

    let mut prefix: Option<LitStr> = None;
    for attr in &input.attrs {
        if attr.path().is_ident("config") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("prefix") {
                    prefix = Some(meta.value()?.parse()?);
                    Ok(())
                } else {
                    Err(meta.error("expected `prefix = \"...\"`"))
                }
            })?;
        }
    }
    let Some(prefix) = prefix else {
        return Err(syn::Error::new_spanned(
            ident,
            "missing #[config(prefix = \"...\")] attribute on ConfigProperties struct",
        ));
    };

    Ok(quote! {
        ::oxide_boot::__private::inventory::submit! {
            ::oxide_boot::__private::ComponentDescriptor {
                type_name: stringify!(#ident),
                type_id: || ::core::any::TypeId::of::<#ident>(),
                deps: || <[_]>::into_vec(::std::boxed::Box::new([(
                    ::core::any::TypeId::of::<::oxide_boot::__private::AppConfig>(),
                    "AppConfig",
                )])),
                construct: |__builder| {
                    let __config = __builder.resolve::<::oxide_boot::__private::AppConfig>()?;
                    let __props: #ident = __config.get(#prefix).map_err(|e| {
                        ::oxide_boot::__private::ContextError::Construct {
                            type_name: stringify!(#ident),
                            message: ::std::format!("binding prefix '{}': {}", #prefix, e),
                        }
                    })?;
                    __builder.insert(__props);
                    ::core::result::Result::Ok(())
                },
                post_construct: ::core::option::Option::None,
                pre_destroy: ::core::option::Option::None,
            }
        }
    })
}
```

`crates/oxide-macros/src/lib.rs` ergänzen:

```rust
mod config_properties;

/// Binds a config subtree (`#[config(prefix = "...")]`) to this struct and
/// registers it as an injectable component. Requires `serde::Deserialize`.
#[proc_macro_derive(ConfigProperties, attributes(config))]
pub fn config_properties(input: TokenStream) -> TokenStream {
    config_properties::expand(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
```

`oxide-boot/src/lib.rs` + `prelude`: `pub use oxide_macros::ConfigProperties;`

- [ ] **Step 4: Tests laufen lassen — müssen grün sein**

Run: `cargo test -p oxide-boot --test boot`
Expected: alle Tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-macros crates/oxide-boot
git commit -m "feat(macros): #[derive(ConfigProperties)] with prefix binding as component"
```

---

### Task 9: `#[oxide_boot::main]` + trybuild-UI-Tests

**Files:**
- Create: `crates/oxide-macros/src/main_macro.rs`
- Modify: `crates/oxide-macros/src/lib.rs`, `crates/oxide-boot/src/lib.rs`
- Create: `crates/oxide-boot/tests/ui.rs`, `crates/oxide-boot/tests/ui/tuple_struct_component.rs`, `crates/oxide-boot/tests/ui/tuple_struct_component.stderr`, `crates/oxide-boot/tests/ui/config_props_without_prefix.rs`, `crates/oxide-boot/tests/ui/config_props_without_prefix.stderr`

**Interfaces:**
- Consumes: Macros aus Task 7/8
- Produces:
  - `#[oxide_boot::main]` auf `async fn main()` — expandiert zu synchronem `main`, das eine Tokio-Multi-Thread-Runtime baut und den Body per `block_on` ausführt.
  - trybuild-Tests, die die Verständlichkeit der Compile-Fehlermeldungen festschreiben (Kernversprechen der Spec, Abschnitt 15).

- [ ] **Step 1: Macro implementieren (kein Runtime-Test möglich, Verhalten wird über Hello-World in Task 10 verifiziert)**

`crates/oxide-macros/src/main_macro.rs`:

```rust
use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

pub fn expand(item: TokenStream) -> syn::Result<TokenStream> {
    let func: ItemFn = syn::parse2(item)?;
    if func.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(&func.sig.fn_token, "#[oxide_boot::main] requires an async fn"));
    }
    let body = &func.block;
    let output = &func.sig.output;
    Ok(quote! {
        fn main() #output {
            ::oxide_boot::__private::tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime")
                .block_on(async #body)
        }
    })
}
```

`lib.rs` (macros): `mod main_macro;` +

```rust
/// Entry point: builds a multi-threaded Tokio runtime and runs the async body.
#[proc_macro_attribute]
pub fn main(_args: TokenStream, item: TokenStream) -> TokenStream {
    main_macro::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
```

`oxide-boot/src/lib.rs`: `pub use oxide_macros::main;` (NICHT ins `prelude` — Aufruf erfolgt qualifiziert als `#[oxide_boot::main]`).

- [ ] **Step 2: trybuild-Fälle schreiben**

`crates/oxide-boot/tests/ui.rs`:

```rust
#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
```

`crates/oxide-boot/tests/ui/tuple_struct_component.rs`:

```rust
use oxide_boot::component;

#[component]
struct Broken(String);

fn main() {}
```

`crates/oxide-boot/tests/ui/config_props_without_prefix.rs`:

```rust
use oxide_boot::ConfigProperties;

#[derive(ConfigProperties, serde::Deserialize)]
struct NoPrefix {
    value: String,
}

fn main() {}
```

Hinweis: Das UI-Testcrate braucht `serde` — steht bereits in den `[dependencies]` von `oxide-boot`; trybuild-Fälle kompilieren im Kontext von `oxide-boot`s Dev-Dependencies.

- [ ] **Step 3: Erwartete stderr-Dateien generieren**

Run: `TRYBUILD=overwrite cargo test -p oxide-boot --test ui`
Expected: Läuft durch und schreibt `*.stderr`-Dateien. Danach die `.stderr`-Dateien MANUELL PRÜFEN: enthalten sie die Meldungen „requires a struct with named fields“ bzw. „missing #[config(prefix = ...)]“? Wenn nicht, Macro-Fehlermeldung nachschärfen.

- [ ] **Step 4: Tests laufen lassen — müssen grün sein**

Run: `cargo test -p oxide-boot`
Expected: `boot`- und `ui`-Tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/oxide-macros crates/oxide-boot
git commit -m "feat(macros): #[oxide_boot::main] entrypoint plus trybuild UI tests for macro errors"
```

---

### Task 10: Hello-World-Beispiel + README

**Files:**
- Modify: `Cargo.toml` (Workspace-Member `examples/hello-world`)
- Create: `examples/hello-world/Cargo.toml`, `examples/hello-world/application.yml`, `examples/hello-world/src/main.rs`, `examples/hello-world/tests/hello.rs`
- Create: `README.md`

**Interfaces:**
- Consumes: gesamte Phase-1-API (`prelude`, `#[component]`, `ConfigProperties`, `#[oxide_boot::main]`)
- Produces: lauffähiges Referenzbeispiel; Integrationstest, der den kompletten Boot ohne Signale testet.

- [ ] **Step 1: Beispiel anlegen**

Workspace-`Cargo.toml`: `"examples/hello-world"` zu `members` hinzufügen.

`examples/hello-world/Cargo.toml`:

```toml
[package]
name = "hello-world"
version.workspace = true
edition.workspace = true
publish = false

[dependencies]
oxide-boot.workspace = true
serde.workspace = true
tracing.workspace = true

[dev-dependencies]
tokio.workspace = true
```

`examples/hello-world/application.yml`:

```yaml
oxide:
  application:
    name: hello-world
  logging:
    level:
      root: info

greeter:
  name: World
  excited: true
```

`examples/hello-world/src/main.rs`:

```rust
use oxide_boot::prelude::*;
use oxide_boot::{component, ConfigProperties};

#[derive(ConfigProperties, serde::Deserialize)]
#[config(prefix = "greeter")]
pub struct GreeterProps {
    pub name: String,
    #[serde(default)]
    pub excited: bool,
}

#[component(post_construct = say_hello)]
pub struct Greeter {
    props: GreeterProps,
}

impl Greeter {
    pub fn greeting(&self) -> String {
        let punctuation = if self.props.excited { "!" } else { "." };
        format!("Hello, {}{punctuation}", self.props.name)
    }

    async fn say_hello(&self) -> ComponentResult<()> {
        tracing::info!("{}", self.greeting());
        Ok(())
    }
}

#[oxide_boot::main]
async fn main() {
    if let Err(e) = OxideApplication::run().await {
        eprintln!("boot failed: {e}");
        std::process::exit(1);
    }
}
```

`examples/hello-world/tests/hello.rs`:

```rust
use hello_world::*; // requires src/lib.rs re-export; see below
```

Damit der Integrationstest die Typen sieht, das Beispiel als lib+bin strukturieren: Inhalt von `main.rs` bis auf `main()` nach `src/lib.rs` verschieben, `main.rs` importiert `hello_world::*`. Test:

```rust
use hello_world::Greeter;
use oxide_boot::prelude::*;
use std::collections::HashMap;

#[tokio::test]
async fn boots_and_greets_from_yaml() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let app = OxideApplication::builder().base_dir(dir).env(HashMap::new()).start().await.unwrap();
    let greeter = app.context().get::<Greeter>().unwrap();
    assert_eq!(greeter.greeting(), "Hello, World!");
    app.stop().await.unwrap();
}
```

- [ ] **Step 2: Test + manueller Lauf**

Run: `cargo test -p hello-world`
Expected: PASS.

Run: `cd examples/hello-world && timeout 5 cargo run; cd ../..`
Expected: Banner-Log „Oxide Boot starting“, dann „Hello, World!“, Prozess läuft bis zum timeout-Signal und loggt beim Beenden „shutdown signal received“ + „Oxide Boot stopped“. (`timeout` schickt SIGTERM — genau der K8s-Pfad.)

- [ ] **Step 3: README schreiben**

`README.md`:

```markdown
# Oxide Boot

A Spring-Boot-like application framework for Rust: components, dependency
injection, `application.yml` with profiles, and an opinionated boot lifecycle —
built on Tokio and battle-tested crates.

**Status: Phase 1 (foundation).** Web, MongoDB, Kafka, HTTP client and
observability starters are planned — see `docs/design/specs/`.

## Quick start

​```rust
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
​```

With `application.yml`:

​```yaml
greeter:
  name: World
​```

Run the example: `cargo run -p hello-world`

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
```

(Die ``​``-Zeichen im Block oben sind normale Backticks im echten README.)

- [ ] **Step 4: Gesamter Workspace grün**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: alle Tests PASS, Clippy ohne Warnungen (auftretende Lints beheben, nicht per `allow` wegdrücken, außer bei False-Positives mit Begründungskommentar).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml examples/ README.md
git commit -m "feat: hello-world example app and README with Spring-to-Oxide cheat sheet"
```

---

## Abschluss-Check Phase 1

- [ ] `cargo test --workspace` grün, `cargo clippy --workspace --all-targets -- -D warnings` sauber
- [ ] `cargo run -p hello-world` bootet, grüßt, fährt bei SIGTERM sauber herunter
- [ ] Spec-Abgleich: Abschnitt 4 (DI), 5 (Konfiguration) und die Phase-1-Zeile aus Abschnitt 14 sind abgedeckt; Abweichungen dokumentieren
- [ ] Bekannte, bewusste Abweichungen von der Spec (in Folge-Phasen nachziehen): (1) Trait-Injection `#[component(binds = MyTrait)]` ist NICHT in Phase 1 — kommt zusammen mit dem Mocking-Support von `oxide-test`; (2) Lifecycle-Hooks als Macro-Args `#[component(post_construct = m, pre_destroy = m)]` statt eigenständiger `#[post_construct]`-Attribute (einfacher zu implementieren, gleiche Ausdruckskraft)
- [ ] Danach: Plan für Phase 2 (Web + Observability) schreiben — mit den Erkenntnissen aus dieser Phase
