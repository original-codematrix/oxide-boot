use serde::de::DeserializeOwned;
use serde_yaml_ng::Value;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid YAML in {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_yaml_ng::Error,
    },
    #[error("missing configuration key '{path}'")]
    Missing { path: String },
    #[error("configuration key '{path}' has an unexpected shape: {source}")]
    Deserialize {
        path: String,
        #[source]
        source: serde_yaml_ng::Error,
    },
    #[error("unresolvable placeholder '${{{placeholder}}}' (no such key, no default)")]
    Placeholder { placeholder: String },
}

/// Registered by `#[derive(ConfigProperties)]` for every struct carrying
/// `#[config(prefix = "...", default)]`. `AppConfig::load` merges each
/// entry's serialized `Default` into the config tree under `prefix`
/// *before* `apply_env_overrides` runs, so multi-segment kebab-case keys
/// (e.g. `request-timeout-seconds`) exist as leaves even when the section
/// is absent from YAML entirely — otherwise `set_existing`'s "bind only to
/// an already-existing key" rule (pinned in Phase 1, see the tests below)
/// has nothing to bind to, and the override silently creates a dead nested
/// structure instead (`server.request.timeout.seconds`).
///
/// Existing YAML values always win over a seeded default — seeding only
/// fills in keys that are still absent.
#[derive(Debug)]
pub struct ConfigDefaults {
    pub prefix: &'static str,
    /// Serializes the struct's `Default` value as a YAML mapping (kebab-case
    /// keys, matching what deserialization expects).
    pub defaults_yaml: fn() -> String,
}

inventory::collect!(ConfigDefaults);

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
        seed_defaults(&mut root);
        apply_env_overrides(&mut root, env);
        resolve_placeholders(&mut root)?;
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
        lookup_path(&self.root, path)
    }

    /// Resolves `oxide.application.name`, falling back to `"oxide-app"` when
    /// the key is absent. Shared by every call site that needs the
    /// application's display name (boot's startup log line, the OTel
    /// `service.name` resource, `/actuator/info`) — previously each
    /// resolved the same key/fallback independently.
    pub fn application_name(&self) -> Result<String, ConfigError> {
        Ok(self.get_opt("oxide.application.name")?.unwrap_or_else(|| "oxide-app".into()))
    }
}

/// Case-insensitive comparison key: lowercased, `-` and `_` stripped.
fn normalize(key: &str) -> String {
    key.chars().filter(|c| *c != '-' && *c != '_').collect::<String>().to_ascii_lowercase()
}

fn apply_env_overrides(root: &mut Value, env: &HashMap<String, String>) {
    let mut matching: Vec<(&String, &String)> =
        env.iter().filter(|(name, _)| name.starts_with("OXIDE_") && name.as_str() != "OXIDE_PROFILES_ACTIVE").collect();
    matching.sort_by_key(|(name, _)| name.as_str());
    for (name, raw) in matching {
        let segments: Vec<String> = name.split('_').map(|s| s.to_ascii_lowercase()).collect();
        // Parse the value as a YAML scalar so "9090" becomes a number, "true" a bool.
        let value: Value = serde_yaml_ng::from_str(raw).unwrap_or_else(|_| Value::String(raw.clone()));
        // 1. Framework namespace against existing keys (`oxide` is the first segment).
        if set_env(root, &segments, value.clone(), BindMode::Existing, 0) {
            continue;
        }
        // 2. Stripped prefix against existing keys — makes app-level config env-overridable.
        if segments.len() > 1 && set_env(root, &segments[1..], value.clone(), BindMode::Existing, 0) {
            continue;
        }
        // 3. Nothing matches: create under the framework namespace (pre-2a behavior).
        set_env(root, &segments, value, BindMode::Relaxed, 0);
    }
}

/// Controls how [`set_env`] is allowed to grow the tree while binding an
/// env-var override.
#[derive(Clone, Copy)]
enum BindMode {
    /// Bind only along EXISTING keys (greedy longest match per level).
    /// Intermediate levels must already exist; only the final leaf may be
    /// newly inserted, and only once at least one existing level was
    /// descended (`depth > 0`).
    Existing,
    /// Freely create mapping levels as needed, one per segment
    /// (pre-2a behavior).
    Relaxed,
}

/// Walks/creates the tree for the given env-var segments. At each level it
/// greedily tries to match the longest run of segments against an existing
/// key (normalized), so `KAFKA_BOOTSTRAP_SERVERS` binds to `bootstrap-servers`.
/// In [`BindMode::Relaxed`], unknown paths are created with one mapping level
/// per segment. In [`BindMode::Existing`], unknown paths fail to bind unless
/// at least one existing level was already descended (`depth > 0`), in which
/// case the single remaining leaf may still be created. Returns whether the
/// value was bound (always `true` in `Relaxed` mode).
///
/// **Precedence:** If an override path must descend through an existing non-mapping
/// (scalar or sequence), that value is replaced by a mapping to allow the env var
/// to set nested keys. This implements the Spring principle: env overrides always win.
fn set_env(node: &mut Value, segments: &[String], value: Value, mode: BindMode, depth: usize) -> bool {
    if segments.is_empty() {
        return false;
    }
    if !matches!(node, Value::Mapping(_)) {
        // Reached via an existing key that holds a scalar: env wins, convert.
        if matches!(mode, BindMode::Existing) && depth == 0 {
            return false;
        }
        *node = Value::Mapping(Default::default());
        if matches!(mode, BindMode::Existing) {
            set_env(node, segments, value, BindMode::Relaxed, depth);
            return true;
        }
        // Relaxed: fall through and keep matching against the fresh mapping.
    }
    let Value::Mapping(map) = node else { unreachable!() };

    for take in (1..=segments.len()).rev() {
        let candidate = normalize(&segments[..take].join(""));
        let existing = map.keys().find(|k| matches!(k, Value::String(s) if normalize(s) == candidate)).cloned();
        if let Some(key) = existing {
            if take == segments.len() {
                map.insert(key, value);
                return true;
            }
            // `key` came from `map.keys().find(...)` on this same `map`
            // two lines above, so `map.get_mut(&key)` cannot miss.
            let child = map.get_mut(&key).expect("key just found");
            return set_env(child, &segments[take..], value, mode, depth + 1);
        }
    }

    match mode {
        BindMode::Relaxed => {
            let head = Value::String(segments[0].clone());
            if segments.len() == 1 {
                map.insert(head, value);
            } else {
                if map.get(&head).is_none() {
                    map.insert(head.clone(), Value::Mapping(Default::default()));
                }
                // `head` is now guaranteed present: either it already existed, or
                // the `insert` two lines above just put it there.
                set_env(map.get_mut(&head).expect("just inserted"), &segments[1..], value, mode, depth + 1);
            }
            true
        }
        // No existing key matched at this level: the leaf may be created here,
        // but only if we already descended through at least one existing level.
        BindMode::Existing if segments.len() == 1 && depth > 0 => {
            map.insert(Value::String(segments[0].clone()), value);
            true
        }
        BindMode::Existing => false,
    }
}

/// Merges every registered [`ConfigDefaults`] entry into `root`, under its
/// `prefix`, before env overrides run. A malformed `defaults_yaml()` (should
/// be unreachable — it round-trips a real `Default` value) is skipped rather
/// than failing config load.
fn seed_defaults(root: &mut Value) {
    for entry in inventory::iter::<ConfigDefaults> {
        let Ok(defaults) = serde_yaml_ng::from_str::<Value>(&(entry.defaults_yaml)()) else {
            continue;
        };
        if let Some(node) = seed_path(root, entry.prefix) {
            merge_seeded_defaults(node, defaults);
        }
    }
}

/// Walks `root` along the dot-separated `path`, creating mapping levels for
/// segments that don't exist yet (as `Value::Null` placeholders, resolved by
/// `merge_seeded_defaults`). Returns `None` if an existing segment along the
/// way is a scalar/sequence rather than a mapping — seeding is skipped
/// there; the user's existing value wins outright.
fn seed_path<'a>(root: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    let mut node = root;
    for segment in path.split('.') {
        match node {
            Value::Null => *node = Value::Mapping(Default::default()),
            Value::Mapping(_) => {}
            _ => return None,
        }
        let Value::Mapping(map) = node else { unreachable!() };
        node = map.entry(Value::String(segment.to_string())).or_insert(Value::Null);
    }
    Some(node)
}

/// Fills `base` in with keys from `overlay` (a seeded default) that are not
/// already present. Existing YAML values always win: a key present in
/// `base` is left untouched (recursing into it if both sides are mappings),
/// and a `base` that is already a non-mapping scalar/sequence is left alone
/// entirely, even if the default would have been a mapping.
fn merge_seeded_defaults(base: &mut Value, overlay: Value) {
    match base {
        Value::Null => *base = overlay,
        Value::Mapping(base_map) => {
            if let Value::Mapping(overlay_map) = overlay {
                for (key, value) in overlay_map {
                    match base_map.get_mut(&key) {
                        Some(existing) => merge_seeded_defaults(existing, value),
                        None => {
                            base_map.insert(key, value);
                        }
                    }
                }
            }
        }
        _ => {}
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

/// Resolves `${key.path}` / `${key.path:default}` inside all string scalars.
/// Runs up to 5 passes so placeholders may reference values that themselves
/// contain placeholders. After all passes, scans the tree for any remaining
/// `${...}` patterns and returns ConfigError::Placeholder if found (catches
/// cycles, self-references, and budget exhaustion).
fn resolve_placeholders(root: &mut Value) -> Result<(), ConfigError> {
    for _ in 0..5 {
        let snapshot = root.clone();
        let mut changed = false;
        substitute_strings(root, &snapshot, &mut changed)?;
        if !changed {
            break;
        }
    }
    // Check for any remaining unresolved placeholders (cycles, self-references, budget exhaustion)
    check_resolved(root)?;
    Ok(())
}

/// Recursively scans the tree for any string still containing `${`.
/// If found, returns ConfigError::Placeholder with the unresolved placeholder text.
fn check_resolved(node: &Value) -> Result<(), ConfigError> {
    match node {
        Value::Mapping(map) => {
            for (_, v) in map.iter() {
                check_resolved(v)?;
            }
        }
        Value::Sequence(seq) => {
            for v in seq.iter() {
                check_resolved(v)?;
            }
        }
        Value::String(s) if s.contains("${") => {
            // Extract the first placeholder from the string for the error message
            if let Some(start) = s.find("${") {
                let after = &s[start + 2..];
                if let Some(end) = after.find('}') {
                    let placeholder = &after[..end];
                    return Err(ConfigError::Placeholder { placeholder: placeholder.to_string() });
                } else {
                    // Malformed: no closing `}`
                    return Err(ConfigError::Placeholder { placeholder: after.to_string() });
                }
            }
        }
        _ => {}
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

#[cfg(test)]
mod tests {
    use crate::config::{AppConfig, ConfigError};
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
        assert!(cfg.get::<bool>("oxide.feature.enabled").unwrap());
    }

    #[test]
    fn non_oxide_env_vars_and_profiles_var_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let env = HashMap::from([("PATH".into(), "/usr/bin".into()), ("OXIDE_PROFILES_ACTIVE".into(), "prod".into())]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();
        assert!(cfg.get_opt::<String>("path").unwrap().is_none());
        assert!(cfg.get_opt::<String>("oxide.profiles.active").unwrap().is_none());
    }

    #[test]
    fn env_var_replaces_scalar_when_descending_through_it_env_wins_by_design() {
        let dir = tempfile::tempdir().unwrap();
        // Start with a scalar value at oxide.server
        write(dir.path(), "application.yml", "oxide:\n  server: \"legacy-connection-string\"\n");
        // Env var sets a nested key under oxide.server
        let env = HashMap::from([("OXIDE_SERVER_PORT".into(), "9090".into())]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();

        // Env override applies: the new nested key is created
        assert_eq!(cfg.get::<u16>("oxide.server.port").unwrap(), 9090);

        // Original scalar is replaced by a mapping; oxide.server is now a mapping {port: 9090},
        // not the scalar "legacy-connection-string". Attempting to deserialize as string fails.
        assert!(matches!(cfg.get::<String>("oxide.server"), Err(ConfigError::Deserialize { .. })));
    }

    #[test]
    fn placeholder_resolves_against_other_key() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "application.yml",
            "oxide:\n  application:\n    name: user-service\nkafka-group: \"${oxide.application.name}-group\"\n",
        );
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
        write(dir.path(), "application.yml", "oxide:\n  greeter:\n    name: framework\ngreeter:\n  name: app\n");
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
        assert!(cfg.get::<bool>("greeter.excited").unwrap());
    }

    #[test]
    fn self_referencing_placeholder_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "a: \"${a}\"\n");
        let err = AppConfig::load(dir.path(), &HashMap::new()).unwrap_err();
        assert!(matches!(err, ConfigError::Placeholder { .. }), "was: {err}");
    }

    #[test]
    fn mutually_referencing_placeholders_are_an_error() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "a: \"${b}\"\nb: \"${a}\"\n");
        let err = AppConfig::load(dir.path(), &HashMap::new()).unwrap_err();
        assert!(matches!(err, ConfigError::Placeholder { .. }), "was: {err}");
    }

    // Test-only ConfigDefaults registration mirroring what
    // `#[derive(ConfigProperties)]` generates for a `default`-marked struct:
    // a hyphenated key (`request-timeout-seconds`) that exists ONLY because
    // it was seeded from the struct's `Default`, never written in YAML.
    #[derive(Debug, serde::Serialize, Default)]
    struct FakeServerProps {
        #[serde(rename = "request-timeout-seconds")]
        request_timeout_seconds: u64,
    }

    fn fake_server_defaults_yaml() -> String {
        serde_yaml_ng::to_string(&FakeServerProps { request_timeout_seconds: 30 }).unwrap()
    }

    inventory::submit! {
        crate::config::ConfigDefaults {
            prefix: "oxide.server",
            defaults_yaml: fake_server_defaults_yaml,
        }
    }

    #[test]
    fn env_override_binds_to_a_hyphenated_key_that_only_exists_as_a_seeded_default() {
        let dir = tempfile::tempdir().unwrap();
        // No `oxide.server` section at all in YAML — the hyphenated key only
        // exists once the seeded default is merged in.
        write(dir.path(), "application.yml", "oxide:\n  application:\n    name: demo\n");
        let env = HashMap::from([("OXIDE_SERVER_REQUEST_TIMEOUT_SECONDS".into(), "5".into())]);
        let cfg = AppConfig::load(dir.path(), &env).unwrap();

        // Must bind to the hyphenated leaf, not create a dead nested structure.
        assert_eq!(cfg.get::<u64>("oxide.server.request-timeout-seconds").unwrap(), 5);
        assert!(cfg.get_opt::<u64>("oxide.server.request.timeout.seconds").unwrap().is_none());
    }

    #[test]
    fn existing_yaml_value_wins_over_seeded_default() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "application.yml", "oxide:\n  server:\n    request-timeout-seconds: 99\n");
        let cfg = AppConfig::load(dir.path(), &HashMap::new()).unwrap();
        assert_eq!(cfg.get::<u64>("oxide.server.request-timeout-seconds").unwrap(), 99);
    }
}
