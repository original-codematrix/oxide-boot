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
}

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
///
/// **Precedence:** If an override path must descend through an existing non-mapping
/// (scalar or sequence), that value is replaced by a mapping to allow the env var
/// to set nested keys. This implements the Spring principle: env overrides always win.
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
        let env = HashMap::from([
            ("PATH".into(), "/usr/bin".into()),
            ("OXIDE_PROFILES_ACTIVE".into(), "prod".into()),
        ]);
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
}
