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
            Value::Mapping(map) => map.get(&Value::String(segment.to_string())),
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
}
