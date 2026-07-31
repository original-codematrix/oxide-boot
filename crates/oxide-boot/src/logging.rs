//! Extension seam for logging setup. Starter crates (e.g. `oxide-observability`)
//! register a [`LoggingInitializer`] to take over logging setup; oxide-boot
//! itself registers a plain-fmt fallback at the lowest priority so the
//! application always has *some* logging configured.

use oxide_core::AppConfig;
use std::collections::HashMap;

/// Extension seam: starter crates register one of these to take over logging
/// setup. The single highest-priority initializer runs during boot, before the
/// DI container is assembled. The built-in plain-fmt setup registers itself at
/// priority 0.
pub struct LoggingInitializer {
    pub priority: i32,
    pub init: fn(&AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
}

inventory::collect!(LoggingInitializer);

/// Runs the single highest-priority registered [`LoggingInitializer`].
///
/// The `expect` below is unreachable by construction: oxide-boot always
/// registers the built-in fallback at priority 0 (see the `inventory::submit!`
/// at the bottom of this file), so `inventory::iter` is never empty.
pub(crate) fn run_highest_priority(config: &AppConfig) -> Result<(), crate::BootError> {
    let winner = inventory::iter::<LoggingInitializer>()
        .max_by_key(|i| i.priority)
        .expect("built-in fallback initializer is always registered");
    (winner.init)(config).map_err(|e| crate::BootError::Logging { message: e.to_string() })
}

/// Builds an env-filter string like "info,my_target=debug" from
/// `oxide.logging.level.root` and `oxide.logging.level.<target>` keys.
pub fn logging_directives(config: &AppConfig) -> String {
    let mut directives: Vec<String> =
        vec![config.get_opt::<String>("oxide.logging.level.root").ok().flatten().unwrap_or_else(|| "info".into())];
    if let Ok(Some(levels)) = config.get_opt::<HashMap<String, String>>("oxide.logging.level") {
        for (target, level) in levels {
            if target != "root" {
                directives.push(format!("{target}={level}"));
            }
        }
    }
    directives.join(",")
}

inventory::submit! {
    LoggingInitializer { priority: 0, init: crate::app::fallback_fmt_init }
}
