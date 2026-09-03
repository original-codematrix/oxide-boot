//! Redis health indicator: reports `/actuator/health` component `redis`.
//!
//! Registered only when `RedisProps` is in the context (the register seam
//! returns `None` otherwise). The check PINGs the server and reports
//! `Up`/`Down{detail}`. The per-command timeout configured on the template
//! already bounds the call, so there is no second timeout here.

use crate::{RedisProps, RedisTemplate};
use oxide_core::Context;
use oxide_web::{HealthIndicator, HealthStatus, RegisteredCheck, simple_indicator};

fn register(ctx: &Context) -> Option<RegisteredCheck> {
    let _props = ctx.get::<RedisProps>()?;
    let template = ctx.get::<RedisTemplate>()?;
    Some(simple_indicator(move || {
        let template = template.clone();
        async move {
            match template.ping().await {
                Ok(()) => HealthStatus::Up,
                Err(crate::Error::Timeout(d)) => HealthStatus::Down { detail: format!("timed out after {d:?}") },
                Err(e) => {
                    // Driver internals (and the URL's credentials) stay out of
                    // `detail`, which the health endpoint surfaces; the real
                    // error still goes to the logs.
                    tracing::warn!(error = %e, "redis health check failed");
                    HealthStatus::Down { detail: "ping failed".to_string() }
                }
            }
        }
    }))
}

inventory::submit! {
    HealthIndicator {
        name: "redis",
        register,
    }
}
