//! Mongo health indicator: reports `/actuator/health` component `mongo`.
//!
//! Registered only when `MongoProps` is in the context (the register seam
//! returns `None` otherwise). The check pings the primary within a short
//! timeout and reports `Up`/`Down{detail}`.

use crate::{MongoClient, MongoProps};
use oxide_core::Context;
use oxide_web::{HealthIndicator, HealthStatus, RegisteredCheck};

/// The per-request budget for the ping (matches the health-check timeout).
const PING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

fn register(ctx: &Context) -> Option<RegisteredCheck> {
    let props = (*ctx.get::<MongoProps>()?).clone();
    let client = ctx.get::<MongoClient>()?;
    Some(Box::new(move || {
        let client = client.clone();
        let _ = &props;
        Box::pin(async move {
            match tokio::time::timeout(PING_TIMEOUT, client.ping()).await {
                Ok(Ok(())) => HealthStatus::Up,
                Ok(Err(e)) => HealthStatus::Down { detail: e.to_string() },
                Err(_) => HealthStatus::Down { detail: "timed out".to_string() },
            }
        })
    }))
}

inventory::submit! {
    HealthIndicator {
        name: "mongo",
        register,
    }
}