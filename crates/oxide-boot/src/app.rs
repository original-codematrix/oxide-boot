//! Application lifecycle: config loading, logging init, DI assembly, and
//! ordered `post_construct` / `pre_destroy` hook execution.

use oxide_core::{AppConfig, ComponentDescriptor, ConfigError, Context, ContextBuilder, ContextError, assemble};
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
    #[error("logging initialization failed: {message}")]
    Logging { message: String },
    #[error("failed to install signal handler: {message}")]
    Signal { message: String },
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

    /// Boots the application: loads config, initializes logging, assembles
    /// the DI container, then runs `post_construct` hooks in construction
    /// order.
    ///
    /// Rollback guarantee: if a `post_construct` hook fails, `start()` does
    /// not return immediately with the still-running components left
    /// dangling (e.g. a management server that would otherwise linger until
    /// process exit). Instead, before returning the error, it runs the
    /// `pre_destroy` hooks of exactly those components whose
    /// `post_construct` already succeeded, using the same ordering as
    /// [`Application::stop`] (`destroy_order` asc, then reverse construction
    /// order) restricted to that subset. Rollback hook failures are logged
    /// via `tracing::error!` but do not change the returned `BootError` —
    /// the caller always sees the original `post_construct` failure.
    pub async fn start(self) -> Result<Application, BootError> {
        // `vars_os()` + lossy conversion: `std::env::vars()` panics outright
        // if any process environment variable is not valid UTF-8. A value
        // that only vars_os() can represent losslessly can never match an
        // env-binding key anyway (config keys are ASCII), so lossy
        // conversion is safe here and keeps boot from aborting on an
        // unrelated non-UTF-8 variable elsewhere in the environment.
        let env = self.env.unwrap_or_else(|| {
            std::env::vars_os()
                .map(|(k, v)| (k.to_string_lossy().into_owned(), v.to_string_lossy().into_owned()))
                .collect()
        });
        let config = AppConfig::load(&self.base_dir, &env)?;
        crate::logging::run_highest_priority(&config)?;
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

        let mut started: Vec<&'static ComponentDescriptor> = Vec::with_capacity(order.len());
        for d in &order {
            if let Some(hook) = d.post_construct
                && let Err(e) = hook(&context).await
            {
                let err = BootError::PostConstruct { component: d.type_name, message: e.to_string() };
                let rollback: Vec<&'static ComponentDescriptor> = started.into_iter().rev().collect();
                if let Some(rollback_err) = run_pre_destroys(&context, rollback).await {
                    tracing::error!(error = %rollback_err, "rollback pre_destroy hook failed after boot failure");
                }
                return Err(err);
            }
            started.push(d);
        }
        tracing::info!(components = order.len(), "Oxide Boot started");
        Ok(Application { context, order })
    }

    pub async fn run(self) -> Result<(), BootError> {
        self.start().await?.wait_for_shutdown().await
    }
}

#[derive(Debug)]
pub struct Application {
    context: Arc<Context>,
    order: Vec<&'static ComponentDescriptor>,
}

impl Application {
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// Runs `pre_destroy` hooks ordered by `(destroy_order asc, then reverse
    /// construction order)`.
    ///
    /// Reverse construction order is first collected via
    /// `self.order.iter().rev()`, then stably re-sorted by
    /// `destroy_order` — the stable sort preserves the reverse-construction
    /// relative order *within* each `destroy_order` value, so order-0
    /// components (the vast majority) behave exactly as before, and higher
    /// slots (e.g. `100` for telemetry flush) simply run after them.
    ///
    /// Every hook runs regardless of earlier failures — a failing hook (e.g.
    /// a server drain that times out) must not skip the hooks after it, such
    /// as an OTEL flush that still needs to run. Each failure is logged as it
    /// happens; the first one is returned to the caller once all hooks have
    /// run.
    pub async fn stop(self) -> Result<(), BootError> {
        let hooks: Vec<&'static ComponentDescriptor> = self.order.iter().rev().copied().collect();
        if let Some(err) = run_pre_destroys(&self.context, hooks).await {
            return Err(err);
        }
        tracing::info!("Oxide Boot stopped");
        Ok(())
    }

    /// Blocks until ctrl-c (and on Unix SIGTERM), then shuts down.
    pub async fn wait_for_shutdown(self) -> Result<(), BootError> {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|e| BootError::Signal { message: e.to_string() })?;
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

/// Runs `pre_destroy` hooks for `descriptors`, ordered by `(destroy_order
/// asc, then the input's own relative order)`.
///
/// `descriptors` must already be in reverse-construction order (as produced
/// by e.g. `self.order.iter().rev()`); this function stably re-sorts that by
/// `destroy_order`, so order-0 components (the vast majority) keep their
/// reverse-construction order and higher slots (e.g. `100` for telemetry
/// flush) run after them — see [`Application::stop`]'s doc comment for the
/// full reasoning.
///
/// Every hook runs regardless of earlier failures. Each failure is logged
/// via `tracing::error!` as it happens; the first one is returned to the
/// caller once all hooks have run, `None` if every hook succeeded.
///
/// Shared by both [`Application::stop`] and [`AppBuilder::start`]'s
/// boot-failure rollback path, so the two stay in lockstep instead of
/// duplicating the sort-and-collect logic.
async fn run_pre_destroys(context: &Context, descriptors: Vec<&'static ComponentDescriptor>) -> Option<BootError> {
    let mut hooks = descriptors;
    hooks.sort_by_key(|d| d.destroy_order);
    let mut first_err: Option<BootError> = None;
    for d in &hooks {
        if let Some(hook) = d.pre_destroy
            && let Err(e) = hook(context).await
        {
            let err = BootError::PreDestroy { component: d.type_name, message: e.to_string() };
            tracing::error!(component = d.type_name, error = %err, "pre_destroy hook failed");
            if first_err.is_none() {
                first_err = Some(err);
            }
        }
    }
    first_err
}

/// Built-in fallback [`crate::LoggingInitializer`], registered at priority 0
/// by oxide-boot itself. Sets up a plain `tracing_subscriber::fmt` subscriber
/// with an `EnvFilter` built from `oxide.logging.level.*` config keys.
pub(crate) fn fallback_fmt_init(config: &AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // try_init: keep the first subscriber if tests boot multiple apps.
    let _ = tracing_subscriber::fmt().with_env_filter(crate::logging::logging_directives(config)).try_init();
    Ok(())
}
