//! Application lifecycle: config loading, logging init, DI assembly, and
//! ordered `post_construct` / `pre_destroy` hook execution.

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
    #[error("logging initialization failed: {message}")]
    Logging { message: String },
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

#[derive(Debug)]
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

/// Built-in fallback [`crate::LoggingInitializer`], registered at priority 0
/// by oxide-boot itself. Sets up a plain `tracing_subscriber::fmt` subscriber
/// with an `EnvFilter` built from `oxide.logging.level.*` config keys.
pub(crate) fn fallback_fmt_init(config: &AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // try_init: keep the first subscriber if tests boot multiple apps.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(crate::logging::logging_directives(config))
        .try_init();
    Ok(())
}
