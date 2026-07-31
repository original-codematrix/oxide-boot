//! Reference example: a minimal Oxide Boot application with a single
//! component and config-properties binding. Types live here (rather than
//! directly in `main.rs`) so the integration test in `tests/hello.rs` can
//! see them.

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
