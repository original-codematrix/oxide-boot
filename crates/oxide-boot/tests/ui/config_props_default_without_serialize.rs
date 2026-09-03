use oxide_boot::ConfigProperties;

#[derive(ConfigProperties, serde::Deserialize, Default)]
#[config(prefix = "no-serialize", default)]
struct NoSerialize {
    #[serde(default)]
    value: String,
}

fn main() {}
