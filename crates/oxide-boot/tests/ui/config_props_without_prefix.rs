use oxide_boot::ConfigProperties;

#[derive(ConfigProperties, serde::Deserialize)]
struct NoPrefix {
    value: String,
}

fn main() {}
