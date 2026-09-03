use oxide_boot::component;

#[component(post_construct = start(bad))]
struct Broken {}

fn main() {}
