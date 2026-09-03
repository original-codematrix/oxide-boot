use oxide_boot::component;

#[component(destroy_order = "soon")]
struct Broken {}

fn main() {}
