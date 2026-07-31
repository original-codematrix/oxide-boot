use oxide_boot::component;
use oxide_web::controller;

#[component]
struct C {}

#[controller("/x")]
impl C {
    #[get("/")]
    async fn broken() -> &'static str {
        "no self here"
    }
}

fn main() {}
