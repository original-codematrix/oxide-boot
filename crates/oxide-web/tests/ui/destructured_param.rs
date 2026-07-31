use oxide_boot::component;
use oxide_web::controller;

#[component]
struct C {}

#[controller("/x")]
impl C {
    #[get("/{id}")]
    async fn broken(&self, axum::extract::Path(id): axum::extract::Path<String>) -> String {
        id
    }
}

fn main() {}
