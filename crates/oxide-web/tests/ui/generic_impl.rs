use oxide_boot::component;
use oxide_web::controller;

#[component]
struct C {}

#[controller("/x")]
impl<T> C {
    #[get("")]
    async fn f(&self) -> String {
        "test".to_string()
    }
}

fn main() {}
