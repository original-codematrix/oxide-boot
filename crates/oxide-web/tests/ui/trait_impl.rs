use oxide_web::controller;

struct C {}

#[controller("/x")]
impl Clone for C {
    fn clone(&self) -> Self {
        C {}
    }
}

fn main() {}
