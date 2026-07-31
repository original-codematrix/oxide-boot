use oxide_web::controller;

#[controller("/x")]
async fn not_an_impl() {}

fn main() {}
