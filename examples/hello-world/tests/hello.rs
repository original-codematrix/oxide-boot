use hello_world::Greeter;
use oxide_boot::prelude::*;
use std::collections::HashMap;

#[tokio::test]
async fn boots_and_greets_from_yaml() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let app = OxideApplication::builder().base_dir(dir).env(HashMap::new()).start().await.unwrap();
    let greeter = app.context().get::<Greeter>().unwrap();
    assert_eq!(greeter.greeting(), "Hello, World!");
    app.stop().await.unwrap();
}
