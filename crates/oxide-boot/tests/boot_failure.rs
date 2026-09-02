// Separate test binary (see boot.rs for the rationale): `inventory` is
// process-global per binary, so a component whose `post_construct` always
// fails would break every other test in `tests/boot.rs` if it lived there.
use oxide_boot::component;
use oxide_boot::prelude::*;
use std::collections::HashMap;

#[component(post_construct = explode)]
struct FailingComponent {}

impl FailingComponent {
    async fn explode(&self) -> ComponentResult<()> {
        Err("intentional startup failure".into())
    }
}

#[tokio::test]
async fn failing_post_construct_surfaces_as_boot_error_with_component_name() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("application.yml"), "oxide: {}\n").unwrap();
    let err = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap_err();
    match err {
        BootError::PostConstruct { component, message } => {
            assert_eq!(component, "FailingComponent");
            assert!(message.contains("intentional startup failure"), "was: {message}");
        }
        other => panic!("expected PostConstruct error, got: {other}"),
    }
}
