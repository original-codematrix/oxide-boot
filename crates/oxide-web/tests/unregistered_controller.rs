// Separate test binary (see tests/server.rs for the rationale): `inventory` is
// process-global per binary, so a controller whose route registration fails
// would break every other test if it lived there.
use oxide_boot::prelude::*;
use oxide_web::controller;
use std::collections::HashMap;

// Deliberately NOT #[component]: boot must fail with ControllerNotRegistered.
struct GhostController;

#[controller("/ghost")]
impl GhostController {
    #[get("")]
    async fn boo(&self) -> String {
        "boo".to_string()
    }
}

#[tokio::test]
async fn unregistered_controller_fails_boot_with_typed_error() {
    let dir = tempfile::tempdir().unwrap();
    // management.port: 0 too — ManagementServer's post_construct runs before
    // WebServer's (alphabetical tie-break) and would otherwise bind the real
    // 8081 default and leak it: this test's boot fails before `stop()` ever
    // runs to tear it back down.
    std::fs::write(dir.path().join("application.yml"), "oxide:\n  server:\n    port: 0\n  management:\n    port: 0\n")
        .unwrap();
    let err = OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("GhostController") && msg.contains("not registered"), "was: {msg}");
}
