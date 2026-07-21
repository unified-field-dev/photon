//! Matrix smoke — bootstrap + scenario wiring (full Photon runtime in Phase 5).

use photon_testkit::{BootstrapSession, MatrixSpec, ScenarioSpec, StubIdentityFactory};

#[test]
fn matrix_ci_mem_embedded_bootstrap_installs() {
    let mut session = BootstrapSession::new(MatrixSpec::ci_mem_embedded());
    session.install().expect("mem bootstrap");
    assert!(session.is_ready());
    let _ = StubIdentityFactory;
}

#[test]
fn scenario_publish_subscribe_smoke_spec_roundtrips_json() {
    let spec = ScenarioSpec::publish_subscribe_smoke();
    let json = serde_json::to_string(&spec).expect("serialize");
    let back: ScenarioSpec = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(spec, back);
}
