// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use qubit_web::ControllerRoutes;
use qubit_web::mvc::ControllerDefinition;
use qubit_web::rest_controller;

struct Health;

struct Conditional;

#[allow(clippy::non_minimal_cfg)] // `all()` is intentional in the cfg propagation contract.
#[rest_controller("/conditional")]
impl Conditional {
    #[cfg(any())]
    #[get_mapping("/disabled")]
    async fn disabled(&self, _: DisabledExtractor) -> &'static str {
        "disabled"
    }

    #[cfg_attr(all(), cfg(any()))]
    #[get_mapping("/cfg-attr")]
    async fn cfg_attr_disabled(&self, _: MissingByCfgAttr) -> &'static str {
        "disabled"
    }

    #[cfg(all())]
    #[get_mapping("/enabled")]
    async fn enabled(&self) -> &'static str {
        "enabled"
    }

    #[cfg(any())]
    #[get_mapping("/shared")]
    async fn shared_disabled(&self) -> &'static str {
        "disabled"
    }

    #[cfg(all())]
    #[get_mapping("/shared")]
    async fn shared_enabled(&self) -> &'static str {
        "shared"
    }
}

// This type intentionally has no definition: cfg(any()) must remove every
// generated reference to the disabled method's signature.

#[derive(Clone)]
struct ConditionalState;

struct ConditionalStateController;

#[allow(clippy::non_minimal_cfg)] // `all()` is intentional in the cfg state inference contract.
#[rest_controller("/conditional-state")]
impl ConditionalStateController {
    #[cfg(any())]
    #[get_mapping("/disabled")]
    async fn disabled(&self, _: axum::extract::State<MissingState>) -> &'static str {
        "disabled"
    }

    #[cfg(all())]
    #[get_mapping("/enabled")]
    async fn enabled(&self, _: axum::extract::State<ConditionalState>) -> &'static str {
        "enabled"
    }
}

mod left {
    pub struct Duplicate;
}

mod right {
    pub struct Duplicate;
}

#[rest_controller("/health")]
impl Health {
    #[get_mapping("")]
    async fn check(&self) -> &'static str {
        "ok"
    }
}

#[rest_controller("/left")]
impl left::Duplicate {
    #[get_mapping("")]
    async fn value(&self) -> &'static str {
        "left"
    }
}

#[rest_controller("/right")]
impl right::Duplicate {
    #[get_mapping("")]
    async fn value(&self) -> &'static str {
        "right"
    }
}

#[test]
fn test_macro_crate_documentation_example_compiles() {
    let router = ControllerRoutes::<()>::new()
        .add(Arc::new(Health))
        .expect("add Health controller")
        .finish();
    let _ = router;
}

#[test]
fn test_impl_cfg_macro_consumer_compiles_with_feature_disabled_and_enabled() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = repository.join("tests/fixtures/macro_consumers/impl_cfg/Cargo.toml");
    // Keep nested Cargo builds under the parent test's isolated target directory.
    let target_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("macro-impl-cfg");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());

    for feature in [
        None,
        Some("impl-enabled"),
        Some("param-enabled"),
        Some("impl-enabled,param-enabled"),
    ] {
        let mut command = Command::new(&cargo);
        command
            .arg("test")
            .arg("--locked")
            .arg("--manifest-path")
            .arg(&manifest)
            .arg("--target-dir")
            .arg(&target_dir)
            .env("CARGO_TARGET_DIR", &target_dir);
        if let Some(feature) = feature {
            command.arg("--features").arg(feature);
        }
        let output = command.output().expect("run the impl cfg consumer fixture");
        assert!(
            output.status.success(),
            "impl cfg consumer failed with feature {feature:?}:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn test_conditional_routes_match_metadata_and_registration() {
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;
    use tower::ServiceExt;

    let metadata = <Conditional as ControllerDefinition<()>>::route_metadata();
    assert_eq!(metadata.len(), 2);
    assert!(metadata.iter().any(|route| route.path() == "/conditional/enabled"));
    assert!(metadata.iter().any(|route| route.path() == "/conditional/shared"));
    assert!(!metadata.iter().any(|route| route.path() == "/conditional/disabled"));
    assert!(!metadata.iter().any(|route| route.path() == "/conditional/cfg-attr"));

    let router = ControllerRoutes::<()>::new()
        .add(Arc::new(Conditional))
        .expect("add conditional controller")
        .finish();
    let enabled = router
        .clone()
        .oneshot(Request::get("/conditional/enabled").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let disabled = router
        .clone()
        .oneshot(Request::get("/conditional/disabled").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let shared = router
        .clone()
        .oneshot(Request::get("/conditional/shared").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let cfg_attr_disabled = router
        .oneshot(Request::get("/conditional/cfg-attr").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(enabled.status(), StatusCode::OK);
    assert_eq!(disabled.status(), StatusCode::NOT_FOUND);
    assert_eq!(shared.status(), StatusCode::OK);
    assert_eq!(cfg_attr_disabled.status(), StatusCode::NOT_FOUND);
}

#[test]
fn test_conditional_state_type_is_inferred_from_enabled_methods() {
    let metadata = <ConditionalStateController as ControllerDefinition<ConditionalState>>::route_metadata();
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].path(), "/conditional-state/enabled");
}

#[tokio::test]
async fn test_distinct_type_paths_can_share_terminal_name() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let router = ControllerRoutes::<()>::new()
        .add(Arc::new(left::Duplicate))
        .expect("add left Duplicate controller")
        .add(Arc::new(right::Duplicate))
        .expect("add right Duplicate controller")
        .finish();
    let left = router
        .clone()
        .oneshot(Request::get("/left").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let right = router
        .oneshot(Request::get("/right").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(left.status(), axum::http::StatusCode::OK);
    assert_eq!(right.status(), axum::http::StatusCode::OK);
    assert_eq!(&axum::body::to_bytes(left.into_body(), 16).await.unwrap()[..], b"left");
    assert_eq!(
        &axum::body::to_bytes(right.into_body(), 16).await.unwrap()[..],
        b"right"
    );
}
