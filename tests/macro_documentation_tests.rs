// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_web::ControllerRoutes;
use qubit_web::rest_controller;

struct Health;

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

#[tokio::test]
async fn distinct_type_paths_can_share_terminal_name() {
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
