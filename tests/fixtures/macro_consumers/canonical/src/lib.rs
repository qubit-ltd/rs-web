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

#[rest_controller("/health")]
impl Health {
    #[get_mapping("")]
    async fn check(&self) -> &'static str {
        "ok"
    }

    #[get_mapping("/opaque")]
    async fn opaque(&self) -> impl axum::response::IntoResponse {
        "ok"
    }

    #[get_mapping("/self")]
    async fn self_extractor(&self, axum::extract::Extension(value): axum::extract::Extension<Arc<Self>>) -> String {
        let _ = value;
        "ok".to_owned()
    }

    #[route("/events", method = "GET", kind = "sse")]
    async fn events(&self) -> &'static str {
        "events"
    }

    #[route("/socket", method = "GET", kind = "ws")]
    async fn socket(&self) -> &'static str {
        "socket"
    }
}

pub fn assemble() {
    let _router = ControllerRoutes::<()>::new()
        .add(Arc::new(Health))
        .expect("valid controller")
        .finish();
}
