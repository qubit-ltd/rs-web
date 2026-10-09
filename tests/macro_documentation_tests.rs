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
}

#[test]
fn test_macro_crate_documentation_example_compiles() {
    let router = ControllerRoutes::<()>::new()
        .add(Arc::new(Health))
        .expect("add Health controller")
        .finish()
        .expect("finish ControllerRoutes");
    let _ = router;
}
