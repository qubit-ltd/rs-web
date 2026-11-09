// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_web::rest_controller;

struct ConditionalParameter;

#[rest_controller("/conditional-parameter")]
impl ConditionalParameter {
    #[get_mapping("")]
    async fn route(&self, #[cfg_attr(all(), cfg(any()))] _: axum::extract::Extension<()>) -> &'static str {
        "ok"
    }
}

fn main() {}
