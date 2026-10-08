// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_web::rest_controller;

struct Controller;

#[rest_controller("/items")]
impl Controller {
    #[get_mapping("")]
    async fn list(&self) -> &'static str { "items" }

    #[get_mapping("")]
    async fn index(&self) -> &'static str { "index" }
}

fn main() {}
