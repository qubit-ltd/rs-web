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
    #[route("", method = "HEAD")]
    async fn first(&self) -> &'static str {
        "first"
    }

    #[route("", method = "HEAD")]
    async fn second(&self) -> &'static str {
        "second"
    }
}

fn main() {}
