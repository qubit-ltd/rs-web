// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_web::get;
use qubit_web::rest_controller;

struct Controller;

#[rest_controller("/items/{id}")]
impl Controller {
    #[get("/{id}")]
    async fn get(&self) -> &'static str {
        "ok"
    }
}

fn main() {}
