// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![allow(unused_imports)]

use qubit_web::get_mapping;
use qubit_web::rest_controller;

struct Legacy;

#[rest_controller("/users")]
impl Legacy {
    #[get_mapping("/:id")]
    async fn show(&self) -> &'static str {
        "unreachable"
    }
}

fn main() {}
