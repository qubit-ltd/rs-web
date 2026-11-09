// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_web::route;

#[route("/health", method = "GET")]
async fn check() -> &'static str {
    "ok"
}

fn main() {}
