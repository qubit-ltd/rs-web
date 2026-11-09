// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_web::get_mapping;

struct Health;

impl Health {
    #[get_mapping("/health")]
    async fn check(&self) -> &'static str {
        "ok"
    }
}

fn main() {}
