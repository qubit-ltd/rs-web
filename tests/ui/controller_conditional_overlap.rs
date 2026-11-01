// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_web::rest_controller;

struct ConditionalOverlap;

#[rest_controller("/conditional")]
impl ConditionalOverlap {
    #[cfg(all())]
    #[get_mapping("/same")]
    async fn first(&self) -> &'static str {
        "first"
    }

    #[cfg(all())]
    #[get_mapping("/same")]
    async fn second(&self) -> &'static str {
        "second"
    }
}

fn main() {}

#[derive(Clone)]
struct FirstState;

#[derive(Clone)]
struct SecondState;

struct ConditionalStateOverlap;

#[rest_controller("/conditional-state")]
impl ConditionalStateOverlap {
    #[cfg(all())]
    #[get_mapping("/first")]
    async fn first(&self, _: axum::extract::State<FirstState>) -> &'static str {
        "first"
    }

    #[cfg(all())]
    #[get_mapping("/second")]
    async fn second(&self, _: axum::extract::State<SecondState>) -> &'static str {
        "second"
    }
}
