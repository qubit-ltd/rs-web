// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_web::rest_controller;

struct TooMany;

#[rest_controller("/{pre0}/{pre1}/{pre2}/{pre3}/{pre4}/{pre5}/{pre6}/{pre7}/{pre8}/{pre9}/{pre10}/{pre11}/{pre12}")]
impl TooMany {
    #[get_mapping(
        "/{child0}/{child1}/{child2}/{child3}/{child4}/{child5}/{child6}/{child7}/{child8}/{child9}/{child10}/{child11}/{child12}"
    )]
    async fn show(&self) -> &'static str {
        "too many"
    }
}

fn main() {}
