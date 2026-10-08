// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Implementation details for limited request and response bodies.

mod bodies;

pub(super) use bodies::limit_request_body;
pub(super) use bodies::permit_body;
