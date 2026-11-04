// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable state coordinating response status and body termination.

/// State shared by the response-header and response-body completion paths.
pub(super) struct BodyObservationState {
    /// HTTP status, published when response headers are produced.
    pub(super) status: Option<u16>,
    /// First terminal body outcome, retained until status is available.
    pub(super) outcome: Option<&'static str>,
    /// Whether the terminal log snapshot has already been claimed.
    pub(super) emitted: bool,
}
