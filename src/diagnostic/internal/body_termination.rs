// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Immutable log snapshot claimed when an observed response body terminates.

/// Allowlisted snapshot claimed by at most one terminal transition.
pub(crate) struct BodyTermination {
    /// First terminal outcome recorded for the body.
    pub(super) outcome: &'static str,
    /// HTTP status published for the response.
    pub(super) status: u16,
    /// Elapsed milliseconds when this snapshot was claimed.
    pub(super) elapsed_ms: u64,
}
