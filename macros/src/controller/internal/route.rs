// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Normalized route data used to generate controller registrations.
pub(in crate::controller) struct Route {
    /// Full path after joining the controller prefix and child path.
    pub(in crate::controller) path: String,
    /// Uppercase HTTP methods handled by this route.
    pub(in crate::controller) methods: Vec<String>,
    /// Transport strategy name, validated as `short`, `sse`, or `ws`.
    pub(in crate::controller) kind: String,
}
