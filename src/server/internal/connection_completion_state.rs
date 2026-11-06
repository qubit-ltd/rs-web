// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::time::Instant;

/// State shared by the transport completion tracker and its guards.
#[derive(Debug, Default)]
pub(super) struct ConnectionCompletionState {
    /// Number of registered transport tasks whose completion guards have not
    /// yet dropped.
    pub(super) active: usize,
    /// Latest recorded task completion time for shutdown deadline checks.
    pub(super) last_completed: Option<Instant>,
}
