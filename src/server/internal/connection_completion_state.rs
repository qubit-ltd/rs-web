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
    /// Internal item implementation detail.
    pub(super) active: usize,
    /// Internal item implementation detail.
    pub(super) last_completed: Option<Instant>,
}
