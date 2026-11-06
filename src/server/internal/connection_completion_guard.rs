// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::connection_completion::ConnectionCompletion;

/// Records transport completion when an accepted connection task exits.
pub(super) struct ConnectionCompletionGuard {
    /// Tracker whose active count and latest completion time this guard
    /// updates.
    completion: ConnectionCompletion,
}

impl ConnectionCompletionGuard {
    /// Constructs the guard or state object from its owned resources.
    /// # Parameters
    ///
    /// * `completion` - Shared completion tracker updated when this guard
    ///   drops.
    pub(super) fn new(completion: ConnectionCompletion) -> Self {
        Self { completion }
    }
}

impl Drop for ConnectionCompletionGuard {
    /// Records the task exit time and decrements the active transport count.
    fn drop(&mut self) {
        self.completion.record_completion(tokio::time::Instant::now());
    }
}
