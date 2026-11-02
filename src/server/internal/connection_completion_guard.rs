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
    completion: ConnectionCompletion,
}

impl ConnectionCompletionGuard {
    pub(super) fn new(completion: ConnectionCompletion) -> Self {
        Self { completion }
    }
}

impl Drop for ConnectionCompletionGuard {
    fn drop(&mut self) {
        self.completion.record_completion(tokio::time::Instant::now());
    }
}
