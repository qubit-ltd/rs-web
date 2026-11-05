// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::server::ServerContext;

/// Starts server cancellation if a running `serve` future is dropped.
pub(super) struct ServeCancellationGuard {
    /// Internal item implementation detail.
    context: ServerContext,
    /// Internal item implementation detail.
    armed: bool,
}

impl ServeCancellationGuard {
    /// Creates an armed guard for the serving future.
    /// # Parameters
    ///
    /// * `context` - Task context used to register a waker or carry request
    pub(super) fn new(context: ServerContext) -> Self {
        Self { context, armed: true }
    }

    /// Disarms cancellation after normal serving cleanup has completed.
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ServeCancellationGuard {
    /// Releases the resources owned by this guard.
    fn drop(&mut self) {
        if self.armed {
            let _ = self.context.begin_shutdown();
        }
    }
}
