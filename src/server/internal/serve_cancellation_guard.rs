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
    /// Server context whose admission gate and cancellation token are closed
    /// if the serving future is dropped while armed.
    context: ServerContext,
    /// Whether dropping this guard should begin server shutdown.
    armed: bool,
}

impl ServeCancellationGuard {
    /// Creates an armed guard for the serving future.
    /// # Parameters
    ///
    /// * `context` - Server context to shut down if the serving future is
    ///   dropped.
    pub(super) fn new(context: ServerContext) -> Self {
        Self { context, armed: true }
    }

    /// Disarms cancellation after normal serving cleanup has completed.
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ServeCancellationGuard {
    /// Begins server shutdown if normal serving cleanup did not disarm the
    /// guard.
    fn drop(&mut self) {
        if self.armed {
            let _ = self.context.begin_shutdown();
        }
    }
}
