// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use tokio::time::Instant;

use super::session_tracker::SessionTrackerInner;

/// RAII registration for one managed SSE or WebSocket session.
#[derive(Debug)]
#[must_use]
pub struct SessionGuard {
    pub(super) inner: Arc<SessionTrackerInner>,
}

impl Drop for SessionGuard {
    /// Decrements the active count and wakes session waiters.
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.draining {
            state.completed_during_shutdown.push(Instant::now());
        }
        state.active -= 1;
        self.inner.updates.send_replace(state.active);
    }
}
