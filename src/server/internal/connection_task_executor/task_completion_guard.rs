// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;

use tokio::time::Instant;

use super::task_executor_state::TaskExecutorState;

/// Records that one registered task has released its application resources.
///
/// The tracked future drops its application future and output before this
/// guard, so `last_completed` marks resource release rather than merely the
/// last poll returning `Ready`.
pub(super) struct TaskCompletionGuard {
    /// Shared counters protected by the executor state mutex.
    pub(super) state: Arc<Mutex<TaskExecutorState>>,
}

impl Drop for TaskCompletionGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = state.active.saturating_sub(1);
        let completed = Instant::now();
        state.last_completed = Some(state.last_completed.map_or(completed, |latest| latest.max(completed)));
    }
}
