// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tracks accepted transport task completion times for shutdown reports.

use std::sync::Arc;
use std::sync::Mutex;

use tokio::time::Instant;

use super::connection_completion_guard::ConnectionCompletionGuard;
use super::connection_completion_state::ConnectionCompletionState;

/// Shared O(1) record of active transports and their latest completion time.
#[derive(Clone, Debug, Default)]
pub(super) struct ConnectionCompletion {
    /// Internal item implementation detail.
    state: Arc<Mutex<ConnectionCompletionState>>,
}

impl ConnectionCompletion {
    /// Creates a tracker with no active transports.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Registers an accepted transport before its task is submitted.
    pub(super) fn register(&self) -> ConnectionCompletionGuard {
        let mut state = self.lock_state();
        state.active += 1;
        drop(state);
        ConnectionCompletionGuard::new(self.clone())
    }

    /// Reports whether all registered transports completed by `deadline`.
    /// # Parameters
    ///
    /// * `deadline` - Optional absolute instant that bounds this operation.
    pub(super) fn completed_by(&self, deadline: Instant) -> bool {
        let state = self.lock_state();
        state.active == 0 && state.last_completed.is_none_or(|completed| completed <= deadline)
    }
    /// Records a task completion and updates active counts.
    ///
    /// # Parameters
    ///
    /// * `completed` - Completion instant used to update active and completed
    pub(super) fn record_completion(&self, completed: Instant) {
        let mut state = self.lock_state();
        state.active = state.active.saturating_sub(1);
        state.last_completed = Some(state.last_completed.map_or(completed, |latest| latest.max(completed)));
    }
    /// Locks the connection completion state.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, ConnectionCompletionState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::Instant;

    use super::ConnectionCompletion;
    use super::ConnectionCompletionState;

    fn tracker(active: usize, last_completed: Option<Instant>) -> ConnectionCompletion {
        ConnectionCompletion {
            state: std::sync::Arc::new(std::sync::Mutex::new(ConnectionCompletionState {
                active,
                last_completed,
            })),
        }
    }

    #[test]
    fn test_completed_by_checks_empty_active_early_equal_and_late_states() {
        let deadline = Instant::now();
        assert!(tracker(0, None).completed_by(deadline));
        assert!(!tracker(1, None).completed_by(deadline));
        assert!(tracker(0, Some(deadline - std::time::Duration::from_millis(1))).completed_by(deadline));
        assert!(tracker(0, Some(deadline)).completed_by(deadline));
        assert!(!tracker(0, Some(deadline + std::time::Duration::from_millis(1))).completed_by(deadline));
    }

    #[test]
    fn test_reverse_completion_order_keeps_latest_completion_time() {
        let tracker = tracker(2, None);
        let early = Instant::now();
        let late = early + std::time::Duration::from_millis(1);
        tracker.record_completion(late);
        tracker.record_completion(early);

        assert!(tracker.completed_by(late));
        assert!(!tracker.completed_by(early));
    }
}
