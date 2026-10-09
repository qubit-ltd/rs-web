// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::watch;
use tokio::time::Instant;

#[path = "session_state.rs"]
mod session_state;
#[path = "session_tracker_inner.rs"]
mod session_tracker_inner;
use session_state::SessionState;
pub(in crate::server::web_server) use session_tracker_inner::SessionTrackerInner;

use super::session_guard::SessionGuard;
use super::session_registration_error::SessionRegistrationError;

/// Atomically tracks managed sessions and the shutdown admission gate.
#[derive(Clone, Debug)]
pub(crate) struct SessionTracker {
    inner: Arc<SessionTrackerInner>,
}

impl SessionTracker {
    /// Creates a tracker with no sessions and an open admission gate.
    pub(crate) fn new() -> Self {
        let (updates, _) = watch::channel(0);
        Self {
            inner: Arc::new(SessionTrackerInner {
                state: Mutex::new(SessionState {
                    active: 0,
                    draining: false,
                    completed_during_shutdown: Vec::new(),
                }),
                updates,
            }),
        }
    }

    /// Registers a session unless shutdown has already closed admission.
    pub(crate) fn try_register(&self) -> Result<SessionGuard, SessionRegistrationError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.draining {
            return Err(SessionRegistrationError::ShuttingDown);
        }
        state.active += 1;
        self.inner.updates.send_replace(state.active);
        Ok(SessionGuard {
            inner: self.inner.clone(),
        })
    }

    /// Returns the current number of registered sessions.
    pub(crate) fn active(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
    }

    /// Returns the number of sessions that were active at an absolute instant.
    pub(crate) fn active_at(&self, instant: Instant) -> usize {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active
            + state
                .completed_during_shutdown
                .iter()
                .filter(|completed| **completed > instant)
                .count()
    }

    /// Closes the admission gate so later registrations are rejected.
    pub(crate) fn begin_shutdown(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.draining = true;
        state.completed_during_shutdown.clear();
        self.inner.updates.send_replace(state.active);
    }

    /// Waits until every session registered before shutdown has been dropped.
    pub(crate) async fn wait_for_zero(&self) {
        let mut updates = self.inner.updates.subscribe();
        loop {
            if *updates.borrow_and_update() == 0 {
                return;
            }
            if updates.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Default for SessionTracker {
    /// Creates an empty tracker with session admission enabled.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::Instant;

    use super::SessionTracker;

    #[tokio::test]
    async fn test_wait_for_zero_completes_after_last_guard_drops() {
        let tracker = SessionTracker::new();
        let guard = tracker.try_register().expect("initial registration");
        tracker.begin_shutdown();
        let waiting = tracker.wait_for_zero();
        tokio::pin!(waiting);
        tokio::select! {
            () = &mut waiting => panic!("wait must remain pending while a guard is active"),
            () = tokio::task::yield_now() => {}
        }
        drop(guard);
        waiting.await;
    }

    #[test]
    fn deadline_snapshot_counts_sessions_released_after_the_deadline() {
        let tracker = SessionTracker::new();
        let guard = tracker.try_register().expect("initial registration");
        tracker.begin_shutdown();
        let deadline = Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(1));
        drop(guard);

        assert_eq!(tracker.active(), 0);
        assert_eq!(tracker.active_at(deadline), 1);
        assert_eq!(tracker.active_at(Instant::now()), 0);
    }

    #[test]
    fn default_tracker_starts_empty_with_admission_open() {
        let tracker = SessionTracker::default();
        assert_eq!(tracker.active(), 0);
        assert!(tracker.try_register().is_ok());
    }
}
