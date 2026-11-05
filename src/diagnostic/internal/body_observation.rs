// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared, allowlisted response-body termination observation.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use axum::http::Method;

use super::body_observation_state::BodyObservationState;
use super::body_termination::BodyTermination;

/// Shared response-body observation state cloned by middleware and body tasks.
pub(crate) struct BodyObservation {
    /// Internal item implementation detail.
    method: String,
    /// Internal item implementation detail.
    route: String,
    /// Internal item implementation detail.
    connection_id: String,
    /// Internal item implementation detail.
    bytes: Option<u64>,
    /// Internal item implementation detail.
    started: Instant,
    /// Internal item implementation detail.
    state: Mutex<BodyObservationState>,
}

impl BodyObservation {
    /// Wraps an observation in the shared handle used across middleware layers.
    /// # Parameters
    ///
    /// * `method` - HTTP method used for the request.
    /// * `route` - Route label used for diagnostics.
    /// * `connection_id` - Identifier attached to this connection.
    /// * `bytes` - Number of message or body bytes charged to the resource
    ///   budget.
    /// * `started` - Time at which response-body observation began.
    pub(crate) fn shared(
        method: Method,
        route: String,
        connection_id: String,
        bytes: Option<u64>,
        started: Instant,
    ) -> Arc<Self> {
        Arc::new(Self::new(method, route, connection_id, bytes, started))
    }
}

impl BodyObservation {
    /// Creates an observation with safe request fields and no response status.
    /// # Parameters
    ///
    /// * `method` - HTTP method used for the request.
    /// * `route` - Route label used for diagnostics.
    /// * `connection_id` - Identifier attached to this connection.
    /// * `bytes` - Number of message or body bytes charged to the resource
    ///   budget.
    /// * `started` - Time at which response-body observation began.
    pub(crate) fn new(
        method: Method,
        route: String,
        connection_id: String,
        bytes: Option<u64>,
        started: Instant,
    ) -> Self {
        Self {
            method: method.to_string(),
            route,
            connection_id,
            bytes,
            started,
            state: Mutex::new(BodyObservationState {
                status: None,
                outcome: None,
                emitted: false,
            }),
        }
    }

    /// Publishes the response status after headers have been produced.
    ///
    /// The status may arrive before or after body termination; once both are
    /// available, one caller claims the log snapshot while holding the state
    /// lock and emits it after releasing the lock.
    /// # Parameters
    ///
    /// * `status` - HTTP response status associated with the observation.
    pub(crate) fn set_status(&self, status: u16) {
        let event = {
            let mut state = self.lock();
            state.status = Some(status);
            self.take_event(&mut state)
        };
        self.emit(event);
    }

    /// Records the first terminal outcome and emits it when status is ready.
    ///
    /// A later terminal caller cannot replace the first outcome. Snapshot
    /// claiming occurs under the state lock, while tracing output occurs after
    /// the lock has been released.
    /// # Parameters
    ///
    /// * `outcome` - Terminal result recorded for the response body.
    pub(crate) fn terminate(&self, outcome: &'static str) {
        self.emit(self.mark_terminal(outcome));
    }

    /// Claims the terminal event without emitting, for callers holding another
    /// lifecycle lock.
    ///
    /// The first terminal outcome wins. The snapshot is claimed while the state
    /// lock is held and can be emitted later, after the caller releases any
    /// other lifecycle lock it owns.
    /// # Parameters
    ///
    /// * `outcome` - First terminal outcome to record.
    pub(crate) fn mark_terminal(&self, outcome: &'static str) -> Option<BodyTermination> {
        let mut state = self.lock();
        if state.outcome.is_none() {
            state.outcome = Some(outcome);
        }
        self.take_event(&mut state)
    }
    /// Locks the shared observation state, recovering a poisoned mutex.
    fn lock(&self) -> std::sync::MutexGuard<'_, BodyObservationState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    /// Returns a completed body termination when its required fields exist.
    ///
    /// # Parameters
    ///
    /// * `state` - Mutable body observation state from which the completed
    ///   termination is read.
    fn take_event(&self, state: &mut BodyObservationState) -> Option<BodyTermination> {
        let (Some(outcome), Some(status)) = (state.outcome, state.status) else {
            return None;
        };
        if state.emitted {
            return None;
        }
        state.emitted = true;
        Some(BodyTermination {
            outcome,
            status,
            elapsed_ms: self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        })
    }

    /// Emits one previously claimed terminal event outside lifecycle locks.
    ///
    /// Callers must invoke this after releasing lifecycle locks; the snapshot
    /// has already been claimed and marked emitted while holding the state
    /// lock, so concurrent transitions cannot produce a second event.
    /// # Parameters
    ///
    /// * `event` - Claimed body termination to emit, if available.
    pub(crate) fn emit(&self, event: Option<BodyTermination>) {
        if let Some(event) = event {
            tracing::info!(target: "qubit_web::diagnostic", event = "response_body_end", outcome = event.outcome, method = %self.method, route = %self.route, status = event.status, elapsed_ms = event.elapsed_ms, connection_id = %self.connection_id, bytes = ?self.bytes);
        }
    }
}
