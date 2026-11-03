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

/// Shared response-body observation state cloned by middleware and body tasks.
pub(crate) struct BodyObservation {
    method: String,
    route: String,
    connection_id: String,
    bytes: Option<u64>,
    started: Instant,
    state: Mutex<State>,
}

impl BodyObservation {
    /// Wraps an observation in the shared handle used across middleware layers.
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

struct State {
    status: Option<u16>,
    outcome: Option<&'static str>,
    emitted: bool,
}

/// Allowlisted log snapshot claimed by one terminal transition.
pub(crate) struct BodyTermination {
    outcome: &'static str,
    status: u16,
    elapsed_ms: u64,
}

impl BodyObservation {
    /// Creates an observation with safe request fields and no response status.
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
            state: Mutex::new(State {
                status: None,
                outcome: None,
                emitted: false,
            }),
        }
    }

    /// Sets response status after headers have been produced.
    pub(crate) fn set_status(&self, status: u16) {
        let event = {
            let mut state = self.lock();
            state.status = Some(status);
            self.take_event(&mut state)
        };
        self.emit(event);
    }

    /// Records the first terminal outcome and emits it when status is ready.
    pub(crate) fn terminate(&self, outcome: &'static str) {
        self.emit(self.mark_terminal(outcome));
    }

    /// Claims the terminal event without emitting, for callers holding another
    /// lifecycle lock.
    pub(crate) fn mark_terminal(&self, outcome: &'static str) -> Option<BodyTermination> {
        let mut state = self.lock();
        if state.outcome.is_none() {
            state.outcome = Some(outcome);
        }
        self.take_event(&mut state)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn take_event(&self, state: &mut State) -> Option<BodyTermination> {
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
    pub(crate) fn emit(&self, event: Option<BodyTermination>) {
        if let Some(event) = event {
            tracing::info!(target: "qubit_web::diagnostic", event = "response_body_end", outcome = event.outcome, method = %self.method, route = %self.route, status = event.status, elapsed_ms = event.elapsed_ms, connection_id = %self.connection_id, bytes = ?self.bytes);
        }
    }
}
