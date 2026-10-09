// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::SseAdmissionError;
use super::SseConnection;
use super::internal::SseConnectionGuard;
use crate::ServerContext;
use crate::SessionRegistrationError;
use crate::WebServerError;

const DEFAULT_MAX_CONNECTIONS: usize = 128;
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Independent limits for server-sent event connections.
///
/// Each call to [`new`](Self::new) or [`default`](Self::default) creates an
/// independent connection-capacity domain. Cloning a policy shares its domain.
/// To apply one limit across routes or handlers, create the policy once during
/// application startup, store it in application state, and use clones obtained
/// from that state (for example, through Axum's
/// [`axum::extract::State`]). [`active_connections`](Self::active_connections)
/// counts reservations in this domain, including its clones; it does not count
/// connections admitted by other policies or all server sessions.
///
/// Event IDs, `Last-Event-ID`, replay, and application event buffering remain
/// application responsibilities. A successful write only means that the
/// transport accepted the data; it does not prove that the client consumed it.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use qubit_web::SseConnectionPolicy;
///
/// let policy = SseConnectionPolicy::new(NonZeroUsize::new(32).expect("positive limit"));
/// assert_eq!(policy.active_connections(), 0);
/// ```
#[derive(Clone, Debug)]
#[must_use]
pub struct SseConnectionPolicy {
    /// Maximum number of simultaneous SSE connections admitted by this policy.
    max_connections: NonZeroUsize,
    /// Delay between comment frames used to keep an idle connection open.
    keep_alive_interval: Duration,
    /// Shared count of active connections reserved through this policy.
    active_connections: Arc<AtomicUsize>,
}

impl Default for SseConnectionPolicy {
    fn default() -> Self {
        Self {
            max_connections: NonZeroUsize::new(DEFAULT_MAX_CONNECTIONS)
                .expect("default SSE connection count is non-zero"),
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL,
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl SseConnectionPolicy {
    /// Creates a policy with the given maximum number of concurrent streams.
    ///
    /// The returned policy starts an independent connection-capacity domain.
    /// Cloning it shares that domain. Create it once at application startup and
    /// reuse clones from application state when routes or handlers must share
    /// the same limit.
    ///
    /// # Parameters
    ///
    /// * `max_connections` - Positive simultaneous connection capacity.
    ///
    /// # Returns
    ///
    /// A policy with the default keep-alive interval and no active streams.
    pub fn new(max_connections: NonZeroUsize) -> Self {
        Self {
            max_connections,
            ..Self::default()
        }
    }

    /// Returns the number of currently active SSE responses.
    ///
    /// This counts only reservations in this policy's capacity domain,
    /// including clones of this policy.
    ///
    /// # Returns
    ///
    /// The number of connection reservations not yet released.
    #[must_use]
    #[inline]
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    /// Sets the interval between SSE keep-alive comment frames.
    ///
    /// A zero interval is rejected to prevent a busy loop.
    ///
    /// # Parameters
    ///
    /// * `interval` - Positive delay between keep-alive comment frames.
    ///
    /// # Returns
    ///
    /// The updated policy.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] if the interval is
    /// zero.
    pub fn with_keep_alive_interval(mut self, interval: Duration) -> Result<Self, WebServerError> {
        if interval.is_zero() {
            return Err(WebServerError::InvalidConfig);
        }
        self.keep_alive_interval = interval;
        Ok(self)
    }

    /// Reserves an SSE connection and returns its event-source cancellation
    /// token.
    ///
    /// Call this before starting the event producer. Pass the returned token to
    /// producer tasks so they can stop on client disconnect or server shutdown.
    /// A full connection budget is rejected immediately with HTTP 503.
    ///
    /// # Parameters
    ///
    /// * `context` - Server lifecycle state used for cancellation and session
    ///   accounting.
    ///
    /// # Returns
    ///
    /// A reserved connection with a cancellation token for the event producer.
    ///
    /// # Errors
    ///
    /// Returns [`SseAdmissionError`] when capacity is full or shutdown has
    /// begun.
    pub fn begin(&self, context: &ServerContext) -> Result<SseConnection, SseAdmissionError> {
        let mut active = self.active_connections.load(Ordering::Acquire);
        loop {
            if active >= self.max_connections.get() {
                return Err(SseAdmissionError::CapacityExceeded);
            }
            match self
                .active_connections
                .compare_exchange_weak(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(current) => active = current,
            }
        }

        let session = match context.try_register_session() {
            Ok(session) => session,
            Err(SessionRegistrationError::ShuttingDown) => {
                self.active_connections.fetch_sub(1, Ordering::AcqRel);
                return Err(SseAdmissionError::ShuttingDown);
            }
        };

        let cancellation = context.cancellation_token().child_token();
        let guard = SseConnectionGuard::new(self.active_connections.clone(), session, cancellation.clone());
        Ok(SseConnection::new(self.keep_alive_interval, guard, cancellation))
    }
}
