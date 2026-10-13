// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::session_guard::SessionGuard;
use super::session_registration_error::SessionRegistrationError;
use super::session_tracker::SessionTracker;

/// Shared cancellation state for application-owned long-lived sessions.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() -> Result<(), qubit_web::WebServerError> {
/// use qubit_web::ServerOptions;
/// use qubit_web::WebServer;
///
/// let options = ServerOptions::new("127.0.0.1:0".parse().expect("socket address"));
/// let server = WebServer::bind_http(options).await?;
/// let context = server.context();
/// let _shutdown = context.cancellation_token();
/// let guard = context.try_register_session().expect("server accepts sessions");
/// assert_eq!(context.active_sessions(), 1);
/// drop(guard);
/// assert_eq!(context.active_sessions(), 0);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
#[must_use]
pub struct ServerContext {
    /// Token cancelled when graceful shutdown starts.
    cancellation: CancellationToken,
    /// Atomically tracks managed sessions and closes admission at shutdown.
    sessions: SessionTracker,
    /// Grace period allowed after shutdown begins.
    shutdown_timeout: Duration,
    /// Shared deadline initialized once when shutdown starts.
    shutdown_deadline: Arc<Mutex<Option<tokio::time::Instant>>>,
}

impl ServerContext {
    /// Creates empty session state with the configured shutdown grace period.
    ///
    /// # Parameters
    ///
    /// * `shutdown_timeout` - Grace period applied after cancellation begins.
    ///
    /// # Returns
    ///
    /// A context with no registered sessions and no shutdown deadline.
    pub(super) fn new(shutdown_timeout: Duration) -> Self {
        Self {
            cancellation: CancellationToken::new(),
            sessions: SessionTracker::new(),
            shutdown_timeout,
            shutdown_deadline: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns a cloneable token cancelled when server shutdown begins.
    ///
    /// # Returns
    ///
    /// A token that is cancelled when the shared shutdown process starts.
    #[must_use]
    #[inline]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Returns the server's graceful-shutdown deadline for long-lived sessions.
    ///
    /// # Returns
    ///
    /// The configured grace period used to calculate the absolute deadline.
    #[must_use]
    #[inline]
    pub const fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Returns the absolute graceful-shutdown deadline after shutdown starts.
    ///
    /// # Returns
    ///
    /// Returns `None` before shutdown begins and the shared deadline afterward.
    pub fn shutdown_deadline(&self) -> Option<tokio::time::Instant> {
        *self
            .shutdown_deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns the number of currently registered sessions.
    ///
    /// The count covers managed SSE reservations and WebSocket upgrades
    /// admitted through `WsUpgradePolicy::on_upgrade`, including upgrades
    /// whose handshake is pending. It does not count ordinary HTTP
    /// requests, native Axum upgrades, or arbitrary application background
    /// tasks.
    ///
    /// # Returns
    ///
    /// The number of registered session guards that have not yet been dropped.
    #[must_use]
    #[inline]
    pub fn active_sessions(&self) -> usize {
        self.sessions.active()
    }

    /// Returns the number of sessions that were still active at an instant.
    pub(crate) fn active_sessions_at(&self, instant: tokio::time::Instant) -> usize {
        self.sessions.active_at(instant)
    }

    /// Registers an active long-lived session unless shutdown has started.
    ///
    /// # Errors
    ///
    /// Returns [`SessionRegistrationError::ShuttingDown`] after the server
    /// closes session admission.
    pub fn try_register_session(&self) -> Result<SessionGuard, SessionRegistrationError> {
        self.sessions.try_register()
    }

    /// Returns the shared deadline storage used by WebSocket lifecycle
    /// tracking.
    ///
    /// # Returns
    ///
    /// A shared handle updated when the server begins shutdown.
    #[cfg(feature = "ws")]
    pub(crate) fn shutdown_deadline_handle(&self) -> Arc<Mutex<Option<tokio::time::Instant>>> {
        self.shutdown_deadline.clone()
    }

    /// Starts cancellation once and returns the deadline shared with all
    /// sessions.
    ///
    /// # Returns
    ///
    /// The first shutdown deadline, or the existing deadline if shutdown began
    /// earlier.
    ///
    /// # Errors
    ///
    /// Returns [`crate::WebServerError::InvalidConfig`] when the first
    /// deadline cannot be represented. Session admission is still closed and
    /// cancellation is still triggered in that case.
    pub(super) fn begin_shutdown(&self) -> Result<tokio::time::Instant, crate::WebServerError> {
        let mut state = self
            .shutdown_deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(deadline) = *state {
            drop(state);
            self.sessions.begin_shutdown();
            self.cancellation.cancel();
            return Ok(deadline);
        }
        let now = tokio::time::Instant::now();
        let deadline = now.checked_add(self.shutdown_timeout);
        *state = Some(deadline.unwrap_or(now));
        drop(state);
        self.sessions.begin_shutdown();
        self.cancellation.cancel();
        deadline.ok_or(crate::WebServerError::InvalidConfig)
    }

    /// Waits until all currently registered managed sessions have finished.
    pub(super) async fn wait_for_sessions(&self) {
        self.sessions.wait_for_zero().await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ServerContext;
    use crate::SessionRegistrationError;
    use crate::WebServerError;

    #[test]
    fn begin_shutdown_overflow() {
        let context = ServerContext::new(Duration::MAX);

        assert!(matches!(context.begin_shutdown(), Err(WebServerError::InvalidConfig)));
        assert!(context.cancellation_token().is_cancelled());
        assert_eq!(
            context.try_register_session().unwrap_err(),
            SessionRegistrationError::ShuttingDown
        );
        let deadline = context.shutdown_deadline().expect("overflow stores immediate deadline");
        assert!(deadline <= tokio::time::Instant::now());
    }

    #[test]
    fn repeated_begin_shutdown_reuses_the_first_deadline() {
        let context = ServerContext::new(Duration::from_secs(30));

        let first = context.begin_shutdown().expect("first shutdown deadline");
        let second = context
            .begin_shutdown()
            .expect("repeated shutdown returns stored deadline");

        assert_eq!(second, first);
    }
}
