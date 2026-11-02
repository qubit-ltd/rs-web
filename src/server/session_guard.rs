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
///
/// The guard should live for the same duration as the managed task. Dropping
/// it removes the session from server tracking; it does not cancel the task or
/// mean that the task has completed its work.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() -> Result<(), qubit_web::WebServerError> {
/// use qubit_web::{ServerOptions, WebServer};
///
/// let options = ServerOptions::new("127.0.0.1:0".parse().expect("socket address"));
/// let server = WebServer::bind_http(options).await?;
/// let context = server.context();
/// let cancellation = context.cancellation_token();
/// assert_eq!(context.active_sessions(), 0);
/// {
///     let _guard = context.try_register_session().expect("server accepts sessions");
///     assert_eq!(context.active_sessions(), 1);
/// }
/// assert_eq!(context.active_sessions(), 0);
/// assert!(!cancellation.is_cancelled());
/// # Ok(())
/// # }
/// ```
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
