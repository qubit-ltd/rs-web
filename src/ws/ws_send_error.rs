// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Error returned when an outbound message cannot be queued.
///
/// A message accepted before shutdown may still be discarded if it remains
/// pending when the session closes.
///
/// # Examples
///
/// ```
/// use qubit_web::ws::WsSendError;
///
/// fn enqueue() -> Result<(), Box<dyn std::error::Error>> {
///     let queue = qubit_web::ws::WsSendQueue::new(0, 1);
///     queue.try_send(axum::extract::ws::Message::text("private-payload"))?;
///     Ok(())
/// }
///
/// let error = enqueue().unwrap_err();
/// assert!(error.downcast_ref::<WsSendError>().is_some());
/// assert_eq!(error.to_string(), "WebSocket send queue capacity exceeded");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum WsSendError {
    /// The queue reached either its item or byte limit.
    Backpressure,
    /// The send queue is closed or the session has started shutting down.
    Closed,
}

impl std::fmt::Display for WsSendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Backpressure => "WebSocket send queue capacity exceeded",
            Self::Closed => "WebSocket send queue is closed",
        })
    }
}

impl std::error::Error for WsSendError {}
