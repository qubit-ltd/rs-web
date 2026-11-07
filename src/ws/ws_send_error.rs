// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Error returned when an outbound message cannot be queued.
///
/// Admission failures are checked in this order: [`Closed`](Self::Closed),
/// [`InvalidCloseCode`](Self::InvalidCloseCode),
/// [`MessageTooLarge`](Self::MessageTooLarge), then
/// [`Backpressure`](Self::Backpressure). `InvalidCloseCode` is permanent for
/// that Close frame; correct the code instead of retrying it. The older
/// `try_send` APIs discard rejected messages. Use `try_send_recoverable` when
/// the application needs to retain ownership of a rejected message.
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
///     let queue = qubit_web::ws::WsSendQueue::new(0, 1024);
///     queue.try_send(axum::extract::ws::Message::text("private-payload"))?;
///     Ok(())
/// }
///
/// let error = enqueue().unwrap_err();
/// assert!(error.downcast_ref::<WsSendError>().is_some());
/// assert_eq!(error.to_string(), "WebSocket send queue capacity exceeded");
/// ```
///
/// Adding variants can affect exhaustive matches. Handle
/// [`InvalidCloseCode`](Self::InvalidCloseCode) by correcting the Close code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum WsSendError {
    /// The queue lacks item or remaining byte capacity for the message.
    Backpressure,
    /// The message exceeds a WebSocket control-frame limit or the configured
    /// total outbound byte budget. Ping/Pong payloads are limited to 125 bytes;
    /// a Close reason is limited to 123 UTF-8 bytes.
    MessageTooLarge,
    /// The Close frame uses a status code a server cannot send.
    InvalidCloseCode,
    /// The send queue is closed or the session has started shutting down.
    Closed,
}

impl std::fmt::Display for WsSendError {
    /// Formats the value using its stable diagnostic representation.
    /// # Parameters
    ///
    /// * `formatter` - Destination receiving the formatted send error.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Backpressure => "WebSocket send queue capacity exceeded",
            Self::MessageTooLarge => "WebSocket message exceeds the outbound limit",
            Self::InvalidCloseCode => "WebSocket close code is invalid for a server",
            Self::Closed => "WebSocket send queue is closed",
        })
    }
}

impl std::error::Error for WsSendError {}
