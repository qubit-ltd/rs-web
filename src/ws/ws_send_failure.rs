// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use axum::extract::ws::Message;

use super::ws_send_error::WsSendError;

/// A rejected outbound message together with the reason it was not queued.
///
/// The failure owns the original message, so callers can inspect, modify, or
/// retry it without reconstructing it. Formatting this value never exposes the
/// message or its payload.
///
/// # Example
///
/// ```
/// use axum::extract::ws::Message;
/// use qubit_web::ws::{WsSendError, WsSendQueue};
///
/// let queue = WsSendQueue::new(0, 1024);
/// let failure = queue.try_send_recoverable(Message::text("retryable"))
///     .expect_err("zero item capacity rejects admission");
/// let (kind, message) = failure.into_parts();
/// assert_eq!(kind, WsSendError::Backpressure);
/// assert_eq!(message.into_text().unwrap(), "retryable");
/// ```
#[must_use]
pub struct WsSendFailure {
    /// Admission failure category.
    kind: WsSendError,
    /// Original message rejected by the queue or session.
    message: Message,
}

impl WsSendFailure {
    /// Creates a failure that retains the rejected message.
    ///
    /// # Parameters
    ///
    /// * `kind` - Reason the message was rejected.
    /// * `message` - Original outbound message, moved into this failure.
    pub(super) fn new(kind: WsSendError, message: Message) -> Self {
        Self { kind, message }
    }

    /// Returns the category of the admission failure.
    ///
    /// # Returns
    ///
    /// The copyable error kind.
    #[must_use = "handle the send error category"]
    pub fn kind(&self) -> WsSendError {
        self.kind
    }

    /// Borrows the original message rejected by admission.
    ///
    /// # Returns
    ///
    /// A reference to the unchanged message retained by this failure.
    #[must_use]
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// Recovers the original rejected message.
    ///
    /// # Returns
    ///
    /// The message moved into this failure, without cloning or re-encoding.
    #[must_use]
    pub fn into_message(self) -> Message {
        self.message
    }

    /// Recovers both the error category and original rejected message.
    ///
    /// # Returns
    ///
    /// The failure kind and message, in that order.
    #[must_use = "handle the error category and rejected message"]
    pub fn into_parts(self) -> (WsSendError, Message) {
        (self.kind, self.message)
    }
}

impl std::fmt::Debug for WsSendFailure {
    /// Formats only the failure kind, never the rejected message.
    /// # Parameters
    ///
    /// * `formatter` - Destination receiving the safe diagnostic.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WsSendFailure")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::fmt::Display for WsSendFailure {
    /// Formats the stable diagnostic for the failure kind.
    /// # Parameters
    ///
    /// * `formatter` - Destination receiving the stable diagnostic.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.kind, formatter)
    }
}

impl std::error::Error for WsSendFailure {}

#[cfg(test)]
mod tests {
    use axum::extract::ws::Message;

    use super::WsSendFailure;
    use crate::ws::ws_send_error::WsSendError;

    #[test]
    fn test_debug_and_display_do_not_expose_rejected_payloads() {
        let failure = WsSendFailure::new(WsSendError::Backpressure, Message::text("private-text-sentinel"));
        let debug = format!("{failure:?}");
        let pretty_debug = format!("{failure:#?}");
        let display = failure.to_string();
        for formatted in [debug, pretty_debug, display] {
            assert!(!formatted.contains("private-text-sentinel"));
            assert!(!formatted.contains("Text("));
        }

        let failure = WsSendFailure::new(
            WsSendError::InvalidCloseCode,
            Message::Close(Some(axum::extract::ws::CloseFrame {
                code: 1005,
                reason: "private-close-sentinel".into(),
            })),
        );
        assert!(!format!("{failure:?}").contains("private-close-sentinel"));
        assert!(!format!("{failure:#?}").contains("private-close-sentinel"));
        assert!(!failure.to_string().contains("private-close-sentinel"));
        assert!(std::error::Error::source(&failure).is_none());

        for message in [
            Message::Binary(axum::body::Bytes::from_static(b"private-binary-sentinel")),
            Message::Ping(axum::body::Bytes::from_static(b"private-ping-sentinel")),
            Message::Pong(axum::body::Bytes::from_static(b"private-pong-sentinel")),
        ] {
            let failure = WsSendFailure::new(WsSendError::Backpressure, message);
            let debug = format!("{failure:?}");
            let pretty_debug = format!("{failure:#?}");
            let display = failure.to_string();
            for formatted in [debug, pretty_debug, display] {
                assert!(!formatted.contains("private-binary-sentinel"));
                assert!(!formatted.contains("private-ping-sentinel"));
                assert!(!formatted.contains("private-pong-sentinel"));
                assert!(!formatted.contains("[112, 114, 105"));
            }
        }
    }
}
