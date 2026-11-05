// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use axum::Error;
use axum::extract::ws::Message;

use super::InboundMessage;
use crate::ws::ws_send_queue::message_size;

/// Per-connection inbound byte accounting and first read failure.
///
/// The byte limit covers payloads held in the inbound channel or waiting to be
/// delivered. Once [`InboundMessage::into_message`] transfers a message to the
/// application, its bytes are no longer included. The first read error is
/// retained until consumed once or explicitly cleared.
#[derive(Debug)]
pub(in crate::ws) struct InboundState {
    /// Maximum payload bytes reserved by queued inbound messages.
    max_bytes: usize,
    /// Currently reserved queued or pending-delivery payload bytes.
    used_bytes: Mutex<usize>,
    /// First read error waiting for the application to consume it.
    read_error: Mutex<Option<Error>>,
}

impl InboundState {
    /// Creates an empty per-connection budget with the given payload limit.
    /// # Parameters
    ///
    /// * `max_bytes` - Maximum number of queued message bytes.
    pub(in crate::ws) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            used_bytes: Mutex::new(0),
            read_error: Mutex::new(None),
        }
    }

    /// Reserves payload bytes and wraps a message while it is queued for
    /// delivery.
    ///
    /// Returns the original message unchanged when its payload would exceed the
    /// remaining budget. The reservation is returned when the wrapper is
    /// dropped.
    /// # Parameters
    ///
    /// * `message` - WebSocket message being queued, wrapped, or delivered.
    pub(in crate::ws) fn try_wrap(self: &Arc<Self>, message: Message) -> Result<InboundMessage, Message> {
        let bytes = message_size(&message);
        let mut used = self.used_bytes.lock().unwrap_or_else(PoisonError::into_inner);
        if bytes > self.max_bytes.saturating_sub(*used) {
            return Err(message);
        }
        *used += bytes;
        drop(used);
        Ok(InboundMessage::new(message, bytes, self.clone()))
    }

    /// Returns a wrapper's reserved payload bytes to this connection's budget.
    /// # Parameters
    ///
    /// * `bytes` - Number of message or body bytes charged to the resource
    pub(in crate::ws) fn release(&self, bytes: usize) {
        let mut used = self.used_bytes.lock().unwrap_or_else(PoisonError::into_inner);
        *used = used.saturating_sub(bytes);
    }

    /// Stores a read error only if no earlier error is waiting for delivery.
    /// # Parameters
    ///
    /// * `error` - Error returned by the underlying I/O or service operation.
    pub(in crate::ws) fn record_read_error(&self, error: Error) {
        let mut slot = self.read_error.lock().unwrap_or_else(PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(error);
        }
    }

    /// Takes the retained read error; subsequent calls return `None`.
    pub(in crate::ws) fn take_read_error(&self) -> Option<Error> {
        self.read_error.lock().unwrap_or_else(PoisonError::into_inner).take()
    }

    /// Discards the retained read error without making it available to a later
    /// consumer. Explicit close handling calls this when the close handshake
    /// takes precedence over an earlier read failure; subsequent reads then
    /// observe no retained error.
    pub(in crate::ws) fn clear_read_error(&self) {
        let _ = self.read_error.lock().unwrap_or_else(PoisonError::into_inner).take();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Error;
    use axum::extract::ws::Message;

    use super::InboundState;

    #[test]
    fn test_byte_budget_accepts_exact_limit_and_releases_on_drop() {
        let state = Arc::new(InboundState::new(4));
        let first = state.try_wrap(Message::text("four")).expect("exact limit fits");
        assert!(state.try_wrap(Message::text("x")).is_err());
        drop(first);
        assert!(state.try_wrap(Message::text("four")).is_ok());
    }

    #[test]
    fn test_first_read_error_is_consumed_once() {
        let state = InboundState::new(4);
        state.record_read_error(Error::new(std::io::Error::other("first")));
        state.record_read_error(Error::new(std::io::Error::other("second")));
        let error = state.take_read_error().expect("first error is retained");
        assert_eq!(error.to_string(), "first");
        assert!(state.take_read_error().is_none());
    }

    #[tokio::test]
    async fn test_failed_channel_send_releases_reserved_bytes() {
        let state = Arc::new(InboundState::new(4));
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        let message = state.try_wrap(Message::text("four")).expect("budget should fit");
        let error = sender.send(message).await.expect_err("closed receiver rejects send");
        drop(error.0);
        assert!(state.try_wrap(Message::text("four")).is_ok());
    }

    #[tokio::test]
    async fn test_receiver_drop_releases_queued_bytes() {
        let state = Arc::new(InboundState::new(4));
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let message = state.try_wrap(Message::text("four")).expect("budget should fit");
        sender.send(message).await.expect("receiver is open");
        drop(receiver);
        assert!(state.try_wrap(Message::text("four")).is_ok());
    }
}
