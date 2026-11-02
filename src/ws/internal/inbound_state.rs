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
#[derive(Debug)]
pub(in crate::ws) struct InboundState {
    max_bytes: usize,
    used_bytes: Mutex<usize>,
    read_error: Mutex<Option<Error>>,
}

impl InboundState {
    pub(in crate::ws) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            used_bytes: Mutex::new(0),
            read_error: Mutex::new(None),
        }
    }

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

    pub(in crate::ws) fn release(&self, bytes: usize) {
        let mut used = self.used_bytes.lock().unwrap_or_else(PoisonError::into_inner);
        *used = used.saturating_sub(bytes);
    }

    pub(in crate::ws) fn record_read_error(&self, error: Error) {
        let mut slot = self.read_error.lock().unwrap_or_else(PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(error);
        }
    }

    pub(in crate::ws) fn take_read_error(&self) -> Option<Error> {
        self.read_error.lock().unwrap_or_else(PoisonError::into_inner).take()
    }

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
    fn byte_budget_accepts_exact_limit_and_releases_on_drop() {
        let state = Arc::new(InboundState::new(4));
        let first = state.try_wrap(Message::text("four")).expect("exact limit fits");
        assert!(state.try_wrap(Message::text("x")).is_err());
        drop(first);
        assert!(state.try_wrap(Message::text("four")).is_ok());
    }

    #[test]
    fn first_read_error_is_consumed_once() {
        let state = InboundState::new(4);
        state.record_read_error(Error::new(std::io::Error::other("first")));
        state.record_read_error(Error::new(std::io::Error::other("second")));
        let error = state.take_read_error().expect("first error is retained");
        assert_eq!(error.to_string(), "first");
        assert!(state.take_read_error().is_none());
    }

    #[tokio::test]
    async fn failed_channel_send_releases_reserved_bytes() {
        let state = Arc::new(InboundState::new(4));
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        let message = state.try_wrap(Message::text("four")).expect("budget should fit");
        let error = sender.send(message).await.expect_err("closed receiver rejects send");
        drop(error.0);
        assert!(state.try_wrap(Message::text("four")).is_ok());
    }

    #[tokio::test]
    async fn receiver_drop_releases_queued_bytes() {
        let state = Arc::new(InboundState::new(4));
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let message = state.try_wrap(Message::text("four")).expect("budget should fit");
        sender.send(message).await.expect("receiver is open");
        drop(receiver);
        assert!(state.try_wrap(Message::text("four")).is_ok());
    }
}
