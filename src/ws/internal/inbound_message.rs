// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use axum::extract::ws::Message;

use super::InboundState;

/// Message envelope that returns its payload bytes to the connection budget.
///
/// The charge is released when this envelope is dropped. Calling
/// [`into_message`](Self::into_message) transfers the message to application
/// owned memory, so that payload is no longer covered by the inbound queue
/// budget.
#[derive(Debug)]
pub(in crate::ws) struct InboundMessage {
    /// Message payload retained while the inbound budget is charged.
    message: Option<Message>,
    /// Payload bytes reserved from the connection budget.
    bytes: usize,
    /// Budget owner that receives `bytes` when this wrapper is dropped.
    state: Arc<InboundState>,
}

impl InboundMessage {
    /// Constructs the guard or state object from its owned resources.
    /// # Parameters
    ///
    /// * `message` - Inbound WebSocket frame retained for delivery.
    /// * `bytes` - Buffered byte count charged to inbound state.
    /// * `state` - Shared inbound accounting state that receives the byte
    ///   reservation when this wrapper is dropped.
    pub(in crate::ws) fn new(message: Message, bytes: usize, state: Arc<InboundState>) -> Self {
        Self {
            message: Some(message),
            bytes,
            state,
        }
    }

    /// Removes the message from this budgeted envelope for application use.
    ///
    /// The envelope then drops and releases its byte charge; the returned
    /// message is application owned and is not counted against the queue
    /// budget.
    pub(in crate::ws) fn into_message(mut self) -> Message {
        self.message.take().expect("inbound message is present")
    }
}

impl Drop for InboundMessage {
    /// Releases the resources owned by this guard.
    fn drop(&mut self) {
        self.state.release(self.bytes);
    }
}
