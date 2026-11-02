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
#[derive(Debug)]
pub(in crate::ws) struct InboundMessage {
    message: Option<Message>,
    bytes: usize,
    state: Arc<InboundState>,
}

impl InboundMessage {
    pub(in crate::ws) fn new(message: Message, bytes: usize, state: Arc<InboundState>) -> Self {
        Self {
            message: Some(message),
            bytes,
            state,
        }
    }

    pub(in crate::ws) fn into_message(mut self) -> Message {
        self.message.take().expect("inbound message is present")
    }
}

impl Drop for InboundMessage {
    fn drop(&mut self) {
        self.state.release(self.bytes);
    }
}
