// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::VecDeque;

use axum::extract::ws::Message;

/// Tracks queued and in-flight message counts and byte charges under one lock.
#[derive(Debug)]
pub(in crate::ws) struct QueueState {
    /// Messages accepted but not yet removed by the writer task.
    pub(in crate::ws) messages: VecDeque<Message>,
    /// Bytes retained in queued messages and current in-flight sends.
    pub(in crate::ws) queued_bytes: usize,
    /// Messages removed from the queue but not yet completed by the sink.
    pub(in crate::ws) in_flight_messages: usize,
    /// Whether the queue rejects new messages and discards pending messages.
    pub(in crate::ws) closed: bool,
}
