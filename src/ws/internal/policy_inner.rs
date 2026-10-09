// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

/// Immutable connection, frame, queue, and timeout settings shared by policy
/// clones.
#[derive(Clone, Debug)]
pub(in crate::ws) struct PolicyInner {
    /// Semaphore tracking concurrent upgraded connections.
    pub(in crate::ws) connections: Arc<Semaphore>,
    /// Configured connection capacity used alongside available permits.
    pub(in crate::ws) max_connections: usize,
    /// Maximum size of one WebSocket frame.
    pub(in crate::ws) max_frame_bytes: usize,
    /// Maximum size of one reassembled WebSocket message.
    pub(in crate::ws) max_message_bytes: usize,
    /// Maximum number of outbound messages queued or currently being sent.
    pub(in crate::ws) queue_messages: usize,
    /// Maximum total bytes held by queued or in-flight outbound messages.
    pub(in crate::ws) queue_bytes: usize,
    /// Maximum idle interval before a session is closed.
    pub(in crate::ws) idle_timeout: Duration,
    /// Exact allowed Origin values; `None` rejects requests that provide
    /// Origin.
    pub(in crate::ws) allowed_origins: Option<Vec<String>>,
    /// Whether any configured finite limit was set to an invalid value.
    pub(in crate::ws) invalid_limits: bool,
}
