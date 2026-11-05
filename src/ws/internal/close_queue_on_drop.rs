// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::ws::ws_send_queue::WsSendQueue;

/// Closes the associated send queue when the owning task exits.
pub(in crate::ws) struct CloseQueueOnDrop(
    /// Send queue closed when this guard is dropped.
    WsSendQueue,
);

impl CloseQueueOnDrop {
    /// Creates a guard that closes `queue` when dropped.
    /// # Parameters
    ///
    /// * `queue` - Send queue to close when this guard is dropped.
    pub(in crate::ws) fn new(queue: WsSendQueue) -> Self {
        Self(queue)
    }
}

impl Drop for CloseQueueOnDrop {
    /// Releases the resources owned by this guard.
    fn drop(&mut self) {
        self.0.close();
    }
}
