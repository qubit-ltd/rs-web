// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::ws::ws_send_queue::WsSendQueue;

/// Closes a send queue when its owning task exits.
pub(in crate::ws) struct CloseQueueOnDrop(WsSendQueue);

impl CloseQueueOnDrop {
    /// Creates a guard that closes `queue` when dropped.
    pub(in crate::ws) fn new(queue: WsSendQueue) -> Self {
        Self(queue)
    }
}

impl Drop for CloseQueueOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}
