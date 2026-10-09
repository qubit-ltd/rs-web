// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::sync::watch;

/// One active request or response-body lifetime.
pub(in crate::server::web_server) struct ActivityLease {
    active: watch::Sender<usize>,
}

impl ActivityLease {
    /// Creates a lease from the connection activity counter.
    pub(super) fn new(active: watch::Sender<usize>) -> Self {
        Self { active }
    }
}

impl Drop for ActivityLease {
    /// Decrements the connection activity count exactly once.
    fn drop(&mut self) {
        self.active.send_modify(|active| *active -= 1);
    }
}
