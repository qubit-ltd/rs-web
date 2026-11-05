// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use super::InboundState;
use super::SessionResources;
/// Reader-owned state and resources retained for the entire reader task.
pub(in crate::ws) struct ReaderContext {
    /// Mutable state used to wrap inbound messages.
    pub(super) inbound_state: Arc<InboundState>,
    /// Keeps the connection permit and session guard alive until this task
    /// exits.
    pub(super) _resources: Arc<SessionResources>,
}

impl ReaderContext {
    /// Creates reader context while retaining the connection's shared
    /// resources.
    ///
    /// The permit and session guard are released when the reader and writer
    /// tasks have both dropped their references to the shared resources.
    /// # Parameters
    ///
    /// * `inbound_state` - Shared state used to account for inbound messages.
    /// * `resources` - Connection permit and session guard retained by the
    ///   reader and writer tasks.
    pub(in crate::ws) fn new(inbound_state: Arc<InboundState>, resources: Arc<SessionResources>) -> Self {
        Self {
            inbound_state,
            _resources: resources,
        }
    }
}
