// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::sync::OwnedSemaphorePermit;

use crate::server::SessionGuard;

/// Connection capacity and server session registration shared by both I/O
/// tasks.
///
/// The permit and guard are released only after the reader and writer have both
/// dropped their `Arc` references to this value.
pub(in crate::ws) struct SessionResources {
    _permit: OwnedSemaphorePermit,
    _session: SessionGuard,
}

impl SessionResources {
    /// Holds the resources reserved for one admitted WebSocket connection.
    pub(in crate::ws) fn new(permit: OwnedSemaphorePermit, session: SessionGuard) -> Self {
        Self {
            _permit: permit,
            _session: session,
        }
    }
}
