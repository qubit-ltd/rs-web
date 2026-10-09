// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio_util::sync::CancellationToken;

use crate::SessionGuard;

/// Owns the counters and cancellation state for one admitted SSE response.
pub(in crate::sse) struct SseConnectionGuard {
    /// Shared policy connection count decremented on drop.
    active_connections: Arc<AtomicUsize>,
    /// Server session count guard released when this reservation ends.
    session: Option<SessionGuard>,
    /// Producer token cancelled after the response stream ends or drops.
    cancellation: CancellationToken,
}

impl SseConnectionGuard {
    pub(in crate::sse) fn new(
        active_connections: Arc<AtomicUsize>,
        session: SessionGuard,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            active_connections,
            session: Some(session),
            cancellation,
        }
    }
}

impl Drop for SseConnectionGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::AcqRel);
        self.session.take();
        self.cancellation.cancel();
    }
}
