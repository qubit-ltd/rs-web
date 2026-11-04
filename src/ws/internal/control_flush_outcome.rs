// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcomes of the writer's control-path flush selection.

use super::flush_outcome::FlushOutcome;

/// Distinguishes peer-close acknowledgment from the flush attempt it races.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum ControlFlushOutcome {
    /// The peer-close acknowledgment completed before the flush result.
    PeerCloseAcknowledged,
    /// The flush attempt completed, was cancelled, failed, or timed out.
    Flushed(FlushOutcome),
}
