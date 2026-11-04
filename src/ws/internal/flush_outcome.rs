// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcomes of flushing the WebSocket sink.

/// Result of a flush attempt, including cancellation and deadline expiry.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum FlushOutcome {
    /// The sink flush operation completed successfully.
    Flushed,
    /// Session shutdown cancellation won the flush selection.
    Cancelled,
    /// The sink returned an error while flushing.
    Failed,
    /// The optional flush deadline expired.
    TimedOut,
}
