// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use axum::Error;
use axum::extract::ws::Message;
use tokio::time::error::Elapsed;

/// Result of waiting for an inbound frame under an optional idle deadline.
pub(super) enum ReaderWaitResult {
    /// A deadline-backed wait. The nested result distinguishes timeout,
    /// stream completion, a received frame, and a stream read error.
    Timed(
        /// Timeout-wrapped stream result from the reader.
        Result<Option<Result<Message, Error>>, Elapsed>,
    ),
    /// No idle deadline could be represented by Tokio's clock.
    IdleDeadlineUnrepresentable,
}
