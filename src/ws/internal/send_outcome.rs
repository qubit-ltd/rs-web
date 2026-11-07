// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcomes of one attempt to send a queued WebSocket message.

/// Result selected when sending, shutdown, or peer close completes first.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum SendOutcome {
    /// The sink accepted the message send operation; this does not guarantee
    /// delivery to the peer.
    Sent,
    /// The peer's close acknowledgment won the send selection.
    PeerCloseAcknowledged,
    /// The sink returned an error while sending.
    SinkError,
    /// Session shutdown cancellation won the send selection.
    Shutdown,
    /// The ordinary send did not complete before its write deadline.
    TimedOut,
}
