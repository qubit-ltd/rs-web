// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
/// Safe local classification of why a managed WebSocket session ended.
///
/// A peer close only means that a Close frame was read; it does not confirm
/// handshake completion or delivery. `IoEnded` likewise does not imply
/// success. This type intentionally contains no peer supplied text or errors.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsTermination {
    /// A Close frame was read from the peer.
    PeerClose { code: Option<u16> },
    /// The application explicitly closed the session.
    LocalClose,
    /// The application handler ended or dropped the session.
    HandlerDropped,
    /// The server began shutting down.
    ServerShutdown,
    /// An inbound idle deadline elapsed.
    IdleTimeout,
    /// The inbound queue or byte budget could not accept a message.
    InboundBackpressure,
    /// A write deadline elapsed.
    WriteTimeout,
    /// Reading failed.
    ReadError,
    /// Writing failed.
    WriteError,
    /// I/O ended without a more specific recorded cause.
    IoEnded,
}
