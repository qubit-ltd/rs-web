// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Safe local classification of why a managed WebSocket session ended.
///
/// A peer close only means that a Close frame was read; it does not confirm
/// handshake completion or delivery. `IoEnded` likewise does not imply
/// success. This type intentionally contains no peer supplied text or errors.
///
/// # Examples
///
/// ```
/// use qubit_web::ws::WsTermination;
///
/// let cause = WsTermination::PeerClose { code: Some(1000) };
/// assert_eq!(cause, WsTermination::PeerClose { code: Some(1000) });
/// ```
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsTermination {
    /// A Close frame was read from the peer.
    ///
    /// # Parameters
    ///
    /// * `code` - Close code supplied by the peer, if present.
    PeerClose {
        /// Close code supplied by the peer, if present.
        code: Option<u16>,
    },
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
