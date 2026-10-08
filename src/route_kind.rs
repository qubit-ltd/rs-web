// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared classification for route registration and request policies.

/// Classification shared by route registration and request policies.
///
/// # Examples
///
/// ```
/// use qubit_web::RouteKind;
///
/// let kind = RouteKind::WebSocket;
/// assert_ne!(kind, RouteKind::Short);
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub enum RouteKind {
    /// A bounded, short-lived HTTP request.
    Short,
    /// A server-sent event stream.
    Sse,
    /// A WebSocket session.
    WebSocket,
}
