// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Invalid WebSocket policy configuration.
///
/// # Examples
///
/// ```
/// use qubit_web::ws::WsPolicyError;
///
/// let error = WsPolicyError::ZeroLimit;
/// assert_eq!(format!("{error:?}"), "ZeroLimit");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum WsPolicyError {
    /// A configured connection, frame, message, queue, or timeout limit was
    /// zero.
    ZeroLimit,
}
