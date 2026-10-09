// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Error returned when an outbound message cannot be queued.
///
/// A message accepted before shutdown may still be discarded if it remains
/// pending when the session closes.
///
/// # Examples
///
/// ```
/// use qubit_web::ws::WsSendError;
///
/// let error = WsSendError::Backpressure;
/// assert_eq!(format!("{error:?}"), "Backpressure");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum WsSendError {
    /// The queue reached either its item or byte limit.
    Backpressure,
    /// The send queue is closed or the session has started shutting down.
    Closed,
}
