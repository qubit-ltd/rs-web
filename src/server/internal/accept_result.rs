// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcomes produced by the transport accept retry policy.

use std::io;

/// Outcome of one transport accept attempt after retry policy is applied.
///
/// A transient error schedules another attempt; a terminal error is retained
/// until the accept loop completes shutdown cleanup.
pub(super) enum AcceptResult<T> {
    Accepted(T),
    Retry,
    Terminal(io::Error),
}
