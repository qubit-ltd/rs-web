// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Outcome of processing one listener accept result.
///
/// # Type Parameters
///
/// * `T` - Accepted connection value produced by the listener path.
pub(super) enum AcceptResult<T> {
    /// The listener accepted a transport and the permit remains with it.
    Accepted(
        /// Accepted connection value.
        T,
    ),
    /// The I/O error is transient and another accept may be attempted.
    Retry,
    /// The listener can no longer accept and serving must stop.
    Terminal(
        /// Error explaining why no further accept may be attempted.
        std::io::Error,
    ),
}
