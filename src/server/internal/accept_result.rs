// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Outcome of processing one listener accept result.
pub(super) enum AcceptResult<T> {
    /// The listener accepted a transport and the permit remains with it.
    Accepted(T),
    /// The I/O error is transient and another accept may be attempted.
    Retry,
    /// The listener can no longer accept and serving must stop.
    Terminal(std::io::Error),
}
