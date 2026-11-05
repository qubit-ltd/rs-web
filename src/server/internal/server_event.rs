// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Event selected while the listener and accepted connections are active.
///
/// # Type Parameters
///
/// * `A` - Result type returned by the listener accept operation.
pub(super) enum ServerEvent<A> {
    /// The host requested shutdown.
    Shutdown,
    /// The accept future completed with its output.
    Accept(
        /// Result returned by the listener's accept future.
        A,
    ),
    /// A completed connection task was reaped.
    Joined,
}
