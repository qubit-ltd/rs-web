// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Lifecycle phases for a response body guarded by its request deadline.

/// Lifecycle of a response body guarded by its request deadline.
pub(super) enum Phase {
    /// The application body may still produce frames.
    Active,
    /// The deadline elapsed and the timeout error has not yet been emitted.
    TimedOut,
    /// The body completed, failed, or was dropped.
    Finished,
}
