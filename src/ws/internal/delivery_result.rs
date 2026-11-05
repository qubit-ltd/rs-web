// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Result of forwarding a frame to the bounded application channel.
pub(in crate::ws) enum DeliveryResult {
    /// Internal item implementation detail.
    Delivered,
    /// Internal item implementation detail.
    ReceiverClosed,
    /// Internal item implementation detail.
    Cancelled,
    /// Internal item implementation detail.
    TimedOut,
}
