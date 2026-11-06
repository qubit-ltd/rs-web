// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Result of forwarding a frame to the bounded application channel.
pub(in crate::ws) enum DeliveryResult {
    /// The frame was enqueued; the reader may continue receiving.
    Delivered,
    /// The application receiver is gone; the reader closes the outbound queue
    /// and exits.
    ReceiverClosed,
    /// Shutdown cancellation won the delivery race; the reader begins close
    /// handling with a deadline.
    Cancelled,
    /// The bounded send timed out (including an unrepresentable deadline); the
    /// reader sets close code 1013 and begins close handling.
    TimedOut,
}
