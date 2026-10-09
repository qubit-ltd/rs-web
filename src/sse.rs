// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Server-sent event connection limits, keep-alive, and cancellation.

mod sse_admission_error;
mod sse_capacity_exceeded;
mod sse_connection;
mod sse_connection_policy;
mod internal {
    mod session_stream;
    mod sse_connection_guard;

    pub(super) use session_stream::SessionStream;
    pub(super) use sse_connection_guard::SseConnectionGuard;
}

pub use sse_admission_error::SseAdmissionError;
pub use sse_capacity_exceeded::SseCapacityExceeded;
pub use sse_connection::SseConnection;
pub use sse_connection_policy::SseConnectionPolicy;
