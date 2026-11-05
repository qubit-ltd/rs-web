// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;

/// Per-connection HTTP settings copied from the listener options.
///
/// # Fields
///
/// The fields carry the connection-level timeout and protocol limits derived
/// from the bound server's options.
#[derive(Clone, Copy)]
pub(super) struct ConnectionSettings {
    /// Deadline for receiving HTTP/1 request headers.
    pub(super) request_header_timeout: Duration,
    /// Maximum duration without an active request or response body.
    pub(super) transport_idle_timeout: Duration,
    /// Maximum concurrent HTTP/2 streams on this connection.
    pub(super) http2_max_concurrent_streams: u32,
    /// Optional maximum HTTP/1 parser buffer size.
    pub(super) http1_max_buffer_bytes: Option<usize>,
    /// Optional HTTP/2 decoded header-list limit.
    pub(super) http2_max_header_list_bytes: Option<u32>,
}
