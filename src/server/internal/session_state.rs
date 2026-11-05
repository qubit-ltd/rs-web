// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::time::Instant;
/// Mutable server session counts and shutdown completion history.
#[derive(Debug)]
pub(in crate::server::web_server) struct SessionState {
    /// Internal item implementation detail.
    pub(in crate::server::web_server) active: usize,
    /// Internal item implementation detail.
    pub(in crate::server::web_server) draining: bool,
    /// Internal item implementation detail.
    pub(in crate::server::web_server) completed_during_shutdown: Vec<Instant>,
}
