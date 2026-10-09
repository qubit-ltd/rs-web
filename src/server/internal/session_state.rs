// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::time::Instant;

#[derive(Debug)]
pub(in crate::server::web_server) struct SessionState {
    pub(in crate::server::web_server) active: usize,
    pub(in crate::server::web_server) draining: bool,
    pub(in crate::server::web_server) completed_during_shutdown: Vec<Instant>,
}
