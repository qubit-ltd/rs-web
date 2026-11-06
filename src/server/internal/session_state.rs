// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::time::Instant;
/// Session tracker state protected by the tracker's mutex.
#[derive(Debug)]
pub(in crate::server::web_server) struct SessionState {
    /// Number of registered session guards not yet dropped.
    pub(in crate::server::web_server) active: usize,
    /// Whether shutdown has closed admission to new sessions.
    pub(in crate::server::web_server) draining: bool,
    /// Completion instants recorded after admission closes; used to reconstruct
    /// how many sessions were still active at the shutdown deadline.
    pub(in crate::server::web_server) completed_during_shutdown: Vec<Instant>,
}
