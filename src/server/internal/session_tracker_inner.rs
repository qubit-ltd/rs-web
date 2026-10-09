// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Mutex;

use tokio::sync::watch;

use super::session_state::SessionState;

#[derive(Debug)]
pub(in crate::server::web_server) struct SessionTrackerInner {
    pub(in crate::server::web_server) state: Mutex<SessionState>,
    pub(in crate::server::web_server) updates: watch::Sender<usize>,
}
