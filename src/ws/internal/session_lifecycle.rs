// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU16;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(in crate::ws) struct SessionLifecycle {
    pub(in crate::ws) shutdown: CancellationToken,
    pub(in crate::ws) close_code: Arc<AtomicU16>,
    pub(in crate::ws) close_deadline: Arc<Mutex<Option<Instant>>>,
    pub(in crate::ws) server_deadline: Option<Arc<Mutex<Option<Instant>>>>,
    pub(in crate::ws) shutdown_timeout: Duration,
}
