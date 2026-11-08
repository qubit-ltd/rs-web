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

use super::TerminationState;
/// Shared close, cancellation, and deadline state for one session.
#[derive(Clone)]
pub(in crate::ws) struct SessionLifecycle {
    /// Child cancellation token shared by the session handler and I/O drivers.
    pub(in crate::ws) shutdown: CancellationToken,
    /// Parent server token, used to distinguish server shutdown from local
    /// cleanup.
    pub(in crate::ws) server_shutdown: CancellationToken,
    /// Shared session termination candidate state.
    pub(in crate::ws) termination: Arc<TerminationState>,
    /// Protocol code selected for the close frame sent during shutdown.
    pub(in crate::ws) close_code: Arc<AtomicU16>,
    /// Absolute deadline established by the first session close request.
    pub(in crate::ws) close_deadline: Arc<Mutex<Option<Instant>>>,
    /// Optional absolute server shutdown deadline, which can shorten session
    /// close.
    pub(in crate::ws) server_deadline: Option<Arc<Mutex<Option<Instant>>>>,
    /// Maximum close wait when no earlier absolute deadline applies.
    pub(in crate::ws) shutdown_timeout: Duration,
}
