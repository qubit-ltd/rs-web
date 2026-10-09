// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub(in crate::ws) struct UpgradeLifecycle {
    /// Cancellation inherited by the upgraded session.
    pub(in crate::ws) shutdown: CancellationToken,
    /// Grace period allowed for the peer's close acknowledgement.
    pub(in crate::ws) shutdown_timeout: Duration,
    /// Shared absolute shutdown deadline, when a server context is available.
    pub(in crate::ws) server_deadline: Option<Arc<Mutex<Option<Instant>>>>,
}
