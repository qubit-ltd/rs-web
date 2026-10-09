// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared state installed on a limited router branch.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use super::http_limits::HttpLimits;

/// Shared body, deadline, and capacity state for one selected route branch.
#[derive(Clone)]
#[doc(hidden)]
#[must_use]
pub struct LimitState {
    /// Body size threshold applied while a handler consumes request data.
    pub(super) max_body_bytes: usize,
    /// Deadline applied to processing and response streaming.
    pub(super) request_timeout: Duration,
    /// Shared capacity semaphore for concurrent short requests.
    pub(super) permits: Arc<Semaphore>,
}

impl LimitState {
    /// Creates the shared state used by selected short-request branches.
    ///
    /// # Parameters
    ///
    /// - `limits`: validated budgets copied into the branch state.
    ///
    /// # Returns
    ///
    /// State that shares one capacity semaphore across the branch's requests.
    #[doc(hidden)]
    pub fn new(limits: HttpLimits) -> Self {
        Self {
            max_body_bytes: limits.max_body_bytes(),
            request_timeout: limits.request_timeout(),
            permits: Arc::new(Semaphore::new(limits.max_concurrent_requests())),
        }
    }
}
