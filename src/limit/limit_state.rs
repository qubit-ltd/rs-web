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

/// Shared body, deadline, and capacity state for selected route branches.
///
/// A state is one budget domain: requests using it share the same body and
/// deadline configuration and concurrent-request capacity. Cloning a state
/// shares that capacity. Each call to [`RequestLimitLayer::new`] creates an
/// independent domain; use [`RequestLimitLayer::from_state`] with clones of
/// this state to share capacity across branches. This is not a process-wide
/// limit. Controller builders create their own state by default; their explicit
/// shared-state setter can install this state, or a clone, for subsequently
/// registered Controllers.
///
/// [`RequestLimitLayer::new`]: super::request_limit_layer::RequestLimitLayer::new
/// [`RequestLimitLayer::from_state`]: super::request_limit_layer::RequestLimitLayer::from_state
#[derive(Clone)]
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
    /// Every independently created state has its own concurrent-request
    /// capacity. Clone the returned state and pass each clone to
    /// [`RequestLimitLayer::from_state`] to share one capacity across branches.
    ///
    /// # Parameters
    ///
    /// - `limits`: validated budgets copied into the branch state.
    ///
    /// # Returns
    ///
    /// State that shares one capacity semaphore across the branch's requests.
    ///
    /// [`RequestLimitLayer::from_state`]: super::request_limit_layer::RequestLimitLayer::from_state
    pub fn new(limits: HttpLimits) -> Self {
        Self {
            max_body_bytes: limits.max_body_bytes(),
            request_timeout: limits.request_timeout(),
            permits: Arc::new(Semaphore::new(limits.max_concurrent_requests())),
        }
    }
}
