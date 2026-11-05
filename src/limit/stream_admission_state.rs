// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared semaphore and budgets for a stream-admission capacity domain.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use super::stream_admission_limits::StreamAdmissionLimits;

/// Cloneable limits and capacity shared by admission middleware instances.
///
/// A newly constructed state is an independent capacity domain. Clones share
/// the same semaphore; the configured body-byte and admission-count defaults
/// are 1 MiB and 256, respectively, and apply per state domain.
///
/// # Example
///
/// ```
/// use axum::Router;
/// use axum::routing::post;
/// use qubit_web::{StreamAdmissionLayer, StreamAdmissionLimits, StreamAdmissionState};
///
/// async fn handler() -> &'static str { "ok" }
///
/// let state = StreamAdmissionState::new(
///     StreamAdmissionLimits::default().with_max_concurrent_admissions(4)?,
/// );
/// let app: Router<()> = Router::new()
///     .route("/events", post(handler).layer(StreamAdmissionLayer::from_state(state.clone())))
///     .route("/socket", post(handler).layer(StreamAdmissionLayer::from_state(state)));
/// # let _ = app;
/// # Ok::<(), qubit_web::WebServerError>(())
/// ```
#[derive(Clone)]
#[must_use]
pub struct StreamAdmissionState {
    /// Internal item implementation detail.
    pub(super) max_body_bytes: usize,
    /// Internal item implementation detail.
    pub(super) admission_timeout: Duration,
    /// Internal item implementation detail.
    pub(super) permits: Arc<Semaphore>,
}

impl StreamAdmissionState {
    /// Creates a state domain that can be cloned into multiple router branches.
    ///
    /// # Parameters
    ///
    /// - `limits`: validated body, capacity, and deadline budgets.
    ///
    /// # Returns
    ///
    /// A state whose clones share the same semaphore and therefore the same
    /// admission capacity domain.
    pub fn new(limits: StreamAdmissionLimits) -> Self {
        Self {
            max_body_bytes: limits.max_body_bytes(),
            admission_timeout: limits.admission_timeout(),
            permits: Arc::new(Semaphore::new(limits.max_concurrent_admissions())),
        }
    }
}
