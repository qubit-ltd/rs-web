// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared state for a response body wrapper and its deadline task.

use std::task::Waker;

use axum::body::Body;
use tokio::sync::OwnedSemaphorePermit;

use super::phase::Phase;

/// State shared by the response wrapper and its deadline task.
pub(super) struct BodyState {
    /// Current response body lifecycle phase.
    pub(super) phase: Phase,
    /// Application response body, removed as soon as it must stop producing.
    pub(super) inner: Option<Body>,
    /// Short-request capacity lease, released with the application body.
    pub(super) permit: Option<OwnedSemaphorePermit>,
    /// Most recent task waiting for another body frame.
    pub(super) waker: Option<Waker>,
}
