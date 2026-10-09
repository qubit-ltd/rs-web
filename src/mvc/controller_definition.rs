// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use axum::Router;

use super::RouteMetadata;
use crate::limit::LimitState;

/// Internal contract implemented by `#[rest_controller]` expansions.
///
/// Implementations must keep `route_metadata` accurate: it must describe every
/// route inserted by `register`, with the same path and method. The assembly
/// builder preflights this metadata before calling `register`; a manual
/// implementation that diverges from it, or inserts conflicting routes itself,
/// can still trigger Axum's route conflict panic.
///
/// # Type Parameters
///
/// * `S` - Cloneable, thread-safe Axum router state shared by registered
///   routes.
#[doc(hidden)]
pub trait ControllerDefinition<S>: Send + Sync + 'static
where
    S: Clone + Send + Sync + 'static,
{
    /// Static metadata used to reject conflicts before inserting routes.
    fn route_metadata() -> &'static [RouteMetadata];

    /// Adds all routes for one explicitly supplied Controller instance.
    fn register(self: Arc<Self>, router: Router<S>, limit_state: LimitState) -> Router<S>;
}
