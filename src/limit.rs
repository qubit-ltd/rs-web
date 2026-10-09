// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Finite HTTP request limits for explicitly protected router branches.

mod http_limits;
mod internal;
mod limit_state;
mod request_limit_layer;
mod web_rejection;

pub use http_limits::HttpLimits;
pub use limit_state::LimitState;
#[doc(hidden)]
pub use request_limit_layer::LimitFuture;
#[doc(hidden)]
pub use request_limit_layer::LimitMiddleware;
pub use request_limit_layer::RequestLimitLayer;
pub use web_rejection::WebRejection;
