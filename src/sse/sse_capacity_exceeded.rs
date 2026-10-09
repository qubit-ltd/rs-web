// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use axum::response::IntoResponse;
use axum::response::Response;

use crate::limit::WebRejection;

/// Returned when the configured number of simultaneous SSE streams is already
/// active. Prefer [`SseAdmissionError`](crate::sse::SseAdmissionError) for new
/// code.
///
/// # Examples
///
/// ```
/// use qubit_web::sse::SseCapacityExceeded;
///
/// let error = SseCapacityExceeded;
/// assert_eq!(format!("{error:?}"), "SseCapacityExceeded");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub struct SseCapacityExceeded;

impl IntoResponse for SseCapacityExceeded {
    fn into_response(self) -> Response {
        WebRejection::CapacityExceeded.into_response()
    }
}
