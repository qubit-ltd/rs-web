// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::response::Response;

use crate::limit::WebRejection;

/// Reports why an SSE connection reservation was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum SseAdmissionError {
    /// The configured number of simultaneous SSE streams is already active.
    CapacityExceeded,
    /// The server has begun shutting down and no longer accepts SSE sessions.
    ShuttingDown,
}

impl IntoResponse for SseAdmissionError {
    fn into_response(self) -> Response {
        match self {
            Self::CapacityExceeded => WebRejection::CapacityExceeded.into_response(),
            Self::ShuttingDown => (
                StatusCode::SERVICE_UNAVAILABLE,
                [(CONTENT_TYPE, "application/problem+json")],
                r#"{"type":"about:blank","title":"Service Unavailable","status":503,"code":"server_shutting_down"}"#,
            )
                .into_response(),
        }
    }
}
