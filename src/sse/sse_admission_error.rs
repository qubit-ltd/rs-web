// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;

use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::response::Response;

use crate::limit::WebRejection;

/// Reports why an SSE connection reservation was rejected.
///
/// The error supports standard error propagation without changing its HTTP
/// response.
///
/// # Examples
///
/// ```rust
/// use qubit_web::ServerContext;
/// use qubit_web::SseConnectionPolicy;
/// use qubit_web::sse::SseConnection;
///
/// fn reserve(
///     policy: &SseConnectionPolicy,
///     context: &ServerContext,
/// ) -> Result<SseConnection, Box<dyn std::error::Error + Send + Sync>> {
///     Ok(policy.begin(context)?)
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum SseAdmissionError {
    /// The configured number of simultaneous SSE streams is already active.
    CapacityExceeded,
    /// The server has begun shutting down and no longer accepts SSE sessions.
    ShuttingDown,
}

impl fmt::Display for SseAdmissionError {
    /// Formats the fixed, value-free admission rejection message.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Formatter receiving the rejection message.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CapacityExceeded => "SSE connection capacity exceeded",
            Self::ShuttingDown => "server is shutting down",
        })
    }
}

impl std::error::Error for SseAdmissionError {}

impl IntoResponse for SseAdmissionError {
    /// Converts this policy rejection into an HTTP response.
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
