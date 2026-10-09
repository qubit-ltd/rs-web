// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stable failures returned by bounded JSON extraction.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;

use super::json_response::problem_response;

/// Stable rejection returned by [`BoundedJson`] extraction.
#[doc(hidden)]
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonRejection {
    /// The body was not valid JSON for the requested type.
    InvalidJson,
    /// The input exceeded a configured byte or structural budget.
    BudgetExceeded,
    /// The request body could not be read completely.
    BodyReadFailed,
    /// The request did not use a supported JSON media type.
    UnsupportedMediaType,
}

impl IntoResponse for JsonRejection {
    /// Converts a stable rejection category into a safe problem response.
    ///
    /// # Returns
    ///
    /// A response whose status and code identify the rejection without exposing
    /// request content or parser details.
    fn into_response(self) -> Response {
        let (status, code, title) = match self {
            Self::InvalidJson => (StatusCode::BAD_REQUEST, "invalid_json", "Invalid JSON"),
            Self::BudgetExceeded => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "json_budget_exceeded",
                "JSON Budget Exceeded",
            ),
            Self::BodyReadFailed => (
                StatusCode::BAD_REQUEST,
                "json_body_read_failed",
                "Failed to read JSON body",
            ),
            Self::UnsupportedMediaType => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Unsupported Media Type",
            ),
        };
        problem_response(status, code, title)
    }
}
