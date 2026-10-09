// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Complete JSON response encoder and problem response helper.

use axum::body::Body;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use qubit_json::encode::JsonEncodeErrorKind;
use qubit_json::encode::JsonEncoder;
use serde::Serialize;

use super::json_limits::JsonLimits;
use super::json_response_error::JsonResponseError;

/// Encodes a complete JSON response before creating its HTTP response body.
///
/// # Type Parameters
///
/// - `T`: serializable response value; it may be unsized behind a reference.
///
/// # Parameters
///
/// - `value`: data to serialize as JSON.
/// - `limits`: independent output and structural budgets for the encoder.
///
/// # Returns
///
/// A complete HTTP response with a JSON body and content type.
///
/// # Errors
///
/// Returns a privacy-safe error when encoding exceeds a budget, serialization
/// fails, or the HTTP response builder rejects its fixed response fields.
pub fn json_response<T: Serialize + ?Sized>(value: &T, limits: &JsonLimits) -> Result<Response, JsonResponseError> {
    let mut encoder = JsonEncoder::with_limits(limits.encode_limits());
    let bytes = encoder.to_vec(value).map_err(|error| JsonResponseError {
        budget_exceeded: error.kind() == JsonEncodeErrorKind::Budget,
    })?;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(bytes))
        .map_err(|_| JsonResponseError { budget_exceeded: false })
}

/// Builds a small problem response without embedding request or parser details.
///
/// # Parameters
///
/// - `status`: HTTP status to expose to the caller.
/// - `code`: stable public code for the rejection.
/// - `title`: safe summary suitable for a response body.
///
/// # Returns
///
/// An HTTP problem response with no request content included.
#[must_use]
pub(super) fn problem_response(status: StatusCode, code: &str, title: &str) -> Response {
    let body = format!(
        "{{\"type\":\"about:blank\",\"title\":\"{title}\",\"status\":{},\"code\":\"{code}\"}}",
        status.as_u16()
    );
    (status, [(header::CONTENT_TYPE, "application/problem+json")], body).into_response()
}
