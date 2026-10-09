// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Strict JSON request extractor.

use axum::body::to_bytes;
use axum::extract::FromRequest;
use axum::extract::Request;
use axum::http::header;
use qubit_json::decode::JsonDecodeErrorKind;
use qubit_json::decode::JsonDecoder;
use serde::de::DeserializeOwned;

use super::internal::content_type::is_json_content_type;
use super::json_limits::JsonLimits;
use super::json_rejection::JsonRejection;

/// A request JSON value decoded under the selected [`JsonLimits`].
///
/// # Type Parameters
///
/// - `T`: deserialized value type expected by the request handler.
///
/// # Examples
///
/// ```
/// use qubit_web::BoundedJson;
///
/// let BoundedJson(value) = BoundedJson(42_u32);
/// assert_eq!(value, 42);
/// ```
#[must_use]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedJson<T>(
    /// The value decoded from the request body.
    pub T,
);

impl<S, T> FromRequest<S> for BoundedJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = JsonRejection;

    /// Decodes a JSON request body under extension-provided or default budgets.
    ///
    /// # Parameters
    ///
    /// - `request`: incoming request; its body is consumed by the extractor.
    /// - `_state`: router state, unused because limits are carried in
    ///   extensions.
    ///
    /// # Returns
    ///
    /// The decoded value wrapped in `BoundedJson`.
    ///
    /// # Errors
    ///
    /// Rejects unsupported media types, oversized bodies, or invalid JSON.
    async fn from_request(request: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let limits = request.extensions().get::<JsonLimits>().copied().unwrap_or_default();
        if !is_json_content_type(request.headers().get(header::CONTENT_TYPE)) {
            return Err(JsonRejection::UnsupportedMediaType);
        }
        let (_, body) = request.into_parts();
        let bytes = to_bytes(body, limits.max_input_bytes.get())
            .await
            .map_err(|_| JsonRejection::BudgetExceeded)?;
        let mut decoder = JsonDecoder::with_limits(limits.decode_limits());
        decoder.decode_utf8::<T>(&bytes).map(BoundedJson).map_err(|error| {
            if error.kind() == JsonDecodeErrorKind::Budget {
                JsonRejection::BudgetExceeded
            } else {
                JsonRejection::InvalidJson
            }
        })
    }
}
