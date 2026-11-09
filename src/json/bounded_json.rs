// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Strict JSON request extractor. Input byte and structure budgets do not
//! guarantee a CPU time bound for user-provided `Deserialize` implementations;
//! decoding is synchronous and does not yield to Tokio while it runs.

use axum::body::Bytes;
use axum::extract::FromRequest;
use axum::extract::Request;
use axum::http::header;
use http_body_util::BodyExt;
use http_body_util::Limited;
use qubit_json::decode::JsonDecodeErrorKind;
use qubit_json::decode::JsonDecoder;
use serde::de::DeserializeOwned;

use super::internal::content_type::is_json_content_type;
use super::json_limits::JsonLimits;
use super::json_rejection::JsonRejection;

/// A request JSON value decoded under the selected [`JsonLimits`].
///
/// The extractor accepts `application/json` and valid `application/*+json`
/// media types with no parameters or a single `charset=utf-8` parameter. The
/// parameter name and value are case-insensitive, and the UTF-8 value may be
/// double-quoted. Other parameters, duplicate charset parameters, non-UTF-8
/// charsets, and malformed parameters are rejected with HTTP 415 as
/// [`JsonRejection::UnsupportedMediaType`].
///
/// While buffering, `BoundedJson` copies each data frame into a `Vec` and
/// drops the frame's `Bytes` handle before polling the next frame. The input
/// byte budget limits JSON payload bytes, not total memory: vector spare
/// capacity, allocation growth, the current frame, transport buffering,
/// decoding, and the resulting `T` can require additional memory.
/// Deserialization remains synchronous, so these budgets do not bound CPU
/// time in a custom `Deserialize` implementation.
///
/// This extractor is available with the `json` feature.
///
/// # Type Parameters
///
/// - `T`: deserialized value type expected by the request handler.
///
/// # Examples
///
/// ```
/// use axum::body::Body;
/// use axum::extract::FromRequest;
/// use axum::http::Request;
/// use axum::http::StatusCode;
/// use axum::response::IntoResponse;
/// use qubit_web::{BoundedJson, JsonLimits};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let mut request = Request::post("/")
///         .header("content-type", "application/json")
///         .body(Body::from("42"))?;
///     request.extensions_mut().insert(JsonLimits::default());
///     let BoundedJson(value) = BoundedJson::<u64>::from_request(request, &())
///         .await
///         .expect("valid JSON within limits");
///     assert_eq!(value, 42);
///
///     let mut request = Request::post("/")
///         .header("content-type", "application/json")
///         .body(Body::from("42"))?;
///     request
///         .extensions_mut()
///         .insert(JsonLimits::default().with_max_input_bytes(1)?);
///     let rejection = BoundedJson::<u64>::from_request(request, &())
///         .await
///         .expect_err("two input bytes exceed the one-byte limit");
///     assert_eq!(rejection.into_response().status(), StatusCode::PAYLOAD_TOO_LARGE);
///     Ok(())
/// }
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
    /// Rejection value returned when bounded JSON decoding fails.
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
        let mut body = Limited::new(body, limits.max_input_bytes.get());
        let mut buffer = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|error| {
                if error.is::<http_body_util::LengthLimitError>() {
                    JsonRejection::BudgetExceeded
                } else {
                    JsonRejection::BodyReadFailed
                }
            })?;
            if let Ok(data) = frame.into_data() {
                buffer.extend_from_slice(&data);
            }
        }
        let bytes = Bytes::from(buffer);
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
