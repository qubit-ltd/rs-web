// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Optional bounded JSON extractor and response encoder.

use std::fmt;
use std::num::NonZeroUsize;

use axum::body::Body;
use axum::body::to_bytes;
use axum::extract::FromRequest;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use qubit_budget::json::JsonDecodeLimits;
use qubit_budget::json::JsonEncodeLimits;
use qubit_json::decode::JsonDecodeErrorKind;
use qubit_json::decode::JsonDecoder;
use qubit_json::encode::JsonEncodeErrorKind;
use qubit_json::encode::JsonEncoder;
use serde::Serialize;
use serde::de::DeserializeOwned;

const DEFAULT_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_DEPTH: usize = 64;
const DEFAULT_NODES: usize = 100_000;
const DEFAULT_ITEMS: usize = 10_000;
const DEFAULT_KEY_BYTES: usize = 16 * 1024;
const DEFAULT_STRING_BYTES: usize = 256 * 1024;
const DEFAULT_NUMBER_BYTES: usize = 128;

/// Finite byte and structural budgets for strict JSON input and output.
///
/// Defaults are 1 MiB each for input and output payloads, depth 64, 100,000
/// value nodes, 10,000 items per array, 10,000 entries per object, 16 KiB per
/// object key, 256 KiB per string, and 128 bytes per number. Input and output
/// use the same structural defaults independently.
///
/// # Examples
///
/// ```
/// use qubit_web::JsonLimits;
///
/// let limits = JsonLimits::default().with_max_depth(16).unwrap();
/// assert!(limits.with_max_input_bytes(4096).is_ok());
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonLimits {
    /// Maximum raw request body size accepted by the extractor.
    max_input_bytes: NonZeroUsize,
    /// Maximum encoded response size accepted by the encoder.
    max_output_bytes: NonZeroUsize,
    /// Maximum inclusive nesting depth for decoded and encoded values.
    max_depth: NonZeroUsize,
    /// Maximum inclusive count of JSON value nodes.
    max_nodes: NonZeroUsize,
    /// Maximum number of elements in one array.
    max_sequence_items: NonZeroUsize,
    /// Maximum number of members in one object.
    max_map_entries: NonZeroUsize,
    /// Maximum UTF-8 byte length of one object key.
    max_key_bytes: NonZeroUsize,
    /// Maximum UTF-8 byte length of one string value.
    max_string_bytes: NonZeroUsize,
    /// Maximum lexical byte length of one JSON number.
    max_number_bytes: NonZeroUsize,
}

impl Default for JsonLimits {
    /// Creates the documented finite input, output, and structural budgets.
    ///
    /// # Returns
    ///
    /// A limits value initialized with the defaults described on
    /// [`JsonLimits`].
    fn default() -> Self {
        Self {
            max_input_bytes: nonzero(DEFAULT_PAYLOAD_BYTES),
            max_output_bytes: nonzero(DEFAULT_PAYLOAD_BYTES),
            max_depth: nonzero(DEFAULT_DEPTH),
            max_nodes: nonzero(DEFAULT_NODES),
            max_sequence_items: nonzero(DEFAULT_ITEMS),
            max_map_entries: nonzero(DEFAULT_ITEMS),
            max_key_bytes: nonzero(DEFAULT_KEY_BYTES),
            max_string_bytes: nonzero(DEFAULT_STRING_BYTES),
            max_number_bytes: nonzero(DEFAULT_NUMBER_BYTES),
        }
    }
}

/// Converts a validated positive budget constant into its nonzero form.
///
/// # Panics
///
/// Panics if a compile-time default is zero; callers pass only positive
/// constants.
///
/// # Parameters
///
/// - `value`: positive default budget constant.
///
/// # Returns
///
/// The positive value represented as `NonZeroUsize`.
const fn nonzero(value: usize) -> NonZeroUsize {
    match NonZeroUsize::new(value) {
        Some(value) => value,
        None => panic!("JSON limits must be positive"),
    }
}

impl JsonLimits {
    /// Sets the maximum number of raw request bytes admitted by the extractor.
    ///
    /// # Parameters
    ///
    /// - `maximum`: positive byte budget for request bodies.
    ///
    /// # Returns
    ///
    /// Updated limits with the new input byte budget.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `maximum` is zero.
    pub fn with_max_input_bytes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_input_bytes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum number of response bytes admitted by the encoder.
    ///
    /// # Parameters
    ///
    /// - `maximum`: positive byte budget for encoded responses.
    ///
    /// # Returns
    ///
    /// Updated limits with the new output byte budget.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `maximum` is zero.
    pub fn with_max_output_bytes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_output_bytes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum inclusive JSON nesting depth.
    ///
    /// # Parameters
    ///
    /// - `maximum`: positive inclusive nesting depth.
    ///
    /// # Returns
    ///
    /// Updated limits with the new depth budget.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `maximum` is zero.
    pub fn with_max_depth(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_depth = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum inclusive number of JSON value nodes.
    ///
    /// # Parameters
    ///
    /// - `maximum`: positive inclusive node count.
    ///
    /// # Returns
    ///
    /// Updated limits with the new node budget.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `maximum` is zero.
    pub fn with_max_nodes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_nodes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum number of items in one JSON array.
    ///
    /// # Parameters
    ///
    /// - `maximum`: positive item count per array.
    ///
    /// # Returns
    ///
    /// Updated limits with the new array item budget.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `maximum` is zero.
    pub fn with_max_sequence_items(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_sequence_items = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum number of entries in one JSON object.
    ///
    /// # Parameters
    ///
    /// - `maximum`: positive entry count per object.
    ///
    /// # Returns
    ///
    /// Updated limits with the new object entry budget.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `maximum` is zero.
    pub fn with_max_map_entries(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_map_entries = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum UTF-8 byte length of one object key.
    ///
    /// # Parameters
    ///
    /// - `maximum`: positive UTF-8 byte count per key.
    ///
    /// # Returns
    ///
    /// Updated limits with the new key budget.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `maximum` is zero.
    pub fn with_max_key_bytes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_key_bytes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum UTF-8 byte length of one string value.
    ///
    /// # Parameters
    ///
    /// - `maximum`: positive UTF-8 byte count per string.
    ///
    /// # Returns
    ///
    /// Updated limits with the new string budget.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `maximum` is zero.
    pub fn with_max_string_bytes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_string_bytes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum lexical byte length of one JSON number.
    ///
    /// # Parameters
    ///
    /// - `maximum`: positive lexical byte count per number.
    ///
    /// # Returns
    ///
    /// Updated limits with the new number budget.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `maximum` is zero.
    pub fn with_max_number_bytes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_number_bytes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Translates input settings into the decoder's byte and structural limits.
    ///
    /// # Returns
    ///
    /// Decoder limits with input bytes and each configured structural bound.
    #[must_use]
    fn decode_limits(self) -> JsonDecodeLimits {
        JsonDecodeLimits::builder()
            .max_input_bytes(self.max_input_bytes.get())
            .max_depth(self.max_depth.get())
            .max_nodes(self.max_nodes.get())
            .max_sequence_items(self.max_sequence_items.get())
            .max_map_entries(self.max_map_entries.get())
            .max_key_bytes(self.max_key_bytes.get())
            .max_string_bytes(self.max_string_bytes.get())
            .max_number_bytes(self.max_number_bytes.get())
            .max_payload_bytes(self.max_input_bytes.get())
            .build()
    }

    /// Translates output settings into the encoder's byte and structural
    /// limits.
    ///
    /// # Returns
    ///
    /// Encoder limits with output bytes and each configured structural bound.
    #[must_use]
    fn encode_limits(self) -> JsonEncodeLimits {
        JsonEncodeLimits::builder()
            .max_output_bytes(self.max_output_bytes.get())
            .max_depth(self.max_depth.get())
            .max_nodes(self.max_nodes.get())
            .max_sequence_items(self.max_sequence_items.get())
            .max_map_entries(self.max_map_entries.get())
            .max_key_bytes(self.max_key_bytes.get())
            .max_string_bytes(self.max_string_bytes.get())
            .max_number_bytes(self.max_number_bytes.get())
            .max_payload_bytes(self.max_output_bytes.get())
            .build()
    }
}

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

/// Accepts JSON media types with no parameter or a single UTF-8 charset.
///
/// # Parameters
///
/// - `value`: request `Content-Type` header, if present and parseable.
///
/// # Returns
///
/// `true` for `application/json` or a `+json` media type with an optional
/// UTF-8 charset; malformed or unsupported parameters return `false`.
#[must_use]
fn is_json_content_type(value: Option<&header::HeaderValue>) -> bool {
    let Some(value) = value.and_then(|value| value.to_str().ok()) else {
        return false;
    };
    let mut segments = value.split(';');
    if !is_json_media_type(segments.next().unwrap_or_default().trim()) {
        return false;
    }

    let mut has_charset = false;
    for parameter in segments {
        if has_charset || !is_utf8_charset(parameter.trim()) {
            return false;
        }
        has_charset = true;
    }
    true
}

/// Checks whether a media type is JSON or has a structured `+json` suffix.
///
/// # Parameters
///
/// - `media_type`: trimmed media type portion of a `Content-Type` header.
///
/// # Returns
///
/// `true` for `application/json` and nonempty `application/*+json` subtypes.
#[must_use]
#[inline]
fn is_json_media_type(media_type: &str) -> bool {
    media_type.eq_ignore_ascii_case("application/json")
        || media_type
            .get(..12)
            .filter(|prefix| prefix.eq_ignore_ascii_case("application/"))
            .and_then(|_| media_type.get(12..))
            .is_some_and(|subtype| {
                subtype
                    .len()
                    .checked_sub(5)
                    .and_then(|name_end| subtype.get(name_end..).map(|suffix| (name_end, suffix)))
                    .is_some_and(|(name_end, suffix)| name_end > 0 && suffix.eq_ignore_ascii_case("+json"))
            })
}

/// Checks a single media type parameter against the supported UTF-8 charset.
///
/// # Parameters
///
/// - `parameter`: trimmed parameter text following the media type.
///
/// # Returns
///
/// `true` only for `charset=utf-8`, allowing matching single or double quotes.
#[must_use]
#[inline]
fn is_utf8_charset(parameter: &str) -> bool {
    let Some((name, raw_value)) = parameter.split_once('=') else {
        return false;
    };
    let name = name.trim();
    let raw_value = raw_value.trim();
    if !name.eq_ignore_ascii_case("charset") || raw_value.contains('=') {
        return false;
    }
    let charset = if raw_value.starts_with('"') || raw_value.ends_with('"') {
        raw_value.strip_prefix('"').and_then(|value| value.strip_suffix('"'))
    } else {
        Some(raw_value)
    };
    charset.is_some_and(|value| value.eq_ignore_ascii_case("utf-8"))
}

/// Stable rejection returned by [`BoundedJson`] extraction.
#[doc(hidden)]
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonRejection {
    /// The body was not valid JSON for the requested type.
    InvalidJson,
    /// The input exceeded a configured byte or structural budget.
    BudgetExceeded,
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
            Self::UnsupportedMediaType => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Unsupported Media Type",
            ),
        };
        problem_response(status, code, title)
    }
}

/// A privacy-safe failure while encoding a complete JSON response.
///
/// # Examples
///
/// ```
/// use qubit_web::{json_response, JsonLimits};
///
/// let limits = JsonLimits::default().with_max_output_bytes(1).unwrap();
/// let error = json_response(&"payload", &limits).unwrap_err();
/// assert_eq!(error.code(), "json_output_budget_exceeded");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonResponseError {
    /// Whether encoding exceeded the configured output budget.
    budget_exceeded: bool,
}

impl JsonResponseError {
    /// Returns the stable machine-readable error code.
    ///
    /// # Returns
    ///
    /// A safe code distinguishing output budget failures from other encoding
    /// failures.
    #[must_use]
    #[inline]
    pub const fn code(self) -> &'static str {
        if self.budget_exceeded {
            "json_output_budget_exceeded"
        } else {
            "json_encode_failed"
        }
    }
}

impl fmt::Display for JsonResponseError {
    /// Formats only the stable code, without including serialized input data.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for JsonResponseError {}

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
fn problem_response(status: StatusCode, code: &str, title: &str) -> Response {
    let body = format!(
        "{{\"type\":\"about:blank\",\"title\":\"{title}\",\"status\":{},\"code\":\"{code}\"}}",
        status.as_u16()
    );
    (status, [(header::CONTENT_TYPE, "application/problem+json")], body).into_response()
}
