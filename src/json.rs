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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonLimits {
    max_input_bytes: NonZeroUsize,
    max_output_bytes: NonZeroUsize,
    max_depth: NonZeroUsize,
    max_nodes: NonZeroUsize,
    max_sequence_items: NonZeroUsize,
    max_map_entries: NonZeroUsize,
    max_key_bytes: NonZeroUsize,
    max_string_bytes: NonZeroUsize,
    max_number_bytes: NonZeroUsize,
}

impl Default for JsonLimits {
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

const fn nonzero(value: usize) -> NonZeroUsize {
    match NonZeroUsize::new(value) {
        Some(value) => value,
        None => panic!("JSON limits must be positive"),
    }
}

impl JsonLimits {
    /// Sets the maximum number of raw request bytes admitted by the extractor.
    pub fn with_max_input_bytes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_input_bytes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum number of response bytes admitted by the encoder.
    pub fn with_max_output_bytes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_output_bytes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum inclusive JSON nesting depth.
    pub fn with_max_depth(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_depth = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum inclusive number of JSON value nodes.
    pub fn with_max_nodes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_nodes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum number of items in one JSON array.
    pub fn with_max_sequence_items(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_sequence_items = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum number of entries in one JSON object.
    pub fn with_max_map_entries(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_map_entries = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum UTF-8 byte length of one object key.
    pub fn with_max_key_bytes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_key_bytes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum UTF-8 byte length of one string value.
    pub fn with_max_string_bytes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_string_bytes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum lexical byte length of one JSON number.
    pub fn with_max_number_bytes(mut self, maximum: usize) -> Result<Self, crate::WebServerError> {
        self.max_number_bytes = NonZeroUsize::new(maximum).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

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

fn is_json_content_type(value: Option<&header::HeaderValue>) -> bool {
    let Some(value) = value.and_then(|value| value.to_str().ok()) else {
        return false;
    };
    let mut segments = value.split(';');
    let media_type = segments.next().unwrap_or_default().trim();
    let valid_media_type = media_type.eq_ignore_ascii_case("application/json")
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
            });
    if !valid_media_type {
        return false;
    }

    let mut has_charset = false;
    for parameter in segments {
        let parameter = parameter.trim();
        let Some((name, raw_value)) = parameter.split_once('=') else {
            return false;
        };
        let name = name.trim();
        let raw_value = raw_value.trim();
        if !name.eq_ignore_ascii_case("charset") || has_charset || raw_value.contains('=') {
            return false;
        }
        let charset = if raw_value.starts_with('"') || raw_value.ends_with('"') {
            raw_value.strip_prefix('"').and_then(|value| value.strip_suffix('"'))
        } else {
            Some(raw_value)
        };
        if !charset.is_some_and(|value| value.eq_ignore_ascii_case("utf-8")) {
            return false;
        }
        has_charset = true;
    }
    true
}

/// Stable rejection returned by [`BoundedJson`] extraction.
#[doc(hidden)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonResponseError {
    budget_exceeded: bool,
}

impl JsonResponseError {
    /// Returns the stable machine-readable error code.
    pub const fn code(self) -> &'static str {
        if self.budget_exceeded {
            "json_output_budget_exceeded"
        } else {
            "json_encode_failed"
        }
    }
}

impl fmt::Display for JsonResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for JsonResponseError {}

/// Encodes a complete JSON response before creating its HTTP response body.
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

fn problem_response(status: StatusCode, code: &str, title: &str) -> Response {
    let body = format!(
        "{{\"type\":\"about:blank\",\"title\":\"{title}\",\"status\":{},\"code\":\"{code}\"}}",
        status.as_u16()
    );
    (status, [(header::CONTENT_TYPE, "application/problem+json")], body).into_response()
}
