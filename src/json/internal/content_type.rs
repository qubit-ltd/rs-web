// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! JSON media type validation.

use axum::http::header;

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
pub(in crate::json) fn is_json_content_type(value: Option<&header::HeaderValue>) -> bool {
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
