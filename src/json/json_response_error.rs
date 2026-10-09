// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Privacy-safe errors from JSON response encoding.

use std::fmt;

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
    pub(super) budget_exceeded: bool,
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
