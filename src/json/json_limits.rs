// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Finite input and output budgets for JSON operations.

use std::num::NonZeroUsize;

use qubit_budget::json::JsonDecodeLimits;
use qubit_budget::json::JsonEncodeLimits;

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
    pub(super) max_input_bytes: NonZeroUsize,
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
    pub(super) fn decode_limits(self) -> JsonDecodeLimits {
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
    pub(super) fn encode_limits(self) -> JsonEncodeLimits {
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
