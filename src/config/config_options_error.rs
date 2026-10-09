// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;

/// A configuration conversion error that identifies its field without
/// retaining or displaying the rejected value.
///
/// # Examples
///
/// ```
/// use qubit_config::Config;
/// use qubit_web::ConfiguredWeb;
///
/// let error = ConfiguredWeb::from_config(&Config::new()).unwrap_err();
/// assert_eq!(error.field(), "address");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigOptionsError {
    /// Name of the setting that failed to parse or validate.
    field: &'static str,
}

impl ConfigOptionsError {
    /// Creates an error associated with a configuration key.
    ///
    /// # Parameters
    ///
    /// - `field`: the key whose value could not be read or validated.
    pub(super) const fn new(field: &'static str) -> Self {
        Self { field }
    }

    /// Returns the configuration field that could not be read or validated.
    ///
    /// # Returns
    ///
    /// The key name only; the rejected configuration value is not retained.
    #[must_use]
    #[inline]
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for ConfigOptionsError {
    /// Formats a value-safe message naming the invalid configuration key.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid server configuration field `{}`", self.field)
    }
}

impl std::error::Error for ConfigOptionsError {}
