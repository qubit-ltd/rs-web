// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Safe field and reason details for invalid [`super::ServerOptions`].

use std::fmt;

/// A safe description of the first invalid server option.
///
/// The error contains only a stable field identifier and reason code. It never
/// includes the configured value.
///
/// # Examples
///
/// ```
/// use qubit_web::ServerOptions;
/// use std::time::Duration;
///
/// let options = ServerOptions::new("127.0.0.1:8080".parse().unwrap())
///     .with_request_header_timeout(Duration::ZERO);
/// let error = options.validate_detailed().unwrap_err();
/// assert_eq!(error.field(), "request_header_timeout");
/// assert_eq!(error.code(), "zero");
/// ```
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ServerOptionsValidationError {
    /// Stable field name selected by `ServerOptions::validate_detailed`.
    field: &'static str,
    /// Stable reason code that explains why the field is invalid.
    code: &'static str,
}

impl ServerOptionsValidationError {
    /// Constructs an error from crate-controlled static field and reason names.
    ///
    /// # Parameters
    ///
    /// * `field` - Stable Rust option field name.
    /// * `code` - Stable reason code for that field.
    ///
    /// # Returns
    ///
    /// An error containing only the supplied static diagnostic identifiers.
    pub(crate) const fn new(field: &'static str, code: &'static str) -> Self {
        Self { field, code }
    }

    /// Returns the name of the invalid option field.
    ///
    /// # Returns
    ///
    /// A stable Rust field name that does not contain the configured value.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }

    /// Returns the stable reason code for the invalid value.
    ///
    /// # Returns
    ///
    /// A stable lowercase code such as `zero` or `deadline_unrepresentable`.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for ServerOptionsValidationError {
    /// Formats the stable field and reason without exposing an input value.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Destination formatter that receives the safe diagnostic.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid server option {} ({})", self.field, self.code)
    }
}

impl fmt::Debug for ServerOptionsValidationError {
    /// Formats the diagnostic identifiers without including configuration data.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Destination formatter that receives the diagnostic
    ///   fields.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerOptionsValidationError")
            .field("field", &self.field)
            .field("code", &self.code)
            .finish()
    }
}

impl std::error::Error for ServerOptionsValidationError {}
