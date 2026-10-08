// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Adapts explicit [`qubit_config::Config`] values to server options.

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use qubit_config::Config;

use crate::ServerOptions;
use crate::WebServerError;
use crate::limit::HttpLimits;

/// A configuration conversion error that identifies its field without
/// retaining or displaying the rejected value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigOptionsError {
    field: &'static str,
}

impl ConfigOptionsError {
    const fn new(field: &'static str) -> Self {
        Self { field }
    }

    /// Returns the configuration field that could not be read or validated.
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for ConfigOptionsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid server configuration field `{}`", self.field)
    }
}

impl std::error::Error for ConfigOptionsError {}

impl ServerOptions {
    /// Builds options from explicit configuration values.
    ///
    /// The required `address` value is a socket address. The optional
    /// `shutdown_timeout_ms` value defaults to 30 seconds. This function does
    /// not discover or load a global configuration source.
    ///
    /// # Errors
    ///
    /// Returns a field-specific error when a value is absent, has the wrong
    /// type, cannot be parsed, or fails server option validation. Rejected
    /// values are never included in the error text.
    pub fn from_config(config: &Config) -> Result<Self, ConfigOptionsError> {
        let address = config
            .get_strict::<String>("address")
            .map_err(|_| ConfigOptionsError::new("address"))?
            .parse::<SocketAddr>()
            .map_err(|_| ConfigOptionsError::new("address"))?;

        let shutdown_timeout_ms = config
            .get_optional::<u64>("shutdown_timeout_ms")
            .map_err(|_| ConfigOptionsError::new("shutdown_timeout_ms"))?
            .unwrap_or(30_000);

        let max_body_bytes = config
            .get_optional::<u64>("http.max_body_bytes")
            .map_err(|_| ConfigOptionsError::new("http.max_body_bytes"))?;
        let max_concurrent_requests = config
            .get_optional::<u64>("http.max_concurrent_requests")
            .map_err(|_| ConfigOptionsError::new("http.max_concurrent_requests"))?;
        let request_timeout_ms = config
            .get_optional::<u64>("http.request_timeout_ms")
            .map_err(|_| ConfigOptionsError::new("http.request_timeout_ms"))?;

        let mut limits = HttpLimits::default();
        if let Some(bytes) = max_body_bytes {
            limits = limits
                .with_max_body_bytes(
                    usize::try_from(bytes).map_err(|_| ConfigOptionsError::new("http.max_body_bytes"))?,
                )
                .map_err(|_| ConfigOptionsError::new("http.max_body_bytes"))?;
        }
        if let Some(requests) = max_concurrent_requests {
            limits = limits
                .with_max_concurrent_requests(
                    usize::try_from(requests).map_err(|_| ConfigOptionsError::new("http.max_concurrent_requests"))?,
                )
                .map_err(|_| ConfigOptionsError::new("http.max_concurrent_requests"))?;
        }
        if let Some(timeout_ms) = request_timeout_ms {
            limits = limits
                .with_request_timeout(Duration::from_millis(timeout_ms))
                .map_err(|_| ConfigOptionsError::new("http.request_timeout_ms"))?;
        }

        let options = Self::new(address)
            .with_shutdown_timeout(Duration::from_millis(shutdown_timeout_ms))
            .with_http_limits(limits);
        options.validate().map_err(|error| match error {
            WebServerError::InvalidConfig => ConfigOptionsError::new("shutdown_timeout_ms"),
            _ => ConfigOptionsError::new("address"),
        })?;
        Ok(options)
    }
}
