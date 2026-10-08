// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;

/// A startup or runtime error that does not expose request or credential data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebServerError {
    /// Server options are invalid.
    InvalidConfig,
    /// The requested socket could not be bound.
    BindFailed,
    /// The HTTP serving task failed.
    ServeFailed,
    /// TLS material or configuration is invalid.
    TlsInvalid,
}

impl WebServerError {
    /// Returns the stable machine-readable error code.
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::BindFailed => "bind_failed",
            Self::ServeFailed => "serve_failed",
            Self::TlsInvalid => "tls_invalid",
        }
    }
}

impl fmt::Display for WebServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "invalid server configuration",
            Self::BindFailed => "failed to bind server socket",
            Self::ServeFailed => "HTTP server failed",
            Self::TlsInvalid => "invalid TLS configuration",
        })
    }
}

impl std::error::Error for WebServerError {}
