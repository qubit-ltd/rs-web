// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;
use std::io;

/// A startup or runtime error that does not expose request or credential data.
///
/// Operating-system details for socket binding, local address lookup, and
/// accepting connections are available through [`std::error::Error::source`];
/// the default display and debug output omit those details.
///
/// # Examples
///
/// ```
/// use qubit_web::WebServerError;
///
/// let error = WebServerError::InvalidConfig;
/// assert_eq!(error.code(), "invalid_config");
/// ```
///
/// Socket errors retain their source while keeping default output generic:
///
/// ```
/// use std::io;
/// use std::error::Error as _;
/// use qubit_web::WebServerError;
///
/// let error = WebServerError::BindFailed {
///     source: io::Error::from(io::ErrorKind::AddrInUse),
/// };
/// assert_eq!(error.code(), "bind_failed");
/// assert!(error.to_string().contains("bind server socket"));
/// assert_eq!(error.source().unwrap().downcast_ref::<io::Error>().unwrap().kind(), io::ErrorKind::AddrInUse);
/// assert!(!format!("{error}").contains("AddrInUse"));
/// ```
#[must_use]
pub enum WebServerError {
    /// Server options are invalid.
    InvalidConfig,
    /// Binding the requested socket failed.
    BindFailed {
        /// Underlying operating-system bind failure.
        source: io::Error,
    },
    /// Querying the bound socket's local address failed.
    LocalAddressFailed {
        /// Underlying operating-system address lookup failure.
        source: io::Error,
    },
    /// Accepting an incoming connection failed.
    ServeFailed {
        /// Underlying listener accept failure.
        source: io::Error,
    },
    /// TLS material or configuration is invalid.
    TlsInvalid,
}

impl WebServerError {
    /// Returns the stable machine-readable error code.
    ///
    /// # Returns
    ///
    /// A lowercase code that is stable across display wording changes.
    #[must_use]
    #[inline]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::BindFailed { .. } => "bind_failed",
            Self::LocalAddressFailed { .. } => "local_address_failed",
            Self::ServeFailed { .. } => "serve_failed",
            Self::TlsInvalid => "tls_invalid",
        }
    }
}

impl fmt::Debug for WebServerError {
    /// Formats only the stable error category, omitting the underlying source.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "InvalidConfig",
            Self::BindFailed { .. } => "BindFailed",
            Self::LocalAddressFailed { .. } => "LocalAddressFailed",
            Self::ServeFailed { .. } => "ServeFailed",
            Self::TlsInvalid => "TlsInvalid",
        })
    }
}

impl fmt::Display for WebServerError {
    /// Formats a value-safe human-readable error message.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "invalid server configuration",
            Self::BindFailed { .. } => "failed to bind server socket",
            Self::LocalAddressFailed { .. } => "failed to query server socket address",
            Self::ServeFailed { .. } => "HTTP server failed while accepting a connection",
            Self::TlsInvalid => "invalid TLS configuration",
        })
    }
}

impl std::error::Error for WebServerError {
    /// Returns the original I/O error for socket operations that retain one.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BindFailed { source } | Self::LocalAddressFailed { source } | Self::ServeFailed { source } => {
                Some(source)
            }
            Self::InvalidConfig | Self::TlsInvalid => None,
        }
    }
}
