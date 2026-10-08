// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Optional rustls transport configuration.

use std::path::Path;

use axum_server::tls_rustls::RustlsConfig;

use crate::WebServerError;

/// Validated rustls server credentials for an HTTPS listener.
///
/// The wrapped configuration is intentionally not exposed through `Debug`.
/// Errors from PEM loading are reduced to [`WebServerError::TlsInvalid`], so
/// key bytes and filesystem paths are not included in diagnostics.
///
/// # Examples
///
/// Applications can load their own certificate and key files before binding
/// the HTTPS listener:
///
/// ```
/// use std::path::Path;
///
/// use qubit_web::TlsConfig;
/// use qubit_web::WebServerError;
///
/// async fn load_tls(
///     certificate_chain: &Path,
///     private_key: &Path,
/// ) -> Result<TlsConfig, WebServerError> {
///     TlsConfig::from_pem_files(certificate_chain, private_key).await
/// }
///
/// let _loader = load_tls;
/// ```
#[derive(Clone)]
#[must_use]
pub struct TlsConfig {
    /// Parsed rustls settings shared with HTTPS server instances.
    inner: RustlsConfig,
}

impl TlsConfig {
    /// Loads a certificate chain and private key from PEM files.
    ///
    /// Both files are parsed before the caller binds or starts the listener.
    /// The key contents and paths are never included in the returned error.
    ///
    /// # Parameters
    ///
    /// * `certificate_chain` - PEM file containing the server certificate
    ///   chain.
    /// * `private_key` - PEM file containing the matching private key.
    ///
    /// # Returns
    ///
    /// A validated TLS configuration whose debug representation omits secrets.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::TlsInvalid`] when either PEM file cannot be
    /// loaded.
    pub async fn from_pem_files(
        certificate_chain: impl AsRef<Path>,
        private_key: impl AsRef<Path>,
    ) -> Result<Self, WebServerError> {
        let inner = RustlsConfig::from_pem_file(certificate_chain, private_key)
            .await
            .map_err(|_| WebServerError::TlsInvalid)?;
        Ok(Self { inner })
    }

    /// Creates TLS settings from an already constructed axum-server rustls
    /// config.
    ///
    /// # Parameters
    ///
    /// * `config` - Prebuilt rustls settings supplied by the application.
    ///
    /// # Returns
    ///
    /// A wrapper that can be passed to the HTTPS server constructor.
    #[inline]
    pub fn from_rustls(config: RustlsConfig) -> Self {
        Self { inner: config }
    }

    /// Returns a clone of the validated rustls config for the HTTPS transport.
    ///
    /// # Returns
    ///
    /// A clone that can be used to construct an HTTPS acceptor.
    #[must_use]
    #[inline]
    pub(crate) fn rustls_config(&self) -> RustlsConfig {
        self.inner.clone()
    }
}
