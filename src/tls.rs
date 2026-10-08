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
#[derive(Clone)]
pub struct TlsConfig {
    inner: RustlsConfig,
}

impl TlsConfig {
    /// Loads a certificate chain and private key from PEM files.
    ///
    /// Both files are parsed before the caller binds or starts the listener.
    /// The key contents and paths are never included in the returned error.
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
    pub fn from_rustls(config: RustlsConfig) -> Self {
        Self { inner: config }
    }

    /// Returns a clone of the validated rustls config for the HTTPS transport.
    pub(crate) fn rustls_config(&self) -> RustlsConfig {
        self.inner.clone()
    }
}
