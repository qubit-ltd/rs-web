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
    /// Whether each connection should read the shared config before handshake.
    reloadable: bool,
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
        Ok(Self {
            inner,
            reloadable: false,
        })
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
    ///
    /// This wrapper retains the supplied configuration handle. The HTTPS
    /// server takes a snapshot of its underlying rustls configuration when
    /// [`crate::WebServer::bind_https`] constructs the server. Reloads made
    /// before that construction completes can be observed by the binding;
    /// reloads made afterward do not update an already bound server. A reload
    /// concurrent with binding has no guaranteed ordering, so applications
    /// must coordinate those operations. To apply a new configuration,
    /// coordinate shutting down the old server and binding a new one; rebinding
    /// the same address is not guaranteed to be seamless.
    #[inline]
    pub fn from_rustls(config: RustlsConfig) -> Self {
        Self {
            inner: config,
            reloadable: false,
        }
    }

    /// Creates TLS settings that use the latest supplied Rustls configuration
    /// for each new connection handshake.
    ///
    /// Keep a clone of `config` to call its reload methods after binding. Each
    /// handshake takes one configuration snapshot before it begins; existing
    /// connections retain their negotiated TLS state. Failed PEM reloads
    /// leave the last successfully loaded configuration active.
    ///
    /// # Parameters
    ///
    /// * `config` - Shared rustls settings that the application can reload.
    ///
    /// # Returns
    ///
    /// TLS settings that use the current config snapshot for each new
    /// handshake.
    #[inline]
    pub fn from_reloadable_rustls(config: RustlsConfig) -> Self {
        Self {
            inner: config,
            reloadable: true,
        }
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

    /// Reports whether the HTTPS listener should read the config before each
    /// handshake.
    ///
    /// # Returns
    ///
    /// `true` when this setting uses the explicitly reloadable source.
    pub(crate) fn is_reloadable(&self) -> bool {
        self.reloadable
    }
}

#[cfg(test)]
mod tests {
    use super::TlsConfig;

    #[tokio::test]
    async fn test_wraps_and_clones_an_existing_rustls_config() {
        let certificate = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls/cert.pem");
        let key = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls/key.pem");
        let rustls = axum_server::tls_rustls::RustlsConfig::from_pem_file(certificate, key)
            .await
            .expect("test TLS fixture should load");

        let tls = TlsConfig::from_rustls(rustls);
        let cloned = tls.rustls_config();
        let server = crate::WebServer::bind_https(
            crate::ServerOptions::new("127.0.0.1:0".parse().unwrap()),
            TlsConfig::from_rustls(cloned),
        )
        .await
        .expect("cloned TLS config should bind an HTTPS listener");
        assert!(server.local_addr().ip().is_loopback());
    }
}
