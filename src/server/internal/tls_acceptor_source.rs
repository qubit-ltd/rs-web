// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use axum_server::tls_rustls::RustlsConfig;
use tokio_rustls::TlsAcceptor;

/// TLS configuration source retained by a bound HTTPS listener.
#[derive(Clone)]
pub(super) enum TlsAcceptorSource {
    /// Fixed acceptor captured while the HTTPS listener is bound.
    Snapshot(
        /// Acceptor built from the configuration captured at bind time.
        TlsAcceptor,
    ),
    /// Shared configuration read once before each new connection handshake.
    Reloadable(
        /// Shared configuration handle queried before a new handshake.
        RustlsConfig,
    ),
}

impl TlsAcceptorSource {
    /// Takes one acceptor snapshot for a connection's upcoming handshake.
    ///
    /// # Returns
    ///
    /// A clone of the fixed acceptor or a new acceptor from the current shared
    /// configuration snapshot.
    pub(super) fn for_handshake(&self) -> TlsAcceptor {
        match self {
            Self::Snapshot(acceptor) => acceptor.clone(),
            Self::Reloadable(config) => TlsAcceptor::from(config.get_inner()),
        }
    }
}
