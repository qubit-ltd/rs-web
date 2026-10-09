// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Allowlisted HTTP diagnostics that never include request content or
//! credentials.

mod diagnostic_layer;
mod request_diagnostic;
mod internal {
    mod connection_id;

    pub(crate) use connection_id::ConnectionId;
}

pub use diagnostic_layer::DiagnosticFuture;
pub use diagnostic_layer::DiagnosticLayer;
pub use diagnostic_layer::DiagnosticMiddleware;
pub(crate) use internal::ConnectionId;
pub use request_diagnostic::RequestDiagnostic;
