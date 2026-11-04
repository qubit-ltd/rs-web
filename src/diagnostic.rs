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
pub(crate) mod internal {
    mod body_observation;
    mod body_observation_state;
    mod body_termination;
    mod connection_id;
    mod diagnostic_body;

    pub(crate) use body_observation::BodyObservation;
    pub(crate) use connection_id::ConnectionId;
    pub(crate) use diagnostic_body::DiagnosticBody;
}

pub use diagnostic_layer::DiagnosticFuture;
pub use diagnostic_layer::DiagnosticLayer;
pub use diagnostic_layer::DiagnosticMiddleware;
pub(crate) use internal::ConnectionId;
pub use request_diagnostic::RequestDiagnostic;
