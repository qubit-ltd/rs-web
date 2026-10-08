// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! HTTP/HTTPS binding, request dispatch, and graceful shutdown.

mod web_server;

pub use web_server::ServerContext;
pub use web_server::SessionGuard;
pub use web_server::SessionRegistrationError;
pub use web_server::ShutdownReport;
pub use web_server::WebServer;
