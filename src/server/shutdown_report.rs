// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;

/// Summary of a completed graceful shutdown.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use qubit_web::ShutdownReport;
///
/// let report = ShutdownReport {
///     graceful: true,
///     forced_connections: None,
///     unfinished_managed_sessions: 0,
///     elapsed: Duration::from_millis(25),
/// };
/// assert!(report.graceful);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use]
pub struct ShutdownReport {
    /// Whether all HTTP connections and managed sessions actually finished
    /// at or before the shared shutdown deadline. A task that is only
    /// observed as finished after scheduler delay is judged by its recorded
    /// completion time, not by when the report task resumes.
    pub graceful: bool,
    /// Number of forcibly closed connections, when the transport can prove it.
    /// WebServer manages connection tasks driven by Hyper/hyper-util, but this
    /// implementation does not provide a provable count of forcibly closed
    /// connections, so this field is `None`.
    pub forced_connections: Option<usize>,
    /// Number of registered SSE and WebSocket sessions still active at the
    /// shutdown deadline. This count does not include ordinary HTTP requests
    /// or arbitrary application background tasks.
    pub unfinished_managed_sessions: usize,
    /// Time from shutdown notification until the server task terminated.
    pub elapsed: Duration,
}
