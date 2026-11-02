// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Invalid WebSocket policy configuration.
///
/// # Examples
///
/// ```
/// use qubit_web::ws::WsPolicyError;
///
/// fn validate_policy() -> Result<(), Box<dyn std::error::Error>> {
///     qubit_web::ws::WsUpgradePolicy::new()
///         .max_connections(0)
///         .validate()?;
///     Ok(())
/// }
///
/// let error = validate_policy().unwrap_err();
/// assert!(error.downcast_ref::<WsPolicyError>().is_some());
/// assert_eq!(error.to_string(), "WebSocket policy limits must be positive");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum WsPolicyError {
    /// A configured connection, frame, message, queue, or timeout limit was
    /// zero.
    ZeroLimit,
    /// The configured connection limit exceeds Tokio Semaphore's maximum.
    ConnectionLimitTooLarge,
    /// The configured inbound message count exceeds Tokio mpsc's maximum.
    InboundQueueLimitTooLarge,
}

impl std::fmt::Display for WsPolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::ZeroLimit => "WebSocket policy limits must be positive",
            Self::ConnectionLimitTooLarge => "WebSocket connection limit exceeds the supported maximum",
            Self::InboundQueueLimitTooLarge => "WebSocket inbound queue limit exceeds the supported maximum",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for WsPolicyError {}
