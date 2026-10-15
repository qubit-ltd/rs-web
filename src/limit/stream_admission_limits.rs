// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Finite budgets for admitting long-lived HTTP requests.

use std::num::NonZeroUsize;
use std::time::Duration;

/// Finite body, concurrency, and admission-time limits for SSE and WebSocket
/// request entry points.
///
/// The defaults are 1 MiB per body, 256 simultaneous admissions, and 10
/// seconds. Limits apply per shared [`super::StreamAdmissionState`] domain.
/// Bodies are buffered before the handler starts; active response bodies and
/// WebSocket sessions are outside the admission deadline.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamAdmissionLimits {
    max_body_bytes: NonZeroUsize,
    max_concurrent_admissions: NonZeroUsize,
    admission_timeout: Duration,
}

impl Default for StreamAdmissionLimits {
    /// Creates finite defaults for long-lived request admission.
    fn default() -> Self {
        Self {
            max_body_bytes: NonZeroUsize::new(1024 * 1024).expect("positive constant"),
            max_concurrent_admissions: NonZeroUsize::new(256).expect("positive constant"),
            admission_timeout: Duration::from_secs(10),
        }
    }
}

impl StreamAdmissionLimits {
    /// Returns the maximum buffered request body size in bytes.
    #[must_use]
    pub const fn max_body_bytes(self) -> usize {
        self.max_body_bytes.get()
    }

    /// Returns the number of concurrent admissions in this state domain.
    #[must_use]
    pub const fn max_concurrent_admissions(self) -> usize {
        self.max_concurrent_admissions.get()
    }

    /// Returns the deadline for buffering the request and creating response
    /// headers.
    #[must_use]
    pub const fn admission_timeout(self) -> Duration {
        self.admission_timeout
    }

    /// Sets the maximum request body size.
    ///
    /// # Parameters
    ///
    /// - `bytes`: positive per-request body budget.
    ///
    /// # Returns
    ///
    /// Updated admission limits.
    ///
    /// # Errors
    ///
    /// Returns `WebServerError::InvalidConfig` when `bytes` is zero.
    pub fn with_max_body_bytes(mut self, bytes: usize) -> Result<Self, crate::WebServerError> {
        self.max_body_bytes = NonZeroUsize::new(bytes).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum simultaneous admissions. The value must be within
    /// Tokio's semaphore permit range.
    ///
    /// # Parameters
    ///
    /// - `admissions`: positive capacity shared by this state domain.
    ///
    /// # Returns
    ///
    /// Updated admission limits.
    ///
    /// # Errors
    ///
    /// Returns `WebServerError::InvalidConfig` when the value is zero or
    /// exceeds `Semaphore::MAX_PERMITS`.
    pub fn with_max_concurrent_admissions(mut self, admissions: usize) -> Result<Self, crate::WebServerError> {
        if !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&admissions) {
            return Err(crate::WebServerError::InvalidConfig);
        }
        self.max_concurrent_admissions = NonZeroUsize::new(admissions).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the positive request admission deadline.
    ///
    /// # Parameters
    ///
    /// - `timeout`: maximum time for buffering the request and creating
    ///   response headers.
    ///
    /// # Returns
    ///
    /// Updated admission limits.
    ///
    /// # Errors
    ///
    /// Returns `WebServerError::InvalidConfig` when `timeout` is zero or
    /// cannot be represented as an absolute monotonic deadline.
    pub fn with_admission_timeout(mut self, timeout: Duration) -> Result<Self, crate::WebServerError> {
        if timeout.is_zero() || tokio::time::Instant::now().checked_add(timeout).is_none() {
            return Err(crate::WebServerError::InvalidConfig);
        }
        self.admission_timeout = timeout;
        Ok(self)
    }
}
