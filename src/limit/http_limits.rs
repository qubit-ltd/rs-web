// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Finite per-branch budgets for ordinary HTTP requests.

use std::num::NonZeroUsize;
use std::time::Duration;

/// Finite limits applied to ordinary, short HTTP requests.
///
/// # Examples
///
/// ```
/// use qubit_web::HttpLimits;
/// use std::time::Duration;
///
/// let limits = HttpLimits::default()
///     .with_max_concurrent_requests(8).unwrap()
///     .with_request_timeout(Duration::from_secs(2)).unwrap();
/// assert_eq!(limits.max_concurrent_requests(), 8);
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpLimits {
    /// Maximum request body size admitted by the middleware.
    max_body_bytes: NonZeroUsize,
    /// Maximum simultaneous short requests protected by one branch.
    max_concurrent_requests: NonZeroUsize,
    /// Deadline for handler work and response body streaming.
    request_timeout: Duration,
}

impl Default for HttpLimits {
    /// Creates finite defaults for body size, concurrency, and request time.
    ///
    /// # Returns
    ///
    /// Limits of 1 MiB per body, 256 in-flight requests, and 30 seconds.
    fn default() -> Self {
        Self {
            max_body_bytes: NonZeroUsize::new(1024 * 1024).expect("positive constant"),
            max_concurrent_requests: NonZeroUsize::new(256).expect("positive constant"),
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl HttpLimits {
    /// Returns the maximum accepted request body size.
    ///
    /// # Returns
    ///
    /// The body budget in bytes.
    #[must_use]
    #[inline]
    pub const fn max_body_bytes(self) -> usize {
        self.max_body_bytes.get()
    }

    /// Returns the maximum number of in-flight short requests.
    ///
    /// # Returns
    ///
    /// The per-branch concurrent request capacity.
    #[must_use]
    #[inline]
    pub const fn max_concurrent_requests(self) -> usize {
        self.max_concurrent_requests.get()
    }

    /// Returns the ordinary request processing deadline.
    ///
    /// # Returns
    ///
    /// The configured request and response-stream duration.
    #[must_use]
    #[inline]
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    /// Sets the maximum number of request body bytes.
    ///
    /// # Parameters
    ///
    /// - `bytes`: positive byte budget for each request body.
    ///
    /// # Returns
    ///
    /// Updated HTTP limits.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when the budget is zero.
    pub fn with_max_body_bytes(mut self, bytes: usize) -> Result<Self, crate::WebServerError> {
        self.max_body_bytes = NonZeroUsize::new(bytes).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum number of concurrent short requests.
    ///
    /// # Parameters
    ///
    /// - `requests`: positive count of in-flight requests.
    ///
    /// # Returns
    ///
    /// Updated HTTP limits.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when the capacity is zero.
    pub fn with_max_concurrent_requests(mut self, requests: usize) -> Result<Self, crate::WebServerError> {
        self.max_concurrent_requests = NonZeroUsize::new(requests).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum processing time for an ordinary short request.
    ///
    /// # Parameters
    ///
    /// - `timeout`: positive duration covering handler and response streaming.
    ///
    /// # Returns
    ///
    /// Updated HTTP limits.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` for a zero duration.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, crate::WebServerError> {
        if timeout.is_zero() {
            return Err(crate::WebServerError::InvalidConfig);
        }
        self.request_timeout = timeout;
        Ok(self)
    }
}
