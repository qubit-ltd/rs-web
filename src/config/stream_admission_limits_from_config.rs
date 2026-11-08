// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;

use qubit_config::Config;

use super::ConfigOptionsError;
use crate::StreamAdmissionLimits;

/// Parses optional stream admission limits from explicit configuration values.
///
/// This independent adapter requires no listener address and does not install
/// the returned policy on a route. Omitted keys retain
/// [`StreamAdmissionLimits::default`]: 1 MiB body size, 256 concurrent
/// admissions, and a 10-second deadline.
///
/// # Parameters
///
/// - `config`: configuration containing any of the `stream_admission.*` keys.
///
/// # Returns
///
/// Validated limits to install explicitly through
/// [`crate::StreamAdmissionLayer`] or the controller route API.
///
/// # Errors
///
/// Returns [`ConfigOptionsError`] with the full key name if a value has the
/// wrong type, does not fit this platform, or fails limit validation. Error
/// formatting does not include the rejected value.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "config")]
/// # {
/// use qubit_config::Config;
/// use qubit_web::config::stream_admission_limits_from_config;
///
/// let mut config = Config::new();
/// config.set("stream_admission.max_body_bytes", 4096_u64)?;
/// let limits = stream_admission_limits_from_config(&config)?;
/// assert_eq!(limits.max_body_bytes(), 4096);
/// assert_eq!(limits.max_concurrent_admissions(), 256);
/// # }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn stream_admission_limits_from_config(config: &Config) -> Result<StreamAdmissionLimits, ConfigOptionsError> {
    let mut limits = StreamAdmissionLimits::default();
    if let Some(bytes) = config
        .get_optional::<u64>("stream_admission.max_body_bytes")
        .map_err(|_| ConfigOptionsError::new("stream_admission.max_body_bytes"))?
    {
        limits = limits
            .with_max_body_bytes(
                usize::try_from(bytes).map_err(|_| ConfigOptionsError::new("stream_admission.max_body_bytes"))?,
            )
            .map_err(|_| ConfigOptionsError::new("stream_admission.max_body_bytes"))?;
    }
    if let Some(admissions) = config
        .get_optional::<u64>("stream_admission.max_concurrent_admissions")
        .map_err(|_| ConfigOptionsError::new("stream_admission.max_concurrent_admissions"))?
    {
        limits = limits
            .with_max_concurrent_admissions(
                usize::try_from(admissions)
                    .map_err(|_| ConfigOptionsError::new("stream_admission.max_concurrent_admissions"))?,
            )
            .map_err(|_| ConfigOptionsError::new("stream_admission.max_concurrent_admissions"))?;
    }
    if let Some(timeout_ms) = config
        .get_optional::<u64>("stream_admission.admission_timeout_ms")
        .map_err(|_| ConfigOptionsError::new("stream_admission.admission_timeout_ms"))?
    {
        limits = limits
            .with_admission_timeout(Duration::from_millis(timeout_ms))
            .map_err(|_| ConfigOptionsError::new("stream_admission.admission_timeout_ms"))?;
    }
    Ok(limits)
}
