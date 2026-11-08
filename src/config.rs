// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Adapts explicit [`qubit_config::Config`] values to server options.

mod config_options_error;
mod configured_web;
mod stream_admission_limits_from_config;

pub use config_options_error::ConfigOptionsError;
pub use configured_web::ConfiguredWeb;
pub use stream_admission_limits_from_config::stream_admission_limits_from_config;
