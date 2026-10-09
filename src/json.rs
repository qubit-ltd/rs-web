// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Optional bounded JSON extractor and response encoder.

mod bounded_json;
mod internal;
mod json_limits;
mod json_rejection;
mod json_response;
mod json_response_error;

pub use bounded_json::BoundedJson;
pub use json_limits::JsonLimits;
pub use json_rejection::JsonRejection;
pub use json_response::json_response;
pub use json_response_error::JsonResponseError;
