// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private body wrappers used by request limit middleware.

mod body_state;
mod limited_request_body;
mod permit_body;
mod phase;

pub(super) use limited_request_body::limit_request_body;
pub(super) use permit_body::permit_body;
