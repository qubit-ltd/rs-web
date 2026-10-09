// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Opaque connection identifier attached to a request while serving it.
#[derive(Clone, Copy)]
pub(crate) struct ConnectionId(
    /// Numeric value propagated through request extensions.
    pub(crate) u64,
);
