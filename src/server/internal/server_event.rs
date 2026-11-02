// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Events selected by the server accept loop.

/// Event selected while the server accepts transports and reaps tasks.
pub(super) enum ServerEvent<A> {
    Shutdown,
    Accept(A),
    Joined,
}
