// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use proc_macro2::TokenStream;

use super::pending_route::PendingRoute;

/// Parsed method declaration awaiting semantic checks.
pub(super) struct PendingMethod {
    /// Index of the method in its implementation block.
    pub(super) item_index: usize,
    /// Effective cfg predicates or the parsing error that prevented their
    /// extraction.
    pub(super) cfg: syn::Result<Vec<TokenStream>>,
    /// Parsed route declarations or errors from the method's attributes.
    pub(super) routes: Vec<syn::Result<PendingRoute>>,
}
