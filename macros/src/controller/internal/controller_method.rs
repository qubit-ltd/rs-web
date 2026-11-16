// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use proc_macro2::TokenStream;

use super::Route;

/// A validated mapped method passed from validation to generation.
pub(super) struct ControllerMethod {
    /// Index of the source method in its implementation block.
    pub(super) item_index: usize,
    /// Routes associated with the validated method.
    pub(super) routes: Vec<Route>,
    /// Effective cfg predicates controlling generated items.
    pub(super) cfg: Vec<TokenStream>,
}
