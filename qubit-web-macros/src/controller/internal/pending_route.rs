// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use proc_macro2::Span;
use syn::LitStr;
use syn::Meta;
use syn::Token;
use syn::punctuated::Punctuated;

/// Parsed route attribute awaiting path and option validation.
pub(super) struct PendingRoute {
    /// Literal child path declared by the route attribute.
    pub(super) path: LitStr,
    /// Parsed route options following the path.
    pub(super) options: Punctuated<Meta, Token![,]>,
    /// HTTP method supplied by a verb marker, if present.
    pub(super) marker_verb: Option<String>,
    /// Source location of the route attribute.
    pub(super) span: Span,
}
