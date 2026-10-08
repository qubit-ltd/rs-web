// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use proc_macro2::TokenStream;

/// Preserves a verb-specific attribute for the enclosing `rest_controller`
/// macro.
///
/// The parent macro reads `_method` and `_args` from the retained marker while
/// expanding the enclosing impl; returning `input` prevents this helper macro
/// from removing the annotated method.
///
/// # Parameters
///
/// - `_method`: HTTP verb associated with the marker attribute.
/// - `_args`: marker path and options, interpreted by the parent macro.
/// - `input`: annotated method tokens to pass through unchanged.
#[must_use]
#[inline]
pub fn expand_mapping(_method: &str, _args: TokenStream, input: TokenStream) -> TokenStream {
    input
}

/// Preserves a generic route marker for the enclosing `rest_controller` macro.
///
/// The parent macro parses the route arguments while it expands the enclosing
/// impl; returning `input` keeps the annotated method available for that pass.
///
/// # Parameters
///
/// - `_args`: path, methods, and transport options parsed by the parent macro.
/// - `input`: annotated method tokens to pass through unchanged.
#[must_use]
#[inline]
pub fn expand_route(_args: TokenStream, input: TokenStream) -> TokenStream {
    input
}
