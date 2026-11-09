// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use proc_macro2::TokenStream;
use syn::Error;

/// Rejects a verb-specific route attribute used outside `rest_controller`.
///
/// These attributes are consumed by the enclosing `rest_controller` macro.
/// Using one independently is a compile-time error and does not register a
/// route.
///
/// # Parameters
///
/// - `_method`: HTTP verb associated with the marker attribute.
/// - `_args`: marker path and options, interpreted by the parent macro.
/// - `input`: annotated method tokens reported in the diagnostic.
#[must_use]
#[inline]
pub fn expand_mapping(_method: &str, _args: TokenStream, input: TokenStream) -> TokenStream {
    Error::new_spanned(
        input,
        "route mapping attributes must be inside a #[rest_controller(...)] impl block",
    )
    .into_compile_error()
}

/// Rejects a generic route attribute used outside `rest_controller`.
///
/// This attribute is consumed by the enclosing `rest_controller` macro.
/// Using it independently is a compile-time error and does not register a
/// route.
///
/// # Parameters
///
/// - `_args`: path, methods, and transport options parsed by the parent macro.
/// - `input`: annotated method tokens reported in the diagnostic.
#[must_use]
#[inline]
pub fn expand_route(_args: TokenStream, input: TokenStream) -> TokenStream {
    Error::new_spanned(
        input,
        "route mapping attributes must be inside a #[rest_controller(...)] impl block",
    )
    .into_compile_error()
}
