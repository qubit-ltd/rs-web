// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use proc_macro2::TokenStream;

/// Lets the enclosing `rest_controller` attribute consume the method marker.
pub fn expand_mapping(_method: &str, _args: TokenStream, input: TokenStream) -> TokenStream {
    input
}

/// Lets the enclosing `rest_controller` attribute consume the generic marker.
pub fn expand_route(_args: TokenStream, input: TokenStream) -> TokenStream {
    input
}
