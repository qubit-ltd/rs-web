// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Procedural macros for `qubit-web`.

mod controller;
mod route;

use proc_macro::TokenStream;

#[proc_macro_attribute]
/// Generates the explicitly mountable route definition for an inherent
/// Controller implementation.
///
/// Generic `impl` blocks and `where` clauses are rejected at compile time in
/// this initial version.
pub fn rest_controller(args: TokenStream, input: TokenStream) -> TokenStream {
    controller::expand(args.into(), input.into()).into()
}

macro_rules! mapping_macro {
    ($($name:ident => $method:literal),* $(,)?) => {$ (
        #[proc_macro_attribute]
        #[doc = concat!("Declares an ", $method, " route on a Controller method.")]
        pub fn $name(args: TokenStream, input: TokenStream) -> TokenStream {
            route::expand_mapping($method, args.into(), input.into()).into()
        }
    )* };
}

mapping_macro! {
    get_mapping => "GET",
    post_mapping => "POST",
    put_mapping => "PUT",
    patch_mapping => "PATCH",
    delete_mapping => "DELETE",
    get => "GET",
    post => "POST",
    put => "PUT",
    patch => "PATCH",
    delete => "DELETE",
}

#[proc_macro_attribute]
/// Declares a Controller method route with one or more explicit HTTP methods.
pub fn route(args: TokenStream, input: TokenStream) -> TokenStream {
    route::expand_route(args.into(), input.into()).into()
}
