// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Procedural macros for `qubit-web`.
//!
//! The macros declare routes on an inherent Controller implementation; the
//! main `qubit-web` crate re-exports them and provides the route builder:
//!
//! ```ignore
//! use std::sync::Arc;
//! use qubit_web::{ControllerRoutes, rest_controller, get_mapping};
//!
//! struct Health;
//!
//! #[rest_controller("/health")]
//! impl Health {
//!     #[get_mapping("")]
//!     async fn check(&self) -> &'static str { "ok" }
//! }
//!
//! let router = ControllerRoutes::new().add(Arc::new(Health))?.finish()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod controller;
mod route;

use proc_macro::TokenStream;

/// Generates the explicitly mountable route definition for an inherent
/// Controller implementation.
///
/// Place this attribute on a non-generic inherent `impl` block. Its string
/// argument is the absolute controller path prefix; mapped methods append a
/// child path to that prefix. Use `#[route(path, method = "GET", kind = "sse")`
/// for explicit methods and transport kinds. Supported kinds are `short`,
/// `sse`, and `ws`; generic impl blocks and `where` clauses produce
/// compile-time diagnostics.
///
/// # Parameters
///
/// - `args`: a string literal containing the controller path prefix.
/// - `input`: the inherent impl block whose async `&self` methods declare
///   routes.
#[must_use]
#[proc_macro_attribute]
pub fn rest_controller(args: TokenStream, input: TokenStream) -> TokenStream {
    controller::expand(args.into(), input.into()).into()
}

macro_rules! mapping_macro {
    ($($name:ident => $method:literal),* $(,)?) => {$ (
        #[doc = concat!(
            "Declares a ", $method, " route on an async `&self` Controller method.\n\n",
            "The argument is a string literal child path, optionally followed by `kind = \"short\"`, `kind = \"sse\"`, or `kind = \"ws\"`. The controller prefix is supplied by `#[rest_controller(\"/...\")]`."
        )]
        #[must_use]
        #[proc_macro_attribute]
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

/// Declares a Controller method route with one or more explicit HTTP methods.
///
/// The first argument is a string literal child path. Repeat `method = "..."`
/// for each supported method (`GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`,
/// or `OPTIONS`); optionally set `kind` to `short`, `sse`, or `ws`. Duplicate
/// methods, unknown options, and invalid route templates produce compile-time
/// diagnostics.
///
/// # Parameters
///
/// - `args`: a route path and its method/transport options.
/// - `input`: an async `&self` Controller method.
#[must_use]
#[proc_macro_attribute]
pub fn route(args: TokenStream, input: TokenStream) -> TokenStream {
    route::expand_route(args.into(), input.into()).into()
}
