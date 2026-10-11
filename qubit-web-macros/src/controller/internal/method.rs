// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::route::Route;

/// A controller handler together with its generated route registrations.
pub(in crate::controller) struct Method {
    /// Routes declared on the handler method.
    pub(in crate::controller) routes: Vec<Route>,
    /// Generated function name used by the router.
    pub(in crate::controller) handler: syn::Ident,
}
