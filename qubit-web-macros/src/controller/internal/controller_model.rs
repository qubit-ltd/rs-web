// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::Route;
use proc_macro2::{Ident, Span, TokenStream};
use syn::punctuated::Punctuated;
use syn::{ItemImpl, LitStr, Meta, Token, Type};

/// Raw controller syntax and normalized analysis shared by the three stages.
pub(in crate::controller) struct ControllerModel {
    pub(super) implementation: ItemImpl,
    pub(super) self_ty: Type,
    pub(super) prefix: LitStr,
    pub(super) runtime: TokenStream,
    pub(super) axum: TokenStream,
    pub(super) instance_name: Option<Ident>,
    pub(super) controller_name: Option<Ident>,
    pub(super) pending: Vec<PendingMethod>,
    pub(super) methods: Vec<ControllerMethod>,
    pub(super) controller_states: Vec<(Type, Vec<TokenStream>)>,
    pub(super) conditional_errors: Vec<(Vec<TokenStream>, String)>,
}

/// Parsed method declaration awaiting semantic checks.
pub(super) struct PendingMethod {
    pub(super) item_index: usize,
    pub(super) cfg: syn::Result<Vec<TokenStream>>,
    pub(super) routes: Vec<syn::Result<PendingRoute>>,
}

/// Parsed route attribute awaiting path and option validation.
pub(super) struct PendingRoute {
    pub(super) path: LitStr,
    pub(super) options: Punctuated<Meta, Token![,]>,
    pub(super) marker_verb: Option<String>,
    pub(super) span: Span,
}

/// A validated mapped method passed from validation to generation.
pub(super) struct ControllerMethod {
    pub(super) item_index: usize,
    pub(super) routes: Vec<Route>,
    pub(super) cfg: Vec<TokenStream>,
}
