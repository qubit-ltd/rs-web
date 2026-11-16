// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use syn::ItemImpl;
use syn::LitStr;
use syn::Type;

use super::controller_method::ControllerMethod;
use super::pending_method::PendingMethod;

/// Raw controller syntax and normalized analysis shared by the three stages.
pub(in crate::controller) struct ControllerModel {
    /// Original implementation block, with mapping attributes removed.
    pub(super) implementation: ItemImpl,
    /// Concrete type implemented by the controller block.
    pub(super) self_ty: Type,
    /// Literal route prefix supplied to the controller attribute.
    pub(super) prefix: LitStr,
    /// Consumer-visible path to the runtime crate.
    pub(super) runtime: TokenStream,
    /// Consumer-visible path to the Axum crate.
    pub(super) axum: TokenStream,
    /// Optional generated wrapper instance identifier.
    pub(super) instance_name: Option<Ident>,
    /// Optional identifier of the named controller type.
    pub(super) controller_name: Option<Ident>,
    /// Methods and route declarations awaiting validation.
    pub(super) pending: Vec<PendingMethod>,
    /// Validated method and route mappings.
    pub(super) methods: Vec<ControllerMethod>,
    /// State types and their generated registration tokens.
    pub(super) controller_states: Vec<(Type, Vec<TokenStream>)>,
    /// Conditional compile errors deferred until generated cfg evaluation.
    pub(super) conditional_errors: Vec<(Vec<TokenStream>, String)>,
}
