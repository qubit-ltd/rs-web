// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::Attribute;
use syn::ImplItem;
use syn::ItemImpl;
use syn::LitStr;
use syn::Type;
use syn::parse2;
use syn::spanned::Spanned;

use super::MappingArgs;
use super::RouteArgs;
use super::controller_model::ControllerModel;
use super::pending_method::PendingMethod;
use super::pending_route::PendingRoute;
use super::resolve_runtime_path;
use crate::controller::cfg_attr_contains_mapping;
use crate::controller::effective_cfg;
use crate::controller::marker_verb;

/// Reads controller syntax and preserves its declaration and cfg order.
///
/// # Parameters
///
/// - `args`: Controller attribute argument tokens.
/// - `input`: Controller implementation tokens.
pub(in crate::controller) fn parse(args: TokenStream, input: TokenStream) -> syn::Result<ControllerModel> {
    let runtime = resolve_runtime_path()?;
    let axum = quote! { #runtime::__private::axum };
    let prefix = parse2::<LitStr>(args)?;
    let mut implementation = parse2::<ItemImpl>(input)?;
    if implementation.trait_.is_some() {
        return Err(syn::Error::new_spanned(
            implementation,
            "rest_controller only supports inherent impl blocks",
        ));
    }
    if !implementation.generics.params.is_empty() || implementation.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &implementation.generics,
            "generic Controller impl blocks are not supported",
        ));
    }
    let self_ty = (*implementation.self_ty).clone();
    let controller_name = if let Type::Path(path) = &self_ty {
        path.path.segments.last().map(|segment| segment.ident.clone())
    } else {
        None
    };
    let instance_name = controller_name
        .as_ref()
        .map(|name| format_ident!("__QubitControllerInstanceFor{}", name));
    let mut pending = Vec::new();
    for (item_index, item) in implementation.items.iter_mut().enumerate() {
        let ImplItem::Fn(method) = item else { continue };
        let cfg = effective_cfg(&method.attrs);
        let attrs = std::mem::take(&mut method.attrs);
        let mut routes = Vec::new();
        for attr in attrs {
            match parse_mapping_attribute(&attr) {
                Ok(Some(route)) => routes.push(Ok(route)),
                Ok(None) => method.attrs.push(attr),
                Err(error) => routes.push(Err(error)),
            }
        }
        if !routes.is_empty() || cfg.is_err() {
            pending.push(PendingMethod {
                item_index,
                cfg,
                routes,
            });
        }
    }
    Ok(ControllerModel {
        implementation,
        self_ty,
        prefix,
        runtime,
        axum,
        instance_name,
        controller_name,
        pending,
        methods: Vec::new(),
        controller_states: Vec::new(),
        conditional_errors: Vec::new(),
    })
}

/// Parses a route mapping attribute, returning `None` for unrelated attributes.
///
/// # Parameters
///
/// - `attr`: Attribute to parse as a route mapping.
fn parse_mapping_attribute(attr: &Attribute) -> syn::Result<Option<PendingRoute>> {
    let Some(name) = attr.path().segments.last().map(|segment| segment.ident.to_string()) else {
        return Ok(None);
    };
    if let Some(verb) = marker_verb(&name) {
        let mapping = parse2::<MappingArgs>(attr.meta.require_list()?.tokens.clone())?;
        return Ok(Some(PendingRoute {
            path: mapping.path,
            options: mapping.options,
            marker_verb: Some(verb.to_owned()),
            span: attr.span(),
        }));
    }
    if name == "route" {
        let route = parse2::<RouteArgs>(attr.meta.require_list()?.tokens.clone())?;
        return Ok(Some(PendingRoute {
            path: route.path,
            options: route.options,
            marker_verb: None,
            span: attr.span(),
        }));
    }
    if name == "cfg_attr" && cfg_attr_contains_mapping(attr)? {
        return Err(syn::Error::new_spanned(
            attr,
            "route mapping attributes inside cfg_attr are not supported",
        ));
    }
    Ok(None)
}
