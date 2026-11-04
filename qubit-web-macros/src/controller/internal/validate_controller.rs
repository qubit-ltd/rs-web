// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;

use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::FnArg;
use syn::ImplItem;

use super::ControllerModel;
use super::controller_model::ControllerMethod;
use crate::controller::cfg_condition;
use crate::controller::combine_conditions;
use crate::controller::make_route;
use crate::controller::parse_options;
use crate::controller::resolve_self_type;
use crate::controller::state_extractor_type;
use crate::controller::validate_method;
use crate::controller::validate_path;
use crate::controller::validate_template;
use crate::controller::validate_websocket_methods;

/// Resolves route declarations and checks method, path, cfg, and state constraints.
pub(in crate::controller) fn validate(model: &mut ControllerModel) -> syn::Result<()> {
    validate_path(&model.prefix.value(), true, model.prefix.span())?;
    validate_template(&model.prefix.value(), model.prefix.span())?;
    let controller_name = model
        .controller_name
        .clone()
        .ok_or_else(|| syn::Error::new_spanned(&model.self_ty, "controller type must be a named struct"))?;
    model.instance_name = Some(format_ident!("__QubitControllerInstanceFor{}", controller_name));
    let mut used_routes: HashMap<(String, String), Vec<Vec<TokenStream>>> = HashMap::new();
    for pending in &model.pending {
        let cfg = pending.cfg.as_ref().map_err(|error| error.clone())?;
        let Some(ImplItem::Fn(method)) = model.implementation.items.get(pending.item_index) else {
            unreachable!("pending controller method index must point to an impl method")
        };
        let mut routes = Vec::new();
        for pending_route in &pending.routes {
            let pending_route = pending_route.as_ref().map_err(|error| error.clone())?;
            let (methods, kind) = if let Some(verb) = &pending_route.marker_verb {
                let (_, kind) = parse_options(pending_route.options.clone(), false)?;
                (vec![verb.clone()], kind)
            } else {
                let (methods, kind) = parse_options(pending_route.options.clone(), true)?;
                if methods.is_empty() {
                    return Err(syn::Error::new(
                        pending_route.span,
                        "route requires at least one method = \"...\"",
                    ));
                }
                (methods, kind)
            };
            let route = make_route(&model.prefix.value(), pending_route.path.clone(), methods, kind)?;
            validate_websocket_methods(&route.methods, &route.kind, pending_route.span)?;
            routes.push(route);
        }
        validate_method(method)?;
        for input in method.sig.inputs.iter().skip(1) {
            let FnArg::Typed(argument) = input else { continue };
            let resolved_ty = resolve_self_type(&argument.ty, &model.self_ty);
            let Some(state) = state_extractor_type(&resolved_ty) else {
                continue;
            };
            if let Some((_, conditions)) = model
                .controller_states
                .iter_mut()
                .find(|(existing, _)| quote!(#existing).to_string() == quote!(#state).to_string())
            {
                conditions.push(cfg_condition(cfg));
            } else {
                model.controller_states.push((state, vec![cfg_condition(cfg)]));
            }
        }
        for route in &routes {
            for verb in &route.methods {
                let key = (verb.clone(), route.path.clone());
                if let Some(previous_conditions) = used_routes.get_mut(&key) {
                    for previous_cfg in previous_conditions.iter() {
                        if previous_cfg.is_empty() && cfg.is_empty() {
                            return Err(syn::Error::new_spanned(
                                method,
                                format!("duplicate controller route: {verb} {}", route.path),
                            ));
                        }
                        model.conditional_errors.push((
                            combine_conditions(previous_cfg, cfg),
                            format!("duplicate controller route: {verb} {}", route.path),
                        ));
                    }
                    previous_conditions.push(cfg.clone());
                } else {
                    used_routes.insert(key, vec![cfg.clone()]);
                }
            }
        }
        model.methods.push(ControllerMethod {
            item_index: pending.item_index,
            routes,
            cfg: cfg.clone(),
        });
    }
    let states = &model.controller_states;
    for left in 0..states.len() {
        for right in (left + 1)..states.len() {
            let (_, left_conditions) = &states[left];
            let (_, right_conditions) = &states[right];
            let overlap = quote! { all(any(#(#left_conditions),*), any(#(#right_conditions),*)) };
            model.conditional_errors.push((
                vec![overlap],
                "all State extractors in one Controller must use the same state type".to_owned(),
            ));
        }
    }
    Ok(())
}
