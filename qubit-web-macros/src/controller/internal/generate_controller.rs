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
use syn::ImplItem;

use super::ControllerModel;
use super::controller_method::ControllerMethod;
use crate::controller::cfg_attribute;
use crate::controller::make_handler;
use crate::controller::method_filter;
use crate::controller::route_kind;

/// Generates handlers, route metadata, and controller registration
/// implementations.
///
/// # Parameters
///
/// - `model`: Parsed and validated controller model to generate.
pub(in crate::controller) fn generate(model: ControllerModel) -> syn::Result<TokenStream> {
    let ControllerModel {
        implementation,
        self_ty,
        prefix: _prefix,
        runtime,
        axum,
        instance_name: instance_name_option,
        controller_name,
        methods,
        controller_states,
        conditional_errors,
        ..
    } = model;
    let controller_name = controller_name.expect("validation checks the named controller type");
    let instance_name = instance_name_option.expect("validation assigns the controller wrapper name");
    let mut declarations = Vec::new();
    let mut methods_with_handlers: Vec<(ControllerMethod, syn::Ident)> = Vec::new();
    for (generated_index, method) in methods.into_iter().enumerate() {
        let Some(ImplItem::Fn(source_method)) = implementation.items.get(method.item_index) else {
            unreachable!("validated method index must point to an impl method")
        };
        let handler_name = controller_name.to_string().to_lowercase();
        let handler = format_ident!("__qubit_controller_handler_{}_{}", handler_name, generated_index);
        let cfg_attr = cfg_attribute(&method.cfg);
        let handler_tokens = make_handler(source_method, &handler, &instance_name, &self_ty, &axum);
        declarations.push(quote! { #cfg_attr #handler_tokens });
        methods_with_handlers.push((method, handler));
    }
    let metadata: Vec<_> = methods_with_handlers
        .iter()
        .flat_map(|(method, _handler)| {
            method.routes.iter().flat_map(|route| {
                route.methods.iter().map(|verb| {
                    let path = &route.path;
                    let kind = route_kind(&route.kind, &runtime);
                    let cfg_attr = cfg_attribute(&method.cfg);
                    quote! { #cfg_attr #runtime::mvc::RouteMetadata::new(#verb, #path, #kind) }
                })
            })
        })
        .collect();
    let mut state_impls = Vec::new();
    let all_state_conditions: Vec<_> = controller_states
        .iter()
        .flat_map(|(_, conditions)| conditions.clone())
        .collect();
    let generic_cfg = cfg_attribute(&[quote! { not(any(#(#all_state_conditions),*)) }]);
    let mut impl_specs = vec![(
        generic_cfg,
        quote! { <__QubitControllerState> },
        quote! { __QubitControllerState },
    )];
    for (index, (state, conditions)) in controller_states.iter().enumerate() {
        let other_conditions: Vec<_> = controller_states
            .iter()
            .enumerate()
            .filter(|(other_index, _)| *other_index != index)
            .flat_map(|(_, (_, conditions))| conditions.clone())
            .collect();
        let cfg_attr = cfg_attribute(&[quote! {
            all(any(#(#conditions),*), not(any(#(#other_conditions),*)))
        }]);
        impl_specs.push((cfg_attr, quote! {}, quote! { #state }));
    }
    let instance_name_ref = &instance_name;
    let runtime_ref = &runtime;
    let axum_ref = &axum;
    for (cfg_attr, impl_generics, state_type) in impl_specs {
        let state_type_ref = &state_type;
        let registrations = methods_with_handlers
            .iter()
            .flat_map(|(method, handler)| {
                method.routes.iter().map(move |route| {
                    let path = &route.path;
                    let strategy_layers = if route.kind == "short" {
                        quote! {
                            let route = route
                                .layer(#runtime_ref::RequestLimitLayer::from_state(limit_state.clone()))
                                .layer(#runtime_ref::diagnostic::DiagnosticLayer::new());
                        }
                    } else {
                        quote! {
                            let route = route
                                .layer(#runtime_ref::StreamAdmissionLayer::from_state(stream_state.clone()))
                                .layer(#runtime_ref::diagnostic::DiagnosticLayer::new());
                        }
                    };
                    let filters = route.methods.iter().map(|verb| method_filter(verb, axum_ref));
                    let mut filters = filters.collect::<Vec<_>>().into_iter();
                    let first = filters.next().expect("route has methods");
                    let filter = filters.fold(first, |combined, next| quote! { #combined.or(#next) });
                    let method_cfg = cfg_attribute(&method.cfg);
                    quote! {
                        #method_cfg
                        {
                            let route = #axum_ref::Router::<#state_type_ref>::new()
                                .route(#path, #axum_ref::routing::on(#filter, #handler))
                                .layer(#axum_ref::Extension(#instance_name_ref(self.clone())));
                            #strategy_layers
                            router = router.merge(route);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        state_impls.push(quote! {
            #cfg_attr
            impl #impl_generics #runtime::mvc::ControllerDefinition<#state_type> for #self_ty
            where
                #state_type: Clone + Send + Sync + 'static,
            {
                fn route_metadata() -> &'static [#runtime::mvc::RouteMetadata] {
                    static __QUBIT_ROUTES: &[#runtime::mvc::RouteMetadata] = &[#(#metadata),*];
                    __QUBIT_ROUTES
                }

                fn register(
                    self: ::std::sync::Arc<Self>,
                    mut router: #axum::Router<#state_type>,
                    limit_state: #runtime::limit::LimitState,
                    stream_state: #runtime::limit::StreamAdmissionState,
                ) -> #axum::Router<#state_type> {
                    #(#registrations)*
                    router
                }
            }
        });
    }
    let conditional_errors = conditional_errors.into_iter().map(|(conditions, message)| {
        let cfg_attr = cfg_attribute(&conditions);
        quote! { #cfg_attr compile_error!(#message); }
    });
    let item_impl = implementation;
    Ok(quote! {
        #item_impl

        const _: () = {
            #[doc(hidden)]
            struct #instance_name<__Controller>(::std::sync::Arc<__Controller>);
            impl<__Controller> ::std::clone::Clone for #instance_name<__Controller> {
                fn clone(&self) -> Self { Self(self.0.clone()) }
            }

            #(#declarations)*

            #(#conditional_errors)*
            #(#state_impls)*
        };
    })
}
