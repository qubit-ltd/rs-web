// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::collections::HashSet;

use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::Attribute;
use syn::Expr;
use syn::ExprLit;
use syn::FnArg;
use syn::GenericArgument;
use syn::ImplItem;
use syn::ImplItemFn;
use syn::ItemImpl;
use syn::Lit;
use syn::LitStr;
use syn::Meta;
use syn::PathArguments;
use syn::Token;
use syn::Type;
use syn::parse2;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;

mod internal;
use internal::MappingArgs;
use internal::Method;
use internal::Route;
use internal::RouteArgs;

/// Expands a controller attribute into its implementation and route definition.
///
/// Invalid input is emitted as a compile-time diagnostic instead of panicking.
#[must_use]
pub fn expand(args: TokenStream, input: TokenStream) -> TokenStream {
    match expand_inner(args, input) {
        Ok(tokens) => tokens,
        Err(error) => error.into_compile_error(),
    }
}

/// Parses the controller and generates its handler functions and registrations.
///
/// The input must be a non-generic inherent implementation with valid route
/// attributes and compatible state extractor types.
fn expand_inner(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
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
    validate_path(&prefix.value(), true, prefix.span())?;
    validate_template(&prefix.value(), prefix.span())?;

    let self_ty = (*implementation.self_ty).clone();
    let mut controller_name = None;
    if let Type::Path(path) = &self_ty
        && let Some(segment) = path.path.segments.last()
    {
        controller_name = Some(segment.ident.clone());
    }
    let controller_name =
        controller_name.ok_or_else(|| syn::Error::new_spanned(&self_ty, "controller type must be a named struct"))?;
    let instance_name = format_ident!("__QubitControllerInstanceFor{}", controller_name);

    let mut methods = Vec::new();
    let mut declarations = Vec::new();
    let mut used_routes: HashMap<(String, String), Vec<Vec<TokenStream>>> = HashMap::new();
    let mut controller_states: Vec<(Type, Vec<TokenStream>)> = Vec::new();
    let mut conditional_errors = Vec::new();
    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else { continue };
        let cfg = effective_cfg(&method.attrs)?;
        let attrs = std::mem::take(&mut method.attrs);
        let mut routes = Vec::new();
        for attr in attrs {
            let Some(name) = attr.path().segments.last().map(|segment| segment.ident.to_string()) else {
                method.attrs.push(attr);
                continue;
            };
            if let Some(verb) = marker_verb(&name) {
                let mapping = parse2::<MappingArgs>(attr.meta.require_list()?.tokens.clone())?;
                let (_, kind) = parse_options(mapping.options, false)?;
                let route = make_route(&prefix.value(), mapping.path, vec![verb.to_owned()], kind)?;
                validate_websocket_methods(&route.methods, &route.kind, attr.span())?;
                routes.push(route);
            } else if name == "route" {
                let route = parse2::<RouteArgs>(attr.meta.require_list()?.tokens.clone())?;
                let (verbs, kind) = parse_options(route.options, true)?;
                if verbs.is_empty() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "route requires at least one method = \"...\"",
                    ));
                }
                let route = make_route(&prefix.value(), route.path, verbs, kind)?;
                validate_websocket_methods(&route.methods, &route.kind, attr.span())?;
                routes.push(route);
            } else if name == "cfg_attr" {
                if cfg_attr_contains_mapping(&attr)? {
                    return Err(syn::Error::new_spanned(
                        attr,
                        "route mapping attributes inside cfg_attr are not supported",
                    ));
                }
                method.attrs.push(attr);
            } else {
                method.attrs.push(attr);
            }
        }
        if routes.is_empty() {
            continue;
        }
        validate_method(method)?;
        for input in method.sig.inputs.iter().skip(1) {
            let FnArg::Typed(argument) = input else {
                continue;
            };
            let Some(state) = state_extractor_type(&argument.ty) else {
                continue;
            };
            if let Some((_, conditions)) = controller_states
                .iter_mut()
                .find(|(existing, _)| quote!(#existing).to_string() == quote!(#state).to_string())
            {
                conditions.push(cfg_condition(&cfg));
            } else {
                controller_states.push((state, vec![cfg_condition(&cfg)]));
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
                        conditional_errors.push((
                            combine_conditions(previous_cfg, &cfg),
                            format!("duplicate controller route: {verb} {}", route.path),
                        ));
                    }
                    previous_conditions.push(cfg.clone());
                } else {
                    used_routes.insert(key, vec![cfg.clone()]);
                }
            }
        }
        let handler_name = controller_name.to_string().to_lowercase();
        let handler = format_ident!("__qubit_controller_handler_{}_{}", handler_name, methods.len());
        let cfg_attr = cfg_attribute(&cfg);
        let handler_tokens = make_handler(method, &handler, &instance_name, &self_ty);
        declarations.push(quote! { #cfg_attr #handler_tokens });
        methods.push(Method { routes, handler, cfg });
    }

    let metadata: Vec<_> = methods
        .iter()
        .flat_map(|method| {
            method.routes.iter().flat_map(|route| {
                route.methods.iter().map(|verb| {
                    let path = &route.path;
                    let kind = route_kind(&route.kind);
                    let cfg_attr = cfg_attribute(&method.cfg);
                    quote! { #cfg_attr ::qubit_web::mvc::RouteMetadata::new(#verb, #path, #kind) }
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
    for left in 0..controller_states.len() {
        for right in (left + 1)..controller_states.len() {
            let (_, left_conditions) = &controller_states[left];
            let (_, right_conditions) = &controller_states[right];
            let overlap = quote! { all(any(#(#left_conditions),*), any(#(#right_conditions),*)) };
            conditional_errors.push((
                vec![overlap],
                "all State extractors in one Controller must use the same state type".to_owned(),
            ));
        }
    }
    let instance_name_ref = &instance_name;
    for (cfg_attr, impl_generics, state_type) in impl_specs {
        let state_type_ref = &state_type;
        let registrations = methods
            .iter()
            .flat_map(|method| {
                let handler = &method.handler;
                method.routes.iter().map(move |route| {
                    let path = &route.path;
                    let strategy_layers = if route.kind == "short" {
                        quote! {
                            let route = route
                                .layer(::qubit_web::RequestLimitLayer::from_state(limit_state.clone()))
                                .layer(::qubit_web::diagnostic::DiagnosticLayer::new());
                        }
                    } else {
                        quote! {
                            let route = route
                                .layer(::qubit_web::StreamAdmissionLayer::from_state(stream_state.clone()))
                                .layer(::qubit_web::diagnostic::DiagnosticLayer::new());
                        }
                    };
                    let filters = route.methods.iter().map(|verb| method_filter(verb));
                    let mut filters = filters.collect::<Vec<_>>().into_iter();
                    let first = filters.next().expect("route has methods");
                    let filter = filters.fold(first, |combined, next| quote! { #combined.or(#next) });
                    let method_cfg = cfg_attribute(&method.cfg);
                    quote! {
                        #method_cfg
                        {
                            let route = ::axum::Router::<#state_type_ref>::new()
                                .route(#path, ::axum::routing::on(#filter, #handler))
                                .layer(::axum::Extension(#instance_name_ref(self.clone())));
                            #strategy_layers
                            router = router.merge(route);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        state_impls.push(quote! {
            #cfg_attr
            impl #impl_generics ::qubit_web::mvc::ControllerDefinition<#state_type> for #self_ty
            where
                #state_type: Clone + Send + Sync + 'static,
            {
                fn route_metadata() -> &'static [::qubit_web::mvc::RouteMetadata] {
                    static __QUBIT_ROUTES: &[::qubit_web::mvc::RouteMetadata] = &[#(#metadata),*];
                    __QUBIT_ROUTES
                }

                fn register(
                    self: ::std::sync::Arc<Self>,
                    mut router: ::axum::Router<#state_type>,
                    limit_state: ::qubit_web::limit::LimitState,
                    stream_state: ::qubit_web::limit::StreamAdmissionState,
                ) -> ::axum::Router<#state_type> {
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

/// Returns the state type when `ty` is an Axum `State<T>` extractor.
fn state_extractor_type(ty: &Type) -> Option<Type> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    if segment.ident != "State" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(state) => Some(state.clone()),
        _ => None,
    })
}

/// Collects the predicates that determine whether a mapped method exists.
fn effective_cfg(attrs: &[Attribute]) -> syn::Result<Vec<TokenStream>> {
    let mut predicates = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("cfg") {
            let predicate: Meta = attr.parse_args()?;
            predicates.push(quote! { #predicate });
        } else if attr.path().is_ident("cfg_attr") {
            let args = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            let mut args = args.into_iter();
            let condition = args
                .next()
                .ok_or_else(|| syn::Error::new_spanned(attr, "cfg_attr requires a condition"))?;
            collect_cfg_attr_predicates(quote! { #condition }, args.collect(), &mut predicates)?;
        }
    }
    Ok(predicates)
}

/// Converts nested cfg attributes under cfg_attr into equivalent implications.
fn collect_cfg_attr_predicates(
    condition: TokenStream,
    nested_attrs: Vec<Meta>,
    predicates: &mut Vec<TokenStream>,
) -> syn::Result<()> {
    for nested in nested_attrs {
        if nested.path().is_ident("cfg") {
            let predicate = nested.require_list()?.tokens.clone();
            predicates.push(quote! { any(not(#condition), #predicate) });
        } else if nested.path().is_ident("cfg_attr") {
            let args = nested
                .require_list()?
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            let mut args = args.into_iter();
            let child_condition = args
                .next()
                .ok_or_else(|| syn::Error::new_spanned(&nested, "cfg_attr requires a condition"))?;
            let combined = quote! { all(#condition, #child_condition) };
            collect_cfg_attr_predicates(combined, args.collect(), predicates)?;
        }
    }
    Ok(())
}

/// Builds a cfg attribute from the conjunction of effective method predicates.
fn cfg_attribute(predicates: &[TokenStream]) -> TokenStream {
    if predicates.is_empty() {
        TokenStream::new()
    } else {
        quote! { #[cfg(all(#(#predicates),*))] }
    }
}

/// Returns a single predicate equivalent to the method's effective cfg set.
fn cfg_condition(predicates: &[TokenStream]) -> TokenStream {
    quote! { all(#(#predicates),*) }
}

/// Combines the conditions under which two route declarations overlap.
fn combine_conditions(left: &[TokenStream], right: &[TokenStream]) -> Vec<TokenStream> {
    vec![cfg_condition(left), cfg_condition(right)]
}

/// Detects route-mapping attributes nested inside `cfg_attr` declarations.
fn cfg_attr_contains_mapping(attr: &Attribute) -> syn::Result<bool> {
    let args = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
    for meta in args.into_iter().skip(1) {
        if meta_contains_mapping(&meta)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Checks one nested attribute for a route mapping, including nested cfg_attr.
fn meta_contains_mapping(meta: &Meta) -> syn::Result<bool> {
    let mapped = meta
        .path()
        .segments
        .last()
        .is_some_and(|segment| marker_verb(&segment.ident.to_string()).is_some() || segment.ident == "route");
    if mapped {
        return Ok(true);
    }
    if meta.path().is_ident("cfg_attr") {
        let nested = meta
            .require_list()?
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for nested_meta in nested.into_iter().skip(1) {
            if meta_contains_mapping(&nested_meta)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Maps a supported verb-mapping attribute name to its HTTP method.
fn marker_verb(name: &str) -> Option<&'static str> {
    match name {
        "get_mapping" | "get" => Some("GET"),
        "post_mapping" | "post" => Some("POST"),
        "put_mapping" | "put" => Some("PUT"),
        "patch_mapping" | "patch" => Some("PATCH"),
        "delete_mapping" | "delete" => Some("DELETE"),
        _ => None,
    }
}

fn validate_websocket_methods(methods: &[String], kind: &str, span: Span) -> syn::Result<()> {
    if kind == "ws" && methods.iter().any(|method| method != "GET") {
        return Err(syn::Error::new(span, "WebSocket route kind requires GET"));
    }
    Ok(())
}

/// Validates route options and returns methods plus the selected strategy.
///
/// `allow_method` is true only for the multi-method `route` attribute.
fn parse_options(options: Punctuated<Meta, Token![,]>, allow_method: bool) -> syn::Result<(Vec<String>, String)> {
    let mut methods = Vec::new();
    let mut kind = "short".to_owned();
    let mut saw_kind = false;
    for option in options {
        let Meta::NameValue(value) = option else {
            return Err(syn::Error::new_spanned(option, "expected `name = \"value\"`"));
        };
        let Some(name) = value.path.get_ident().map(ToString::to_string) else {
            return Err(syn::Error::new_spanned(value.path, "expected a simple option name"));
        };
        let Expr::Lit(ExprLit {
            lit: Lit::Str(literal), ..
        }) = value.value
        else {
            return Err(syn::Error::new_spanned(
                value.value,
                "option values must be string literals",
            ));
        };
        match name.as_str() {
            "method" if allow_method => {
                let method = literal.value().to_ascii_uppercase();
                if !matches!(
                    method.as_str(),
                    "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS"
                ) {
                    return Err(syn::Error::new(literal.span(), "unknown HTTP method"));
                }
                if methods.contains(&method) {
                    return Err(syn::Error::new(literal.span(), "duplicate HTTP method"));
                }
                methods.push(method);
            }
            "kind" => {
                if saw_kind {
                    return Err(syn::Error::new(literal.span(), "duplicate route kind"));
                }
                kind = literal.value();
                if !matches!(kind.as_str(), "short" | "sse" | "ws") {
                    return Err(syn::Error::new(
                        literal.span(),
                        "route kind must be `short`, `sse`, or `ws`",
                    ));
                }
                saw_kind = true;
            }
            "method" => {
                return Err(syn::Error::new(
                    literal.span(),
                    "method is only supported by #[route(...)]",
                ));
            }
            _ => return Err(syn::Error::new(value.path.span(), "unknown route option")),
        }
    }
    Ok((methods, kind))
}

/// Joins a controller prefix and child path after validating both templates.
fn make_route(prefix: &str, child: LitStr, methods: Vec<String>, kind: String) -> syn::Result<Route> {
    let child = child.value();
    validate_path(&child, false, child_span(&child))?;
    let path = if prefix == "/" {
        if child.is_empty() { "/".to_owned() } else { child }
    } else if child.is_empty() {
        prefix.to_owned()
    } else {
        format!("{prefix}{child}")
    };
    validate_template(&path, child_span(&path))?;
    Ok(Route { path, methods, kind })
}

/// Produces the fallback call-site span used for synthesized path diagnostics.
fn child_span(_value: &str) -> Span {
    Span::call_site()
}

/// Checks absolute-path syntax and rejects query or fragment components.
///
/// Prefixes cannot end in a slash except for `/`; child paths may be empty.
fn validate_path(path: &str, prefix: bool, span: Span) -> syn::Result<()> {
    let valid = if prefix {
        path == "/" || (path.starts_with('/') && !path.ends_with('/') && !path.contains("//"))
    } else {
        path.is_empty() || (path.starts_with('/') && !path.contains("//"))
    };
    if !valid || path.contains('?') || path.contains('#') {
        let label = if prefix { "controller prefix" } else { "route path" };
        return Err(syn::Error::new(
            span,
            format!("{label} must be a valid absolute route template"),
        ));
    }
    if !prefix {
        validate_template(path, span)?;
    }
    Ok(())
}

/// Validates path parameter segments, uniqueness, and final wildcard placement.
fn validate_template(path: &str, span: Span) -> syn::Result<()> {
    let mut names = HashSet::new();
    let segments: Vec<_> = path.split('/').collect();
    for (index, segment) in segments.iter().enumerate() {
        if segment.is_empty() {
            continue;
        }
        let Some(open) = segment.find(['{', '}']) else {
            continue;
        };
        let Some(close) = segment.rfind('}') else {
            return Err(syn::Error::new(span, "route path contains an unclosed parameter"));
        };
        if segment[open..].contains('{') && segment[open + 1..].contains('{')
            || segment[close + 1..].contains('}')
            || close <= open
            || open != 0
            || close + 1 != segment.len()
        {
            return Err(syn::Error::new(
                span,
                "route parameters must occupy a complete path segment",
            ));
        }
        let parameter = &segment[open + 1..close];
        let (wildcard, name) = parameter
            .strip_prefix('*')
            .map_or((false, parameter), |name| (true, name));
        let valid_name = !name.is_empty() && !name.chars().any(|character| matches!(character, '{' | '}' | '*' | '/'));
        if !valid_name {
            return Err(syn::Error::new(
                span,
                "route parameter names must be nonempty and cannot contain braces, slashes, or wildcard markers",
            ));
        }
        if !names.insert(name) {
            return Err(syn::Error::new(span, "route path contains a duplicate parameter name"));
        }
        if wildcard && index + 1 != segments.len() {
            return Err(syn::Error::new(
                span,
                "wildcard route parameters must be the final segment",
            ));
        }
    }
    Ok(())
}

/// Enforces the async, immutable `&self`, non-generic handler contract.
fn validate_method(method: &ImplItemFn) -> syn::Result<()> {
    if method.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "mapped controller methods must be async",
        ));
    }
    let Some(FnArg::Receiver(receiver)) = method.sig.inputs.first() else {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "mapped controller methods must use `&self`",
        ));
    };
    if receiver.reference.is_none() || receiver.mutability.is_some() || receiver.colon_token.is_some() {
        return Err(syn::Error::new_spanned(
            receiver,
            "mapped controller methods must use `&self`",
        ));
    }
    if !method.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &method.sig.generics,
            "mapped controller methods cannot be generic",
        ));
    }
    Ok(())
}

/// Generates an Axum handler that forwards extracted arguments to the method.
fn make_handler(method: &ImplItemFn, handler: &syn::Ident, instance: &syn::Ident, self_ty: &Type) -> TokenStream {
    let method_name = &method.sig.ident;
    let mut params = Vec::new();
    let mut args = Vec::new();
    for (index, input) in method.sig.inputs.iter().skip(1).enumerate() {
        let FnArg::Typed(arg) = input else { continue };
        let ident = format_ident!("__arg_{index}");
        let ty = &arg.ty;
        params.push(quote! { #ident: #ty });
        args.push(ident);
    }
    quote! {
        async fn #handler(
            ::axum::Extension(__instance): ::axum::Extension<#instance<#self_ty>>,
            #(#params),*
        ) -> impl ::axum::response::IntoResponse + use<> {
            __instance.0.#method_name(#(#args),*).await
        }
    }
}

/// Converts a validated strategy name into its generated `RouteKind`
/// expression.
fn route_kind(kind: &str) -> TokenStream {
    match kind {
        "sse" => quote! { ::qubit_web::RouteKind::Sse },
        "ws" => quote! { ::qubit_web::RouteKind::WebSocket },
        _ => quote! { ::qubit_web::RouteKind::Short },
    }
}

/// Creates the Axum method-filter expression for a validated method name.
fn method_filter(method: &str) -> TokenStream {
    let name = format_ident!("{}", method);
    quote! { ::axum::routing::MethodFilter::#name }
}
