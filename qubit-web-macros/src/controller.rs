// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashSet;

use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
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
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::parse2;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;

/// Parsed arguments for a verb-specific controller mapping attribute.
struct MappingArgs {
    /// Literal child path appended to the controller prefix.
    path: LitStr,
    /// Optional route settings accepted after the path.
    options: Punctuated<Meta, Token![,]>,
}

impl Parse for MappingArgs {
    /// Parses the path followed by an optional comma-separated option list.
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let path = input.parse()?;
        let options = if input.is_empty() {
            Punctuated::new()
        } else {
            input.parse::<Token![,]>()?;
            Punctuated::parse_terminated(input)?
        };
        Ok(Self { path, options })
    }
}

/// Parsed arguments for the multi-method `route` attribute.
struct RouteArgs {
    /// Literal child path appended to the controller prefix.
    path: LitStr,
    /// HTTP method and route-kind settings following the path.
    options: Punctuated<Meta, Token![,]>,
}

impl Parse for RouteArgs {
    /// Parses a path and its optional comma-separated route settings.
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let path = input.parse()?;
        let options = if input.is_empty() {
            Punctuated::new()
        } else {
            input.parse::<Token![,]>()?;
            Punctuated::parse_terminated(input)?
        };
        Ok(Self { path, options })
    }
}

/// Normalized route data used to generate controller registrations.
struct Route {
    /// Full path after joining the controller prefix and child path.
    path: String,
    /// Uppercase HTTP methods handled by this route.
    methods: Vec<String>,
    /// Transport strategy name, validated as `short`, `sse`, or `ws`.
    kind: String,
}

/// A controller handler together with its generated route registrations.
struct Method {
    /// Routes declared on the handler method.
    routes: Vec<Route>,
    /// Generated function name used by the router.
    handler: syn::Ident,
}

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
    let mut used_routes = HashSet::new();
    let mut controller_state: Option<Type> = None;
    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else { continue };
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
                routes.push(make_route(&prefix.value(), mapping.path, vec![verb.to_owned()], kind)?);
            } else if name == "route" {
                let route = parse2::<RouteArgs>(attr.meta.require_list()?.tokens.clone())?;
                let (verbs, kind) = parse_options(route.options, true)?;
                if verbs.is_empty() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "route requires at least one method = \"...\"",
                    ));
                }
                routes.push(make_route(&prefix.value(), route.path, verbs, kind)?);
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
            if controller_state
                .as_ref()
                .is_some_and(|existing| quote!(#existing).to_string() != quote!(#state).to_string())
            {
                return Err(syn::Error::new_spanned(
                    &argument.ty,
                    "all State extractors in one Controller must use the same state type",
                ));
            }
            controller_state = Some(state);
        }
        for route in &routes {
            for verb in &route.methods {
                if !used_routes.insert((verb.clone(), route.path.clone())) {
                    return Err(syn::Error::new_spanned(
                        method,
                        format!("duplicate controller route: {verb} {}", route.path),
                    ));
                }
            }
        }
        let handler_name = controller_name.to_string().to_lowercase();
        let handler = format_ident!("__qubit_controller_handler_{}_{}", handler_name, methods.len());
        declarations.push(make_handler(method, &handler, &instance_name, &self_ty));
        methods.push(Method { routes, handler });
    }

    let metadata = methods.iter().flat_map(|method| {
        method.routes.iter().flat_map(|route| {
            route.methods.iter().map(|verb| {
                let path = &route.path;
                let kind = route_kind(&route.kind);
                quote! { ::qubit_web::mvc::RouteMetadata::new(#verb, #path, #kind) }
            })
        })
    });
    let route_count = methods
        .iter()
        .map(|method| method.routes.iter().map(|route| route.methods.len()).sum::<usize>())
        .sum::<usize>();
    let (impl_generics, state_type) = if let Some(state) = controller_state {
        (quote! {}, quote! { #state })
    } else {
        (quote! { <__QubitControllerState> }, quote! { __QubitControllerState })
    };
    let state_type_ref = &state_type;
    let instance_name_ref = &instance_name;
    let registrations = methods.iter().flat_map(|method| {
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
                    let route = route.layer(::qubit_web::diagnostic::DiagnosticLayer::new());
                }
            };
            let filters = route.methods.iter().map(|verb| method_filter(verb));
            let mut filters = filters.collect::<Vec<_>>().into_iter();
            let first = filters.next().expect("route has methods");
            let filter = filters.fold(first, |combined, next| quote! { #combined.or(#next) });
            quote! {
                {
                    let route = ::axum::Router::<#state_type_ref>::new()
                        .route(#path, ::axum::routing::on(#filter, #handler))
                        .layer(::axum::Extension(#instance_name_ref(self.clone())));
                    #strategy_layers
                    router = router.merge(route);
                }
            }
        })
    });
    let item_impl = implementation;
    Ok(quote! {
        #item_impl

        #[doc(hidden)]
        struct #instance_name<__Controller>(::std::sync::Arc<__Controller>);
        impl<__Controller> ::std::clone::Clone for #instance_name<__Controller> {
            fn clone(&self) -> Self { Self(self.0.clone()) }
        }

        #(#declarations)*

        impl #impl_generics ::qubit_web::mvc::ControllerDefinition<#state_type> for #self_ty
        where
            #state_type: Clone + Send + Sync + 'static,
        {
            fn route_metadata() -> &'static [::qubit_web::mvc::RouteMetadata] {
                static __QUBIT_ROUTES: [::qubit_web::mvc::RouteMetadata; #route_count] = [#(#metadata),*];
                &__QUBIT_ROUTES
            }

            fn register(
                self: ::std::sync::Arc<Self>,
                mut router: ::axum::Router<#state_type>,
                limit_state: ::qubit_web::limit::LimitState,
            ) -> ::axum::Router<#state_type> {
                #(#registrations)*
                router
            }
        }
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
