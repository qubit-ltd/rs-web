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
use syn::Attribute;
use syn::Expr;
use syn::ExprLit;
use syn::FnArg;
use syn::GenericArgument;
use syn::ImplItemFn;
use syn::Lit;
use syn::LitStr;
use syn::Meta;
use syn::PathArguments;
use syn::Token;
use syn::Type;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;

mod internal;
use internal::Route;
use internal::resolve_self_type;

/// Expands a controller attribute into its implementation and route definition.
///
/// Invalid input is emitted as a compile-time diagnostic instead of panicking.
///
/// # Parameters
///
/// - `args`: Tokens supplied to the controller attribute.
/// - `input`: Implementation block to expand.
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
///
/// # Parameters
///
/// - `args`: Tokens supplied to the controller attribute.
/// - `input`: Implementation block to parse and expand.
fn expand_inner(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    let mut model = internal::parse_controller::parse(args, input)?;
    internal::validate_controller::validate(&mut model)?;
    internal::generate_controller::generate(model)
}

/// Returns the state type when `ty` is an Axum `State<T>` extractor.
///
/// # Parameters
///
/// - `ty`: Type to inspect for an Axum `State<T>` extractor.
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
///
/// # Parameters
///
/// - `attrs`: Attributes whose effective cfg predicates are collected.
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
///
/// # Parameters
///
/// - `condition`: Current condition guarding nested attributes.
/// - `nested_attrs`: Nested attributes to translate.
/// - `predicates`: Output list receiving equivalent predicates.
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
///
/// # Parameters
///
/// - `predicates`: Predicates to combine into a cfg attribute.
fn cfg_attribute(predicates: &[TokenStream]) -> TokenStream {
    if predicates.is_empty() {
        TokenStream::new()
    } else {
        quote! { #[cfg(all(#(#predicates),*))] }
    }
}

/// Returns a single predicate equivalent to the method's effective cfg set.
///
/// # Parameters
///
/// - `predicates`: Predicates to combine into one condition.
fn cfg_condition(predicates: &[TokenStream]) -> TokenStream {
    quote! { all(#(#predicates),*) }
}

/// Combines the conditions under which two route declarations overlap.
///
/// # Parameters
///
/// - `left`: Predicates from the first route.
/// - `right`: Predicates from the second route.
fn combine_conditions(left: &[TokenStream], right: &[TokenStream]) -> Vec<TokenStream> {
    vec![cfg_condition(left), cfg_condition(right)]
}

/// Detects route-mapping attributes nested inside `cfg_attr` declarations.
///
/// # Parameters
///
/// - `attr`: Attribute to inspect for nested route mappings.
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
///
/// # Parameters
///
/// - `meta`: Nested metadata to inspect.
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
///
/// # Parameters
///
/// - `name`: Attribute name to map to an HTTP method.
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

/// Checks that WebSocket routes use only the GET method.
///
/// # Parameters
///
/// - `methods`: HTTP methods declared for the route.
/// - `kind`: Route strategy name.
/// - `span`: Source span for a validation error.
fn validate_websocket_methods(methods: &[String], kind: &str, span: Span) -> syn::Result<()> {
    if kind == "ws" && methods.iter().any(|method| method != "GET") {
        return Err(syn::Error::new(span, "WebSocket route kind requires GET"));
    }
    Ok(())
}

/// Validates route options and returns methods plus the selected strategy.
///
/// `allow_method` is true only for the multi-method `route` attribute.
///
/// # Parameters
///
/// - `options`: Route options to validate.
/// - `allow_method`: Whether the `method` option is accepted.
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
///
/// # Parameters
///
/// - `prefix`: Controller path prefix to join with the child path.
/// - `child`: Literal child path.
/// - `methods`: HTTP methods served by the route.
/// - `kind`: Route strategy name.
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
///
/// # Parameters
///
/// - `_value`: Unused path value retained by this helper's interface.
fn child_span(_value: &str) -> Span {
    Span::call_site()
}

/// Checks absolute-path syntax and rejects query or fragment components.
///
/// Prefixes cannot end in a slash except for `/`; child paths may be empty.
///
/// # Parameters
///
/// - `path`: Path string to validate.
/// - `prefix`: Whether the path is a controller prefix.
/// - `span`: Source span for a validation error.
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
    if path
        .split('/')
        .any(|segment| segment.starts_with(':') || segment.starts_with('*'))
    {
        return Err(syn::Error::new(
            span,
            "legacy route parameters are not supported; use `{name}` for parameters and `{*name}` for wildcards",
        ));
    }
    if !prefix {
        validate_template(path, span)?;
    }
    Ok(())
}

/// Validates path parameter segments, uniqueness, and final wildcard placement.
///
/// # Parameters
///
/// - `path`: Route template to validate.
/// - `span`: Source span for a validation error.
fn validate_template(path: &str, span: Span) -> syn::Result<()> {
    let mut names = HashSet::new();
    let mut non_wildcard_parameters = 0;
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
        if !wildcard {
            non_wildcard_parameters += 1;
            if non_wildcard_parameters > 25 {
                return Err(syn::Error::new(
                    span,
                    "route path supports at most 25 non-wildcard parameters",
                ));
            }
        }
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
///
/// # Parameters
///
/// - `method`: Method declaration to check against the handler contract.
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
    let immutable_reference =
        matches!(&receiver.kind, syn::ReceiverKind::Reference(_, _, None)) && receiver.mutability.is_none();
    if !immutable_reference {
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
    for input in method.sig.inputs.iter().skip(1) {
        let FnArg::Typed(argument) = input else { continue };
        for attr in &argument.attrs {
            if !effective_cfg(std::slice::from_ref(attr))?.is_empty() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "conditional parameters are not supported; use mutually exclusive method-level cfg attributes",
                ));
            }
        }
    }
    Ok(())
}

/// Generates an Axum handler that forwards extracted arguments to the method.
///
/// # Parameters
///
/// - `method`: Source method to wrap as an Axum handler.
/// - `handler`: Generated handler identifier.
/// - `instance_name`: Generated controller instance identifier.
/// - `self_ty`: Concrete controller type.
/// - `axum`: Token stream naming the consumer-visible Axum path.
fn make_handler(
    method: &ImplItemFn,
    handler: &syn::Ident,
    instance: &syn::Ident,
    self_ty: &Type,
    axum: &TokenStream,
) -> TokenStream {
    let method_name = &method.sig.ident;
    let mut params = Vec::new();
    let mut args = Vec::new();
    for (index, input) in method.sig.inputs.iter().skip(1).enumerate() {
        let FnArg::Typed(arg) = input else { continue };
        let ident = format_ident!("__arg_{index}");
        let ty = resolve_self_type(&arg.ty, self_ty);
        params.push(quote! { #ident: #ty });
        args.push(ident);
    }
    quote! {
        async fn #handler(
            #axum::Extension(__instance): #axum::Extension<#instance<#self_ty>>,
            #(#params),*
        ) -> #axum::response::Response {
            #axum::response::IntoResponse::into_response(
                __instance.0.#method_name(#(#args),*).await
            )
        }
    }
}

/// Converts a validated strategy name into its generated `RouteKind`
/// expression.
///
/// # Parameters
///
/// - `kind`: Validated route strategy name.
/// - `runtime`: Token stream naming the runtime crate path.
fn route_kind(kind: &str, runtime: &TokenStream) -> TokenStream {
    match kind {
        "sse" => quote! { #runtime::RouteKind::Sse },
        "ws" => quote! { #runtime::RouteKind::WebSocket },
        _ => quote! { #runtime::RouteKind::Short },
    }
}

/// Creates the Axum method-filter expression for a validated method name.
///
/// # Parameters
///
/// - `method`: HTTP method name to convert into an Axum filter.
/// - `axum`: Token stream naming the consumer-visible Axum path.
fn method_filter(method: &str, axum: &TokenStream) -> TokenStream {
    let name = format_ident!("{}", method);
    quote! { #axum::routing::MethodFilter::#name }
}

#[cfg(test)]
mod tests {
    use syn::ImplItemFn;

    use super::validate_method;

    #[test]
    fn test_validate_method_preserves_receiver_contract() {
        let shared: ImplItemFn = syn::parse_quote!(
            async fn route(&self) {}
        );
        assert!(validate_method(&shared).is_ok());

        let attributed: ImplItemFn = syn::parse_quote!(
            async fn route(&self, #[cfg_attr(all(), allow(unused_variables))] _: u8) {}
        );
        assert!(validate_method(&attributed).is_ok());

        let invalid: [ImplItemFn; 4] = [
            syn::parse_quote!(
                async fn route(&mut self) {}
            ),
            syn::parse_quote!(
                async fn route(mut self) {}
            ),
            syn::parse_quote!(
                async fn route(self) {}
            ),
            syn::parse_quote!(
                async fn route(self: Box<Self>) {}
            ),
        ];
        for method in &invalid {
            let error = validate_method(method).expect_err("only immutable `&self` is supported");
            assert_eq!(error.to_string(), "mapped controller methods must use `&self`",);
        }
    }

    #[test]
    fn test_validate_method_rejects_conditional_parameter_attributes() {
        let cfg: ImplItemFn = syn::parse_quote!(
            async fn route(&self, #[cfg(feature = "extra")] _: u8) {}
        );
        let error = validate_method(&cfg).unwrap_err();
        assert_eq!(
            error.to_string(),
            "conditional parameters are not supported; use mutually exclusive method-level cfg attributes"
        );

        let cfg_attr: ImplItemFn = syn::parse_quote!(
            async fn route(&self, #[cfg_attr(feature = "extra", cfg(any()))] _: u8) {}
        );
        assert_eq!(validate_method(&cfg_attr).unwrap_err().to_string(), error.to_string());
    }
}
