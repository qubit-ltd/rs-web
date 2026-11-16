// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
mod controller_method;
mod controller_model;
pub(super) mod generate_controller;
mod mapping_args;
pub(super) mod parse_controller;
mod pending_method;
mod pending_route;
mod resolve_self_type;
mod route;
mod route_args;
mod runtime_path;
pub(super) mod validate_controller;

pub(in crate::controller) use controller_model::ControllerModel;
pub(super) use mapping_args::MappingArgs;
pub(super) use resolve_self_type::resolve_self_type;
pub(super) use route::Route;
pub(super) use route_args::RouteArgs;
pub(super) use runtime_path::runtime_path as resolve_runtime_path;
