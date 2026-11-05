// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use syn::ExprPath;
use syn::Type;
use syn::TypePath;
use syn::visit_mut::VisitMut;

/// Replaces `Self` references in a copied type with its controller type.
///
/// # Parameters
///
/// - `ty`: Type syntax to copy and resolve.
/// - `self_ty`: Concrete controller type that replaces `Self`.
pub(in crate::controller) fn resolve_self_type(ty: &Type, self_ty: &Type) -> Type {
    let mut resolved = ty.clone();
    ResolveSelfType { self_ty }.visit_type_mut(&mut resolved);
    resolved
}

/// Visitor that substitutes unqualified `Self` paths with the controller type.
struct ResolveSelfType<'a> {
    /// Controller type syntax used to replace `Self` references.
    self_ty: &'a Type,
}

/// Applies the `Self` type substitution while traversing syntax trees.
impl VisitMut for ResolveSelfType<'_> {
    /// Rewrites a type path rooted at `Self`, including qualified associated
    /// types.
    ///
    /// # Parameters
    ///
    /// - `ty`: Mutable type path being visited.
    fn visit_type_path_mut(&mut self, ty: &mut TypePath) {
        if ty.qself.is_none() && ty.path.is_ident("Self") {
            *ty = match self.self_ty.clone() {
                Type::Path(path) => path,
                _ => return,
            };
            return;
        }

        resolve_self_path(&mut ty.qself, &mut ty.path, self.self_ty);
        syn::visit_mut::visit_type_path_mut(self, ty);
    }

    /// Rewrites an expression path rooted at `Self`.
    ///
    /// # Parameters
    ///
    /// - `expr`: Mutable expression path being visited.
    fn visit_expr_path_mut(&mut self, expr: &mut ExprPath) {
        resolve_self_path(&mut expr.qself, &mut expr.path, self.self_ty);
        syn::visit_mut::visit_expr_path_mut(self, expr);
    }
}

/// Rewrites a leading `Self` segment into a qualified path for `self_ty`.
///
/// # Parameters
///
/// - `qself`: Optional qualified-self syntax to populate.
/// - `path`: Path whose leading `Self` segment may be replaced.
/// - `self_ty`: Concrete type used as the qualified-self type.
fn resolve_self_path(qself: &mut Option<syn::QSelf>, path: &mut syn::Path, self_ty: &Type) {
    if qself.is_some() || !path.segments.first().is_some_and(|segment| segment.ident == "Self") {
        return;
    }

    let remaining_segments = std::mem::take(&mut path.segments).into_pairs().skip(1);
    let mut segments = syn::punctuated::Punctuated::new();
    for pair in remaining_segments {
        let (segment, separator) = pair.into_tuple();
        segments.push_value(segment);
        if let Some(separator) = separator {
            segments.push_punct(separator);
        }
    }
    *qself = Some(syn::QSelf {
        lt_token: Default::default(),
        ty: Box::new(self_ty.clone()),
        position: 0,
        as_token: None,
        gt_token: Default::default(),
    });
    path.leading_colon = Some(Default::default());
    path.segments = segments;
}
