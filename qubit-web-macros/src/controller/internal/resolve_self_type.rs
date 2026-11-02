// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use syn::Type;
use syn::TypePath;
use syn::visit_mut::VisitMut;

/// Replaces `Self` references in a copied type with its controller type.
pub(in crate::controller) fn resolve_self_type(ty: &Type, self_ty: &Type) -> Type {
    let mut resolved = ty.clone();
    SelfTypeResolver { self_ty }.visit_type_mut(&mut resolved);
    resolved
}

struct SelfTypeResolver<'a> {
    self_ty: &'a Type,
}

impl VisitMut for SelfTypeResolver<'_> {
    fn visit_type_path_mut(&mut self, ty: &mut TypePath) {
        if ty.qself.is_none() && ty.path.is_ident("Self") {
            *ty = match self.self_ty.clone() {
                Type::Path(path) => path,
                _ => return,
            };
            return;
        }

        if ty.qself.is_none()
            && ty.path.segments.first().is_some_and(|segment| segment.ident == "Self")
        {
            let remaining_segments = std::mem::take(&mut ty.path.segments)
                .into_pairs()
                .skip(1);
            let mut segments = syn::punctuated::Punctuated::new();
            for pair in remaining_segments {
                let (segment, separator) = pair.into_tuple();
                segments.push_value(segment);
                if let Some(separator) = separator {
                    segments.push_punct(separator);
                }
            }
            ty.qself = Some(syn::QSelf {
                lt_token: Default::default(),
                ty: Box::new(self.self_ty.clone()),
                position: 0,
                as_token: None,
                gt_token: Default::default(),
            });
            ty.path.segments = segments;
        }
        syn::visit_mut::visit_type_path_mut(self, ty);
    }
}
