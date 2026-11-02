use proc_macro_crate::FoundCrate;
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::quote;

/// Resolves the consumer-visible path to the `qubit-web` runtime crate.
pub(crate) fn runtime_path() -> syn::Result<TokenStream> {
    match proc_macro_crate::crate_name("qubit-web").map_err(|error| {
        syn::Error::new(
            Span::call_site(),
            format!("could not find `qubit-web` dependency: {error}"),
        )
    })? {
        FoundCrate::Itself => Ok(quote! { ::qubit_web }),
        FoundCrate::Name(name) => {
            let ident = if is_keyword(&name) {
                Ident::new_raw(&name, Span::call_site())
            } else {
                Ident::new(&name, Span::call_site())
            };
            Ok(quote! { ::#ident })
        }
    }
}

fn is_keyword(name: &str) -> bool {
    matches!(
        name,
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "union"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "async"
            | "await"
            | "dyn"
            | "gen"
            | "abstract"
            | "become"
            | "box"
            | "do"
            | "final"
            | "macro"
            | "override"
            | "priv"
            | "typeof"
            | "unsized"
            | "virtual"
            | "yield"
            | "try"
    )
}
