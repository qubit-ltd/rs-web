use syn::LitStr;
use syn::Meta;
use syn::Token;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::punctuated::Punctuated;

/// Parsed arguments for the multi-method `route` attribute.
pub(in crate::controller) struct RouteArgs {
    /// Literal child path appended to the controller prefix.
    pub(in crate::controller) path: LitStr,
    /// HTTP method and route-kind settings following the path.
    pub(in crate::controller) options: Punctuated<Meta, Token![,]>,
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
