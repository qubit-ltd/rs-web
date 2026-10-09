use syn::LitStr;
use syn::Meta;
use syn::Token;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::punctuated::Punctuated;

/// Parsed arguments for a verb-specific controller mapping attribute.
pub(in crate::controller) struct MappingArgs {
    /// Literal child path appended to the controller prefix.
    pub(in crate::controller) path: LitStr,
    /// Optional route settings accepted after the path.
    pub(in crate::controller) options: Punctuated<Meta, Token![,]>,
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
