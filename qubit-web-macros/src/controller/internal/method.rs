use super::route::Route;

/// A controller handler together with its generated route registrations.
pub(in crate::controller) struct Method {
    /// Routes declared on the handler method.
    pub(in crate::controller) routes: Vec<Route>,
    /// Generated function name used by the router.
    pub(in crate::controller) handler: syn::Ident,
}
