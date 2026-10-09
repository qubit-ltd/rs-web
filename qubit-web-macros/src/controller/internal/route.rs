/// Normalized route data used to generate controller registrations.
pub(in crate::controller) struct Route {
    /// Full path after joining the controller prefix and child path.
    pub(in crate::controller) path: String,
    /// Uppercase HTTP methods handled by this route.
    pub(in crate::controller) methods: Vec<String>,
    /// Transport strategy name, validated as `short`, `sse`, or `ws`.
    pub(in crate::controller) kind: String,
}
