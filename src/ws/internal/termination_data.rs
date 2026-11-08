// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::ws::WsTermination;

/// Mutable first-cause state guarded by `TerminationState`'s mutex.
#[derive(Default)]
pub(in crate::ws) struct TerminationData {
    /// First observed termination candidate.
    pub(in crate::ws) cause: Option<WsTermination>,
    /// Whether the final snapshot has been published.
    pub(in crate::ws) finalized: bool,
}

/// Records the first cause until the state is finalized.
///
/// # Parameters
///
/// * `data` - Mutable state guarded by the owning mutex.
/// * `cause` - Candidate cause to store if no cause has been recorded.
pub(in crate::ws) fn record_data(data: &mut TerminationData, cause: WsTermination) {
    if !data.finalized && data.cause.is_none() {
        data.cause = Some(cause);
    }
}

/// Freezes the state, supplying the I/O-ended fallback when necessary.
///
/// # Parameters
///
/// * `data` - Mutable state guarded by the owning mutex.
pub(in crate::ws) fn finalize_data(data: &mut TerminationData) {
    data.cause.get_or_insert(WsTermination::IoEnded);
    data.finalized = true;
}

/// Returns a cause only after finalization.
///
/// # Parameters
///
/// * `data` - State to inspect.
pub(in crate::ws) fn snapshot_data(data: &TerminationData) -> Option<WsTermination> {
    if data.finalized { data.cause } else { None }
}
