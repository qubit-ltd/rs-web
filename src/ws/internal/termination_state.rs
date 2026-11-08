// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Mutex;
use std::sync::PoisonError;

use super::termination_data::TerminationData;
use super::termination_data::finalize_data;
use super::termination_data::record_data;
use super::termination_data::snapshot_data;
use crate::ws::WsTermination;

/// Thread-safe candidate and finalized termination snapshot for one session.
#[derive(Default)]
pub(in crate::ws) struct TerminationState {
    /// Candidate cause and publication flag.
    data: Mutex<TerminationData>,
}

impl TerminationState {
    /// Records a candidate if no earlier candidate or finalized result exists.
    ///
    /// # Parameters
    ///
    /// * `cause` - Candidate cause to record.
    pub(in crate::ws) fn record(&self, cause: WsTermination) {
        let mut data = self.data.lock().unwrap_or_else(PoisonError::into_inner);
        record_data(&mut data, cause);
    }

    /// Freezes the result, defaulting to `IoEnded` when no cause was recorded.
    pub(in crate::ws) fn finalize(&self) {
        let mut data = self.data.lock().unwrap_or_else(PoisonError::into_inner);
        finalize_data(&mut data);
    }

    /// Returns the immutable result only after finalization.
    pub(in crate::ws) fn snapshot(&self) -> Option<WsTermination> {
        let data = self.data.lock().unwrap_or_else(PoisonError::into_inner);
        snapshot_data(&data)
    }

    /// Returns the current candidate for internal close-frame selection.
    pub(in crate::ws) fn candidate(&self) -> Option<WsTermination> {
        self.data.lock().unwrap_or_else(PoisonError::into_inner).cause
    }
}

#[cfg(test)]
mod tests {
    use super::TerminationState;
    use crate::ws::WsTermination;

    #[test]
    fn test_candidate_is_hidden_until_finalization() {
        let state = TerminationState::default();
        assert_eq!(state.snapshot(), None);
        state.record(WsTermination::IdleTimeout);
        assert_eq!(state.candidate(), Some(WsTermination::IdleTimeout));
        assert_eq!(state.snapshot(), None);
    }

    #[test]
    fn test_first_cause_wins_and_finalized_result_is_immutable() {
        let state = TerminationState::default();
        state.record(WsTermination::ReadError);
        state.record(WsTermination::WriteError);
        state.finalize();
        state.record(WsTermination::ServerShutdown);
        state.finalize();
        assert_eq!(state.snapshot(), Some(WsTermination::ReadError));
    }

    #[test]
    fn test_finalize_without_candidate_uses_io_ended() {
        let state = TerminationState::default();
        state.finalize();
        assert_eq!(state.snapshot(), Some(WsTermination::IoEnded));
    }
}
