// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::io;
use std::time::Duration;

use tokio::time::Instant;

/// Tracks consecutive accept failures and retains a terminal failure.
#[derive(Default)]
pub(super) struct AcceptRetry {
    failures: u32,
    terminal_error: Option<io::Error>,
    retry_at: Option<Instant>,
}

impl AcceptRetry {
    pub(super) fn record_error(&mut self, error: io::Error) -> Option<Instant> {
        if is_fatal_accept_error(&error) || self.failures == 8 {
            self.terminal_error = Some(error);
            return None;
        }

        self.failures += 1;
        let retry_at = Instant::now() + accept_retry_delay(self.failures);
        self.retry_at = Some(retry_at);
        Some(retry_at)
    }

    pub(super) fn record_success(&mut self) {
        self.failures = 0;
        self.retry_at = None;
    }

    pub(super) fn retry_at(&self) -> Option<Instant> {
        self.retry_at
    }

    #[cfg(test)]
    pub(super) fn consecutive_failures(&self) -> u32 {
        self.failures
    }

    pub(super) fn take_terminal_error(&mut self) -> Option<io::Error> {
        self.terminal_error.take()
    }
}

fn is_fatal_accept_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::InvalidInput
            | io::ErrorKind::PermissionDenied
            | io::ErrorKind::Unsupported
            | io::ErrorKind::NotConnected
    )
}

pub(super) fn accept_retry_delay(retry_number: u32) -> Duration {
    debug_assert!((1..=8).contains(&retry_number));
    Duration::from_millis((10_u64 << (retry_number - 1)).min(1000))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::accept_retry_delay;

    #[test]
    fn test_accept_backoff_is_exponential_and_capped() {
        let delays: Vec<_> = (1..=8).map(accept_retry_delay).collect();
        assert_eq!(delays, [10, 20, 40, 80, 160, 320, 640, 1000].map(Duration::from_millis));
    }
}
