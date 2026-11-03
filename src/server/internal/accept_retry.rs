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
    use std::io;
    use std::time::Duration;

    use tokio::time::Instant;

    use super::super::accept_loop::handle_accept_result;
    use super::super::accept_result::AcceptResult;
    use super::AcceptRetry;

    #[test]
    fn test_accept_transient_error_then_success_keeps_server_running() {
        let mut retry = AcceptRetry::default();
        let before = Instant::now();
        let retry_at =
            match handle_accept_result::<()>(&mut retry, Err(io::Error::from(io::ErrorKind::ConnectionAborted))) {
                AcceptResult::Retry => retry.retry_at().expect("retry state retains its deadline"),
                _ => panic!("transient failure must schedule retry"),
            };
        assert!(retry_at.duration_since(before) >= Duration::from_millis(10));
        assert!(retry_at.duration_since(before) < Duration::from_millis(11));
        assert!(matches!(
            handle_accept_result(&mut retry, Ok(())),
            AcceptResult::Accepted(())
        ));
        assert!(retry.retry_at().is_none());
        let before = Instant::now();
        let retry_at =
            match handle_accept_result::<()>(&mut retry, Err(io::Error::from(io::ErrorKind::ConnectionAborted))) {
                AcceptResult::Retry => retry.retry_at().expect("retry state retains its deadline"),
                _ => panic!("failure after success starts a new retry series"),
            };
        assert!(retry_at.duration_since(before) >= Duration::from_millis(10));
        assert!(retry_at.duration_since(before) < Duration::from_millis(11));
    }

    #[test]
    fn test_accept_success_resets_consecutive_failure_limit() {
        let mut retry = AcceptRetry::default();
        for _ in 0..8 {
            assert!(matches!(
                handle_accept_result::<()>(&mut retry, Err(io::Error::from(io::ErrorKind::ConnectionAborted))),
                AcceptResult::Retry
            ));
        }
        assert!(matches!(
            handle_accept_result(&mut retry, Ok(())),
            AcceptResult::Accepted(())
        ));
        for _ in 0..8 {
            assert!(matches!(
                handle_accept_result::<()>(&mut retry, Err(io::Error::from(io::ErrorKind::ConnectionAborted))),
                AcceptResult::Retry
            ));
        }
        let final_result = handle_accept_result::<()>(
            &mut retry,
            Err(io::Error::new(io::ErrorKind::ConnectionAborted, "last source")),
        );
        let AcceptResult::Terminal(final_error) = final_result else {
            panic!("ninth consecutive failure must terminate")
        };
        assert_eq!(final_error.to_string(), "last source");
    }

    #[test]
    fn test_accept_fatal_error_terminates_without_retry() {
        let mut retry = AcceptRetry::default();
        let result = handle_accept_result::<()>(
            &mut retry,
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "fatal source")),
        );
        let AcceptResult::Terminal(error) = result else {
            panic!("fatal failure must terminate immediately")
        };
        assert_eq!(error.to_string(), "fatal source");
    }

    #[test]
    fn test_accept_backoff_is_exponential_and_capped() {
        let delays: Vec<_> = (1..=8).map(super::accept_retry_delay).collect();
        assert_eq!(delays, [10, 20, 40, 80, 160, 320, 640, 1000].map(Duration::from_millis));
    }
}
