// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sampled, privacy-safe connection failure diagnostics for one server run.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Counters for sampled connection failures in one `WebServer::serve` call.
///
/// Each failure class has an independent saturating counter. Only powers of
/// two are emitted, which bounds log volume without retaining per-connection
/// state.
#[derive(Default)]
pub(super) struct ConnectionDiagnostics {
    #[cfg(feature = "tls")]
    tls_handshake_timeouts: AtomicU64,
    #[cfg(feature = "tls")]
    tls_handshake_failures: AtomicU64,
    http_connection_failures: AtomicU64,
}

impl ConnectionDiagnostics {
    /// Records a TLS handshake deadline expiration.
    #[cfg(feature = "tls")]
    pub(super) fn record_tls_handshake_timeout(&self) {
        record(&self.tls_handshake_timeouts, "tls_handshake_timeout");
    }

    /// Records a TLS handshake rejected by the TLS acceptor.
    #[cfg(feature = "tls")]
    pub(super) fn record_tls_handshake_failure(&self) {
        record(&self.tls_handshake_failures, "tls_handshake_failed");
    }

    /// Records an HTTP connection future failure.
    pub(super) fn record_http_connection_failure(&self) {
        record(&self.http_connection_failures, "http_connection_failed");
    }
}

fn record(counter: &AtomicU64, failure_kind: &'static str) {
    if let Some(failure_count) = next_sample(counter) {
        tracing::debug!(
            target: "qubit_web::transport",
            failure_kind,
            failure_count,
            sampling = "powers_of_two",
            "connection failure"
        );
    }
}

fn next_sample(counter: &AtomicU64) -> Option<u64> {
    let previous = counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_add(1))
        .ok()?;
    let count = previous + 1;
    count.is_power_of_two().then_some(count)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicU64;
    use std::thread;

    use super::ConnectionDiagnostics;
    use super::next_sample;

    #[test]
    fn samples_only_powers_of_two_and_saturates() {
        let counter = AtomicU64::new(0);
        let samples: Vec<_> = (0..=9).filter_map(|_| next_sample(&counter)).collect();
        assert_eq!(samples, [1, 2, 4, 8]);

        counter.store(u64::MAX - 1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(next_sample(&counter), None);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), u64::MAX);
        assert_eq!(next_sample(&counter), None);
    }

    #[test]
    fn counters_are_independent_per_kind_and_instance() {
        let first = ConnectionDiagnostics::default();
        let second = ConnectionDiagnostics::default();
        first.record_http_connection_failure();
        first.record_http_connection_failure();
        second.record_http_connection_failure();
        assert_eq!(
            first
                .http_connection_failures
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        assert_eq!(
            second
                .http_connection_failures
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        #[cfg(feature = "tls")]
        assert_eq!(
            first.tls_handshake_failures.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn concurrent_counter_updates_are_exact() {
        let counter = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(9));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let counter = Arc::clone(&counter);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..1_000 {
                        let _ = next_sample(&counter);
                    }
                })
            })
            .collect();
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 8_000);
    }
}
