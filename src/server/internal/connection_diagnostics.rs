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

use tokio::task::JoinError;

/// Records a panic observed while reaping an owned connection task.
///
/// # Parameters
///
/// * `result` - Result returned by joining the completed task.
/// * `task_kind` - Stable label identifying the managed task category.
pub(super) fn record_task_result(result: Result<(), JoinError>, task_kind: &'static str) {
    if let Err(error) = result
        && error.is_panic()
    {
        tracing::warn!(
            target: "qubit_web::transport",
            event = "connection_task_panic",
            task_kind,
            "connection task panicked"
        );
    }
}

/// Counters for sampled connection failures in one `WebServer::serve` call.
///
/// Each failure class has an independent saturating counter. Only powers of
/// two are emitted, which bounds log volume without retaining per-connection
/// state.
#[derive(Default)]
pub(super) struct ConnectionDiagnostics {
    /// Counts TLS handshake deadline expirations for power-of-two sampling.
    #[cfg(feature = "tls")]
    tls_handshake_timeouts: AtomicU64,
    /// Counts TLS acceptor failures for power-of-two sampling.
    #[cfg(feature = "tls")]
    tls_handshake_failures: AtomicU64,
    /// Counts HTTP connection failures for power-of-two sampling.
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
/// Records a sampled connection failure diagnostic.
///
/// # Parameters
///
/// * `counter` - Failure counter whose sample is considered.
/// * `failure_kind` - Stable label attached to a sampled diagnostic event.
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
/// Increments a failure counter and returns periodic sample numbers.
///
/// # Parameters
///
/// * `counter` - Atomic counter used to select periodic diagnostic samples.
fn next_sample(counter: &AtomicU64) -> Option<u64> {
    let previous = counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_add(1))
        .ok()?;
    let count = previous + 1;
    count.is_power_of_two().then_some(count)
}

#[cfg(test)]
pub(super) mod tests {
    use std::io;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;
    use std::thread;

    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::fmt::MakeWriter;

    use super::ConnectionDiagnostics;
    use super::next_sample;
    use super::record_task_result;

    #[derive(Clone, Default)]
    pub(in super::super) struct SharedLog(Arc<Mutex<Vec<u8>>>);

    pub(in super::super) struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedLog {
        type Writer = LogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            LogWriter(Arc::clone(&self.0))
        }
    }

    impl SharedLog {
        pub(in super::super) fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    pub(in super::super) fn diagnostic_subscriber(log: &SharedLog) -> tracing::Dispatch {
        tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_target(true)
                .with_ansi(false)
                .with_writer(log.clone())
                .finish(),
        )
    }

    #[tokio::test]
    async fn test_task_result_reports_only_private_safe_panics() {
        const PANIC_PAYLOAD: &str = "private-panic-sentinel";
        let log = SharedLog::default();
        let dispatch = diagnostic_subscriber(&log);
        async {
            let panicked = tokio::spawn(async { panic!("{PANIC_PAYLOAD}") });
            let panic_result = panicked.await;
            record_task_result(panic_result, "hyper");

            let successful = tokio::spawn(async {}).await;
            record_task_result(successful, "transport");

            let aborted = tokio::spawn(std::future::pending::<()>());
            aborted.abort();
            let abort_result = aborted.await;
            record_task_result(abort_result, "hyper");
        }
        .with_subscriber(dispatch)
        .await;

        let output = log.contents();
        assert_eq!(output.matches("connection_task_panic").count(), 1, "{output}");
        assert!(output.contains("task_kind=\"hyper\""), "{output}");
        assert!(output.contains("connection task panicked"), "{output}");
        assert!(!output.contains(PANIC_PAYLOAD), "{output}");
    }

    #[test]
    fn test_samples_only_powers_of_two_and_saturates() {
        let counter = AtomicU64::new(0);
        let samples: Vec<_> = (0..=9).filter_map(|_| next_sample(&counter)).collect();
        assert_eq!(samples, [1, 2, 4, 8]);

        counter.store(u64::MAX - 1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(next_sample(&counter), None);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), u64::MAX);
        assert_eq!(next_sample(&counter), None);
    }

    #[test]
    fn test_counters_are_independent_per_kind_and_instance() {
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
    fn test_concurrent_counter_updates_are_exact() {
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
