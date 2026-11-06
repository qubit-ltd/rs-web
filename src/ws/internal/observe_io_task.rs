// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Safe panic diagnostics for WebSocket I/O tasks.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::resume_unwind;

use futures_util::FutureExt;

/// Observes an I/O task panic, records a safe diagnostic, and resumes
/// unwinding.
///
/// The task kind must be a fixed direction label such as `reader` or `writer`.
/// Panic payloads are deliberately not inspected or recorded.
///
/// # Parameters
///
/// * `task_kind` - Fixed I/O direction label attached to the diagnostic.
/// * `future` - I/O task future whose panic should remain visible to its
///   joiner.
///
/// # Type Parameters
///
/// * `F` - Future that performs the I/O task and returns `()`.
pub(in crate::ws) async fn observe_io_task<F>(task_kind: &'static str, future: F)
where
    F: Future<Output = ()>,
{
    if let Err(payload) = AssertUnwindSafe(future).catch_unwind().await {
        tracing::warn!(
            target: "qubit_web::ws",
            event = "websocket_task_panic",
            task_kind,
            "websocket I/O task panicked"
        );
        resume_unwind(payload);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use tokio::sync::Notify;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::fmt::MakeWriter;

    use super::observe_io_task;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Clone, Default)]
    struct SharedLog {
        bytes: Arc<Mutex<Vec<u8>>>,
        written: Arc<Notify>,
    }

    struct LogWriter(SharedLog);

    impl Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.bytes.lock().unwrap().extend_from_slice(bytes);
            self.0.written.notify_one();
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedLog {
        type Writer = LogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            LogWriter(self.clone())
        }
    }

    impl SharedLog {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.bytes.lock().unwrap()).into_owned()
        }
    }

    fn subscriber(log: SharedLog) -> tracing::Dispatch {
        tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::WARN)
                .with_target(true)
                .with_ansi(false)
                .with_writer(log)
                .finish(),
        )
    }

    #[tokio::test]
    async fn test_normal_completion_and_abort_do_not_report_panics() {
        let log = SharedLog::default();
        let dispatch = subscriber(log.clone());
        async {
            observe_io_task("reader", async {}).await;
            let (polled_tx, polled_rx) = tokio::sync::oneshot::channel();
            let aborted = tokio::spawn(observe_io_task("writer", async move {
                let _ = polled_tx.send(());
                std::future::pending::<()>().await;
            }));
            polled_rx.await.expect("pending future should be polled");
            aborted.abort();
            let result = aborted.await;
            assert!(result.unwrap_err().is_cancelled());
        }
        .with_subscriber(dispatch)
        .await;
        assert!(!log.contents().contains("websocket_task_panic"));
    }

    #[tokio::test]
    async fn test_panic_is_logged_safely_and_remains_a_panic_join_error() {
        const SENTINEL: &str = "private-ws-panic-sentinel";
        for task_kind in ["reader", "writer"] {
            let log = SharedLog::default();
            let dispatch = subscriber(log.clone());
            let dropped = Arc::new(AtomicBool::new(false));
            let task_dropped = dropped.clone();
            let task = tokio::spawn(
                observe_io_task(task_kind, async move {
                    let _guard = DropFlag(task_dropped);
                    panic!("{SENTINEL}");
                })
                .with_subscriber(dispatch),
            );
            let error = task.await.unwrap_err();
            assert!(error.is_panic());
            assert!(dropped.load(Ordering::SeqCst), "task resources should unwind");
            let output = log.contents();
            assert_eq!(output.matches("websocket_task_panic").count(), 1, "{output}");
            assert!(output.contains("qubit_web::ws"), "{output}");
            assert!(output.contains(&format!("task_kind=\"{task_kind}\"")), "{output}");
            assert!(!output.contains(SENTINEL), "{output}");
        }
    }

    #[tokio::test]
    async fn test_dropped_join_handle_does_not_cancel_observation() {
        let log = SharedLog::default();
        let dispatch = subscriber(log.clone());
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(
            async move {
                let _ = release_rx.await;
                observe_io_task("reader", async move {
                    let _guard = DropFlag(task_dropped);
                    panic!("private-ws-panic-sentinel");
                })
                .await;
            }
            .with_subscriber(dispatch),
        );
        drop(task);
        release_tx.send(()).unwrap();
        // Yield until the synchronous log writer has received the event.
        if !log.contents().contains("websocket_task_panic") {
            log.written.notified().await;
        }
        let output = log.contents();
        assert_eq!(output.matches("websocket_task_panic").count(), 1, "{output}");
        assert!(!output.contains("private-ws-panic-sentinel"), "{output}");
        assert!(dropped.load(Ordering::SeqCst), "task resources should unwind");
    }
}
