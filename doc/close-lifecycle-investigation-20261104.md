# WebSocket Close lifecycle investigation

Date: 2026-11-04 UTC
Target baseline: `3adf93dca8cd2218b8e9ce665d0a9eaca39c1446`
Lockfile versions used: Axum 0.8.9, Tokio 1.54.1, Tungstenite 0.29.0, Tokio-Tungstenite 0.29.0.

## Experiment

Command:

```sh
cargo test -p qubit-web --locked --features ws --test websocket_tests close_only -- --nocapture
```

The harness used a raw TCP WebSocket peer, a 60-second idle timeout, a 100-millisecond server shutdown timeout, a one-second observation window, and a three-second test limit. It read and checked a custom Close frame (code `4001`, reason `close-only probe`), kept the TCP connection open, and sent no acknowledgement. A oneshot reported when the handler's `closed()` future completed. The comparator ran the same non-acknowledging peer against explicit `WsSession::close()`.

Before the repair, both tests were discovered and executed. The Close-only case did not complete within one second and retained both managed counts at `(active_connections=1, active_sessions=1)`. The explicit `close()` comparator sent a Close and completed within its 100-millisecond shutdown budget, releasing both counts to zero. This distinguishes a queued application Close from the existing explicit shutdown path and confirms the lifecycle gap.

The initial harness build attempt failed because its Axum router state contained a non-Clone oneshot sender. The test state was changed to the repository's `Arc<Mutex<Option<Sender>>>` pattern and the experiment was rerun successfully; this was a harness compile failure, not a runtime observation.

## Repair and regression evidence

When the writer dequeues an application Close, it now closes the outbound queue, establishes the existing shared absolute close deadline, cancels the session token so the reader enters its bounded close phase, sends the original Close frame, and waits for acknowledgement only until that deadline. It finishes the in-flight queue accounting once and exits without sending a default Close. Pending messages behind Close are discarded and subsequent sends are rejected.

After the repair, the same command discovered and executed all three Close-only tests successfully in 0.10 seconds. The no-ack peer observed the exact custom code and reason, then the handler completed and both managed counts returned to zero within the one-second bound while the peer remained open until the observation finished. The peer-ack case and explicit-close comparator also passed.

The writer unit regression uses a recording sink and queues custom Close followed by Text before starting the writer. With no acknowledgement, it verifies only the original Close was sent, code/reason were preserved, pending and in-flight queue accounting returned to zero, late sends return `Closed`, and both session registration and policy permit were released. Existing deterministic `FailingSink` coverage continues to exercise readiness, send, and flush failures and asserts one redacted diagnostic, closed/empty queue accounting, and resource release.

## Contract

`try_send(Message::Close(...))` still means enqueue only. The close timeout starts when the writer begins processing that queued Close, not when it is enqueued. Once processing begins, future sends return `WsSendError::Closed`; queued messages behind the Close may be discarded. The submitted Close code and reason are retained. Applications that need a business message delivered must queue it before Close. Explicit `close()` remains idempotent and bounded by the session/server shutdown deadline.
