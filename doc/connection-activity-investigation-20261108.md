# Connection activity lease investigation

## Scope and environment

- Project worktree: `/data/working/qubit/rust-common/.worktrees/rs-web/codex-web-contracts-20261108` (`rs-web`).
- Starting commit: `e255d353594be4d460f1de6a922a26a5d34e1001`.
- Locked versions used by this worktree: Axum 0.8.9, Hyper 1.12.0, and Tokio 1.54.1 (from `Cargo.lock`). No dependency or lockfile changes were made for T3.
- `ActivityLease::drop` decrements the shared `watch::Sender<usize>` count. `retain_response_activity` transfers non-empty responses into `ActivityBody`; before this investigation, `ActivityBody::poll_frame` released the lease on `None`/error and wrapper drop only.

## Local body lifecycle evidence

Baseline command, before adding a matching unit test:

```text
cargo test --locked --lib activity_body
exit 0; 0 tests matched, 48 filtered out
```

The new local test uses a final-frame body whose `is_end_stream()` becomes true immediately after its only successful data frame. With the original implementation, the test failed immediately after that frame: observed activity count was `1`, expected `0`. This demonstrates a body-production lifecycle gap while the wrapper remains retained; it does not by itself demonstrate a network-visible protocol failure.

The implementation now releases the lease after a successful frame when the inner body reports end-of-stream. Frames are still returned unchanged. The local tests check that Pending and an intermediate frame keep the count at 1, final frame/EOF/error/drop bring it to 0, and follow-up polling/drop does not decrement twice. The first run after adding the final-frame regression failed as expected (exit 101; count was 1 instead of 0). The final focused local command passed after the fix and after adding the empty-body EOF assertion:

```text
cargo test --locked --lib activity_body
exit 0; 2 passed, 0 failed, 48 filtered out
```

The final local result includes the empty-body EOF assertion added during the investigation.

## Real transport observations

Both network tests use existing project fixtures and finite handlers. The client reads a complete response while retaining the connection, awaits transport idle close with bounded timeouts, then verifies a fresh HTTP/1 request succeeds with a one-connection server limit.

```text
cargo test --locked --test http_server_tests test_final_frame_response_allows_transport_idle_timeout
exit 0; 1 passed, 0 failed; HTTP/1.1 idle close and capacity recovery observed

cargo test --locked --test http2_resource_tests test_final_frame_response_allows_transport_idle_timeout
exit 0; 1 passed, 0 failed; cleartext HTTP/2 idle close and capacity recovery observed
```

These passing checks establish successful idle timeout and capacity recovery after completed finite responses in the tested local fixtures. They do not establish that the old implementation consistently failed over the network, nor that transport bytes were withheld or any external interoperability defect existed. The local lease count is the direct evidence motivating the small lifecycle repair; network behavior is recorded separately.

## Limitations

- The initial baseline filter selected no tests, so it was not a behavioral baseline until the targeted local regression was added.
- The local pre-fix regression reproduced the lease retention deterministically; the two network checks did not show an externally visible failure.
- This is focused evidence only. Full project CI/coverage checks belong to the integration task.
