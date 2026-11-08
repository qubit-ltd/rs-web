# Trailers budget investigation (2026-11-08)

## Scope and environment

- Baseline HEAD: `320803bb1e3396e803d40acce83d738a41400373`.
- Toolchain: `rustc 1.94.0 (4a4ef493e3a1488c6e321570238084b38948f6db)`, host `x86_64-unknown-linux-gnu`.
- Locked dependencies: Hyper `1.12.0`, h2 `0.4.20`.
- Focused command: `cargo test --locked --manifest-path /data/working/qubit/rust-common/.worktrees/rs-web/codex-web-regressions-20261108/Cargo.toml --test stream_admission_trailers_tests -- --nocapture` (exit 0; 5 passed, 0 failed).
- Each network request used at most 20 KiB of trailer value data. Synthetic cases used at most 1 MiB total trailers and eight concurrent bodies. Each async operation had a 10-second deadline; no larger or unbounded load was attempted.

## Locked transport behavior

For HTTP/1, `WebServer` forwards `http1_max_buffer_bytes` to Hyper's `http1::Builder::max_buf_size`. It bounds the parser read buffer; it is not a cumulative trailer budget independent of parser behavior. Hyper can accept trailers delivered across reads under the tested 16 KiB buffer, but the tested 20 KiB trailer block surfaced as a body read error to `StreamAdmissionLayer`.

For HTTP/2, Hyper 1.12.0's default receive-side `max_header_list_size` is 16 KiB (`proto/h2/server.rs::DEFAULT_SETTINGS_MAX_HEADER_LIST_SIZE`). `WebServer` forwards an explicit `http2_max_header_list_bytes` to Hyper's builder. h2 0.4.20's `frame/headers.rs::HeaderBlock::load` measures each decoded field as name bytes + value bytes + 32 bytes. Once a decoded block reaches the configured cap it marks the block over-size and stops appending subsequent fields. The separate abuse ceiling is four times the configured size; exceeding it follows h2's connection-level abuse error path. The request trailer path in `proto/streams/streams.rs` dispatches to `recv_trailers` without separately converting `is_over_size` into a request rejection.

This means an over-budget HTTP/2 trailer block can be delivered to the application with the oversized field omitted. The configured cap bounds retained decoded fields, but does not guarantee that the request is rejected or that the complete trailer map is preserved. Applications that require a trailer field must validate that it is present before relying on it.

## Bounded observations

The persistent target `tests/stream_admission_trailers_tests.rs` records both actual wire behavior and synthetic `Body` behavior:

- HTTP/1 chunked requests with an 8 KiB `x-pad` trailer reached the handler and preserved all 8,192 value bytes with default settings and with `http1_max_buffer_bytes = 16 KiB`.
- With `http1_max_buffer_bytes = 16 KiB`, a 20 KiB trailer request did not enter the handler. The observed response was HTTP 400 `body_read_failed`, produced when Hyper's body parser error reached stream admission. This is an application response to a body read failure, not evidence that Hyper returned a protocol-level HTTP status.
- HTTP/2 requests with an 8 KiB `x-pad` trailer reached the handler and preserved all 8,192 value bytes both with the default setting and with an explicit 16 KiB header-list limit.
- With the explicit 16 KiB HTTP/2 limit, a 20 KiB trailer block reached the handler and the application returned HTTP 200, but the decoded trailer map did not contain `x-pad` (zero observed value bytes). A subsequent request on a fresh connection preserved a 32-byte trailer and returned HTTP 200. The test does not describe the oversized block as a 400/413 or as a protocol rejection.
- Synthetic bodies cover trailers without DATA, repeated values, multiple trailer frames, a 1-byte DATA budget, and eight concurrent requests with 64 KiB trailers each (512 KiB total). These establish aggregation and preservation behavior for application-supplied Body frames; they do not stand in for network parser behavior.
- A controlled body error and cancellation while reading are followed by successful requests at capacity one, verifying permit recovery.

## Decision

WEB-R03 is complete as an evidence-gated investigation. The locked HTTP/2 decoder retains decoded trailer fields only up to its configured header-list cap, but over-budget fields may be silently omitted while the request continues. HTTP/1's configured parser-buffer limit produces the observed body-read failure for the tested oversized trailer shape. The experiments found no evidence that retained network trailer fields bypass the applicable bounds, and they do not justify a new public trailers-budget API or a production change in this task.

The observable HTTP/2 behavior is still relevant to applications: required trailer fields must be checked for presence. The test and this report preserve that behavior for future dependency upgrades. The investigation did not test alternate operating systems, a remote client, requests above 20 KiB, or HTTP/2 abuse-ceiling inputs; conclusions beyond the recorded shapes remain unverified.
