# HTTP/2 resource investigation (2026-11-03)

## Question and branch

WEB-004 asked whether a client can leave a second HTTP/2 request header block incomplete while an established SSE response is active, and whether this blocks the SSE response or demonstrates unbounded resource retention.

**Conclusion: branch 1, unconfirmed defect.** In the valid local frame experiment, SSE DATA continued while the second stream's HEADERS block was incomplete. The experiment does not show that the incomplete stream was timed out or bounded by a header deadline; it only shows that the tested shared-connection SSE stream continued to make progress. The locked stack also has finite header-block limits and bounded reset counters. There is no evidence here for a production fix or a new HTTP/2 header timer. No claim is made about memory growth over long durations, arbitrary header patterns, or all clients.

## Locked versions and source findings

- `Cargo.lock`: `hyper` 1.12.0 and `h2` 0.4.20.
- Hyper's `hyper-1.12.0/src/server/conn/http2.rs::Builder::max_concurrent_streams` documents the default of 200 and says it is not a Hyper stability guarantee. `hyper-1.12.0/src/server/conn/http2.rs::Builder::max_header_list_size` forwards an explicit list-size setting to h2.
- h2's `h2-0.4.20/src/codec/framed_read.rs::FrameDecoder` tracks a partial header block and its continuation-frame count. Its default received header-list bound is 16 MiB (`DEFAULT_SETTINGS_MAX_HEADER_LIST_SIZE`), and `calc_max_continuation_frames` derives a finite continuation count from that bound and frame size. `load_frame` retains a HEADERS/PUSH_PROMISE block until END_HEADERS; a partial block therefore has parser state and occupies its HTTP/2 connection while waiting for continuation.
- h2's `h2-0.4.20/src/frame/headers.rs::HeaderBlock::load` enforces that 16 MiB header-list bound and a 4× abuse cutoff. `h2-0.4.20/src/server.rs::Connection::handshake2` applies the builder's max received frame and header-list sizes.
- h2 `h2-0.4.20/src/proto/mod.rs` defines reset limits: 20 pending remote resets, 50 reset streams over one second, and 1,024 locally generated error resets. These are reset-abuse limits, not an incomplete-header deadline.
- rs-web `src/server/web_server.rs::serve` wraps TLS negotiation and HTTP/1 request-header reading in `request_header_timeout`; `serve_connection` configures Hyper's HTTP/1 header timeout. It does not configure an HTTP/2 header-block timer. The idle-connection mechanism is separately activity-aware; see `tests/http_server_tests.rs::test_idle_http2_preface_releases_transport_capacity` and `test_active_http2_handler_survives_transport_idle_timeout`.

## Experiment

`tests/http2_resource_tests.rs` uses a loopback rs-web server and raw HTTP/2 frames in two bounded cases. `investigates_incomplete_h2_headers_with_active_sse` runs the active-SSE experiment:

1. Send the HTTP/2 client preface and an empty SETTINGS frame; read server SETTINGS and send its ACK.
2. Send a complete, valid HPACK GET on stream 1 to `/events`. Confirm SSE response DATA arrives.
3. Send a complete, valid HPACK GET on stream 3 to `/health`; observe response HEADERS and further stream 1 DATA as the normal multiplexing control.
4. Send a valid HEADERS frame on stream 5 with END_STREAM set, END_HEADERS clear, and a valid HPACK indexed `:method GET` fragment. Send no CONTINUATION for an observation interval (the test's next-frame read is bounded to three seconds), and record whether stream 1 DATA arrives.
5. Send a valid CONTINUATION completing the HPACK block, close the client transport, and verify that a new HTTP/1 request can use the server's sole transport permit. Server shutdown is bounded to three seconds and awaited.

`incomplete_h2_header_block_on_idle_connection_waits_for_continuation` is the idle control. It completes and drains a finite `/health` response, sends the same kind of partial HEADERS on stream 3, and observes no server frame or EOF for 300 ms. It then sends a valid CONTINUATION and confirms that the response completes on that connection. This is evidence that the incomplete block remained pending while the connection had no active response stream; it does not establish how long it may remain pending or quantify resources retained.

The active-SSE test reported `SSE_DATA_WHILE_SECOND_HEADER_BLOCK_INCOMPLETE=true`. Both cases passed. A complete second request also succeeded concurrently with SSE. These were local runs; the timing observations are diagnostic, not portable latency or RSS thresholds. Existing idle-preface and active-handler tests also cover transport idle activity, but do not exercise incomplete HTTP/2 headers.

## Interpretation and remaining boundary

The experiment disproves the narrow claim that this valid incomplete block necessarily stops SSE progress on this connection under the tested stack. It does not establish that an attacker cannot consume bounded per-connection memory or keep parser state allocated indefinitely; neither duration-based retention nor RSS was measured. The existing header-list and frame/continuation bounds constrain the data accumulated, but do not impose a wall-clock deadline on a client that pauses between valid frames.

Accordingly, WEB-004 remains **unconfirmed**, rather than “fixed.” WEB-001's per-connection stream budget is useful capacity control, but this experiment does not show that it is necessary to resolve a reproduced incomplete-header/SSE failure. A dedicated HTTP/2 header deadline would need a separate design and evidence about the correct connection-level behavior, especially because one incomplete header block can hold up later frames on that connection.
