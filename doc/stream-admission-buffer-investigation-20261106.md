# Stream admission buffer investigation (2026-11-06)

## Question and conclusion

WEB-001 concerns per-frame backing owners retained by `http_body_util::Collected` until the entire request has been read. A fixed-payload synthetic comparison confirms that owner count scales with frame count in the old collector and stays at one in the incremental-copy loop. A loopback HTTP/1 and HTTP/2 experiment confirms that Hyper delivers fragmented body frames in both protocols and that the admission layer accepts the tested request shapes. The experiments do not establish a remotely exploitable denial-of-service condition or an exact production-process memory multiplier.

## Environment and method

- Source baseline: `d599ad24a0fda2f563b4b744c9bde9cd0446a954` (`d599ad2`); this was the starting commit for both the old and changed worktrees.
- Toolchain: `rustc 1.94.0 (4a4ef493e 2026-03-02)`, `cargo 1.94.0 (85eff7c80 2026-01-15)`.
- Locked body stack: `http-body-util 0.1.5`, `bytes 1.12.1`, `http-body 1.1.0`, Hyper `1.12.0`, h2 `0.4.20`.
- Synthetic comparison: a disposable Rust program outside the repository implemented the old `Limited::collect().to_bytes()` path and the new `Limited::frame()` plus `Vec` copy path. Each generated independent `Bytes::from_owner` backing owners, with an atomic live-owner peak. Each run used 1 MiB total payload, one concurrent request, three repetitions, and 1 MiB, 1 KiB, or 1 byte frames. GNU `/usr/bin/time -v` measured the child process peak RSS. No threshold or allocator-specific guarantee is inferred from this single-host measurement.
- Network comparison: a disposable loopback integration test wrapped the incoming body to count data frames, sent one 1 MiB request for each protocol, and recorded request elapsed time. HTTP/1 used 1 KiB chunked chunks; HTTP/2 used 1 KiB body items. The same temporary fixture ran against the original checkout and the implementation worktree. A second fixture disconnected a partial HTTP/1 upload, then sent a complete request to verify capacity recovery. Both fixtures were removed after recording results.
- Experimental commands: `cargo run --release --manifest-path /tmp/rs-web-buffer-experiment/Cargo.toml -- <old|new> <frame-bytes> 3`, wrapped by `/usr/bin/time -v`; `cargo test --test stream_admission_network_experiment -- --nocapture`; and `cargo test --test stream_admission_network_recovery_experiment -- --nocapture`. The network test fixtures were temporary files with those target names and were deleted after the runs.

## Synthetic results

Each row is one 1 MiB request repeated three times in the same process. Times are elapsed milliseconds for all three requests. RSS is maximum resident set in KiB for the process, not only body storage.

| Algorithm | Frame bytes | Frames/request | Peak live owners | Elapsed ms / 3 requests | Peak RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| old collector | 1,048,576 | 1 | 1 | 1.624 | 4,152 |
| incremental copy | 1,048,576 | 1 | 1 | 4.651 | 4,600 |
| old collector | 1,024 | 1,024 | 1,024 | 3.648 | 4,928 |
| incremental copy | 1,024 | 1,024 | 1 | 1.598 | 3,828 |
| old collector | 1 | 1,048,576 | 1,048,576 | 874.678 | 118,520 |
| incremental copy | 1 | 1,048,576 | 1 | 286.668 | 3,832 |

The one-byte independent-owner case deliberately isolates worst-case frame-object/backing-owner retention. Real transports may coalesce, split, or share backing storage differently. The single-frame timing shows the accepted copy cost in this harness (about 3 ms across three 1 MiB requests); these timings are one sample set and are not portable performance estimates. The owner count is the deterministic resource signal; RSS reflects this allocator and execution environment.

## Loopback network results

| Implementation | Protocol/input shape | Observed incoming DATA frames | Elapsed ms | Result |
| --- | --- | ---: | ---: | --- |
| old collector | HTTP/1, 1 KiB chunked chunks, 1 MiB | 1,032 | 15.744 | HTTP 200, 1,048,576 bytes |
| incremental copy | HTTP/1, 1 KiB chunked chunks, 1 MiB | 1,031 | 19.085 | HTTP 200, 1,048,576 bytes |
| old collector | HTTP/2, 1 KiB body items, 1 MiB | 1,026 | 20.799 | HTTP 200, 1,048,576 bytes |
| incremental copy | HTTP/2, 1 KiB body items, 1 MiB | 1,026 | 21.086 | HTTP 200, 1,048,576 bytes |

These were single local requests with one admitted request at a time. Hyper's observed frame counts differ slightly from the input chunk/item counts, confirming that wire chunks do not map one-for-one to body frames. Elapsed times are individual loopback samples and are too close to treat as a reliable throughput comparison. On both old and new implementations, disconnecting after one byte of a declared 1 KiB HTTP/1 upload was followed by a complete request receiving HTTP 200; the admission capacity was reusable. An initial 1-byte HTTP/1 chunk test exceeded the default 10-second admission deadline (observed at 10.10 seconds), so the bounded network comparison used 1 KiB chunks. That timeout is recorded as an experiment limit, not a code failure.

## Bounded concurrent HTTP/1 loopback load

To add a bounded concurrency check, a disposable integration test ran against the baseline checkout and the implementation worktree. Each batch used fresh loopback HTTP/1 connections, a fixed 64 KiB body per client, 64 chunked 1 KiB data chunks per request, a 1 ms delay between chunks, an 8-admission limit, and a 3-second absolute admission timeout. Batches increased from 2 to 4 to 8 clients, then 10 clients to observe the configured capacity rejection. The test had a four-thread Tokio runtime. No unbounded retry or client loop was used. In each checkout, `cargo test --test stream_admission_concurrent_experiment -- --nocapture` exited 0. The reported frame count is the sum of DATA frames Hyper delivered to the admission body wrapper for that batch; the two capacity-rejected bodies were not polled by the middleware.

| Implementation | Clients | HTTP 200 / admitted | HTTP 503 / capacity rejected | Handler calls | Delivered DATA frames | Batch elapsed ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 2 | 2 | 0 | 2 | 128 | 137.218 |
| baseline | 4 | 4 | 0 | 4 | 256 | 140.365 |
| baseline | 8 | 8 | 0 | 8 | 512 | 151.516 |
| baseline | 10 | 8 | 2 | 8 | 512 | 145.406 |
| incremental copy | 2 | 2 | 0 | 2 | 128 | 137.087 |
| incremental copy | 4 | 4 | 0 | 4 | 256 | 142.002 |
| incremental copy | 8 | 8 | 0 | 8 | 512 | 142.192 |
| incremental copy | 10 | 8 | 2 | 8 | 512 | 143.922 |

Each run completed below the 3-second deadline; no admission timed out. These single samples show the expected capacity boundary and successful accepted request count, but are not a throughput benchmark and do not establish a meaningful performance difference between implementations. After each load run, a separate one-capacity loopback server received a partial HTTP/1 chunked upload whose client connection was dropped, followed by a complete 64 KiB request. Both baseline and incremental-copy runs returned HTTP 200 for the follow-up, called the handler once, and observed 65 DATA frames total (one partial-upload frame plus 64 follow-up frames), showing that the failed partial request released its admission permit.

The temporary integration-test fixtures were removed from both checkouts after the runs. This bounded concurrency matrix covered HTTP/1 only; concurrent HTTP/2 behavior and network-process RSS remain unmeasured. The earlier single-request HTTP/2 result above does not substitute for those measurements.

## Downstream WebSocket interoperability

The repository's `scripts/check-downstream-interop.py` ran against a disposable rs-websocket clone with `--with-wss`. The upstream checkout HEAD was `0329e6dc878da25b93027ca0c2f06ade1f4fb7a5` (the implementation commits); the downstream checkout HEAD was `3c90a4c413b51dabadfd2fbb387c764ad88a915a`. The script verified that the downstream `qubit-web` dependency resolved to `/tmp/rs-web-interop-20261106/Cargo.toml`, and the WSS fixture resolved both `qubit-web` and `qubit-websocket` to local manifests under `/tmp/rs-web-interop-20261106` and `/tmp/rs-websocket-interop-20261106` (Cargo metadata reported `source=None` for each). `web_interop_tests` passed 5/5 and `wss_interop` passed 3/3; the script exited 0. The temporary upstream/downstream checkouts and disposable marker were removed after validation.

## Interpretation and remaining boundary

The deterministic synthetic test supports the stated implementation property: prior data-frame owners are released as the loop advances, while the aggregate buffer remains proportional to payload. The loopback tests confirm both tested network stacks reach the implementation with fragmented frames and preserve request acceptance/recovery. No network-process RSS matrix, load above 10 clients, alternate operating system, or remote-client experiment was performed. In particular, the real-network memory amplification across clients and concurrency remains unquantified; these results do not establish an exploitability or denial-of-service conclusion. The 256 MiB default product remains a concurrent payload estimate, not a buffer-allocation or process-memory ceiling.
