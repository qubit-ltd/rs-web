# Qubit Web

[![Rust CI](https://github.com/qubit-ltd/rs-web/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-web/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-web/coverage-badge.json)](https://qubit-ltd.github.io/rs-web/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-web.svg?color=blue)](https://crates.io/crates/qubit-web)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Qubit Web is intended to provide reusable server-side HTTP, HTTPS, and
WebSocket infrastructure for Rust services built on Axum. It complements
the client-side [`qubit-http`](https://github.com/qubit-ltd/rs-http) crate.

## Status

This repository is an initial scaffold. It has no server implementation or
stable public API yet. The design is under review in [`doc/`](doc/).

## Intended Scope

The planned crate serves HTTP, HTTPS, and WebSocket endpoints. Applications
own their routes, authentication decisions, persistence, agent workflows,
and application protocols.

## Installation

The crate is not published yet. The initial library builds locally with Rust
1.94 or newer; it does not expose an application-facing server API yet.

## Testing

```bash
cargo test
cargo test --all-features
./.infra/bin/ci-check.sh
./.infra/bin/coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please keep public API documentation and tests
current, and run `./.infra/bin/align-ci.sh` and `./.infra/bin/ci-check.sh`
before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-web](https://github.com/qubit-ltd/rs-web)
