# Qubit Web

[![Rust CI](https://github.com/qubit-ltd/rs-web/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-web/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-web/coverage-badge.json)](https://qubit-ltd.github.io/rs-web/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-web.svg?color=blue)](https://crates.io/crates/qubit-web)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

Qubit Web 计划为基于 Axum 的 Rust 服务提供可复用的 HTTP、HTTPS 和
WebSocket 服务端基础设施，与客户端 crate
[`qubit-http`](https://github.com/qubit-ltd/rs-http) 配合使用。

## 当前状态

仓库目前只有初始骨架，没有服务端实现或稳定公共 API。设计文档将在
[`doc/`](doc/) 中供审阅。

## 计划范围

计划提供 HTTP、HTTPS 和 WebSocket 端点所需的基础能力。业务项目自行
管理路由、身份授权决策、持久化、Agent 流程和应用协议。

## 安装

crate 尚未发布。初始库可使用 Rust 1.94 或更高版本在本地构建，目前
没有供应用接入的服务端 API。

## 测试

```bash
cargo test
cargo test --all-features
./.infra/bin/ci-check.sh
./.infra/bin/coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请及时更新公共 API 文档与测试，并在提交 Pull Request 前运行
`./.infra/bin/align-ci.sh` 和 `./.infra/bin/ci-check.sh`。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-web](https://github.com/qubit-ltd/rs-web)
