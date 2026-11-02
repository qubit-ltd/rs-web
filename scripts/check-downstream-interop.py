#!/usr/bin/env python3
"""Run rs-websocket's interoperability tests against a local rs-web checkout."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
from typing import NoReturn


EXPECTED_GIT_URL = "https://github.com/qubit-ltd/rs-web"
DOWNSTREAM_PACKAGE = "qubit-websocket"
DEPENDENCY_NAME = "qubit-web"


def fail(message: str) -> "NoReturn":
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def run(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    print("+ " + " ".join(command), flush=True)
    result = subprocess.run(
        command, cwd=cwd, env=env, text=True, capture_output=capture_output, check=False
    )
    if capture_output and result.stderr:
        print(result.stderr, file=sys.stderr, end="")
    return result


def require_checkout(path: Path, name: str) -> Path:
    if not path.is_absolute():
        fail(f"--{name} must be an absolute path: {path}")
    resolved = path.resolve()
    if not (resolved / "Cargo.toml").is_file():
        fail(f"--{name} must contain Cargo.toml: {resolved}")
    return resolved


def cargo_package_sha(path: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=path, text=True, capture_output=True, check=False
    )
    return result.stdout.strip() if result.returncode == 0 else "unavailable"


def check_dependency_declaration(downstream: Path, upstream_version: str) -> None:
    with (downstream / "Cargo.toml").open("rb") as manifest_file:
        manifest = tomllib.load(manifest_file)
    dev_dependencies = manifest.get("dev-dependencies", {})
    dependency = dev_dependencies.get(DEPENDENCY_NAME)
    if (
        not isinstance(dependency, dict)
        or dependency.get("git", "").removesuffix(".git") != EXPECTED_GIT_URL
        or dependency.get("version") != upstream_version
    ):
        actual = dependency.get("git") if isinstance(dependency, dict) else dependency
        version = dependency.get("version") if isinstance(dependency, dict) else None
        fail(
            f"{DEPENDENCY_NAME} must be a dev-dependency from {EXPECTED_GIT_URL} with version "
            f"{upstream_version!r}; found git={actual!r}, version={version!r}"
        )


def assert_source(metadata: dict, upstream: Path, downstream: Path, upstream_version: str) -> tuple[str, str]:
    packages = {package["id"]: package for package in metadata.get("packages", [])}
    resolve = metadata.get("resolve") or {}
    nodes = {node["id"]: node for node in resolve.get("nodes", [])}
    downstream_manifest = (downstream / "Cargo.toml").resolve()
    downstream_ids = [package_id for package_id, package in packages.items()
                      if package.get("name") == DOWNSTREAM_PACKAGE
                      and Path(package.get("manifest_path", "")).resolve() == downstream_manifest]
    if len(downstream_ids) != 1:
        fail(f"metadata should contain one {DOWNSTREAM_PACKAGE} package at {downstream_manifest}; found {len(downstream_ids)}")
    node = nodes.get(downstream_ids[0])
    if node is None:
        fail(f"metadata resolve graph has no node for {downstream_ids[0]}")
    dependency_ids = [dep["pkg"] for dep in node.get("deps", [])
                      if dep["pkg"] in packages
                      and packages[dep["pkg"]].get("name") == DEPENDENCY_NAME
                      and any(kind.get("kind") == "dev" for kind in dep.get("dep_kinds", []))]
    if len(dependency_ids) != 1:
        fail(f"{DOWNSTREAM_PACKAGE} must resolve exactly one direct {DEPENDENCY_NAME} dependency; found {dependency_ids}")
    package = packages.get(dependency_ids[0])
    actual_manifest = package.get("manifest_path") if package else None
    expected_manifest = (upstream / "Cargo.toml").resolve()
    actual_source = package.get("source") if package else None
    actual_version = package.get("version") if package else None
    if (
        package is None
        or actual_manifest is None
        or Path(actual_manifest).resolve() != expected_manifest
        or actual_source is not None
        or actual_version != upstream_version
    ):
        fail(
            f"{DEPENDENCY_NAME} resolved to source={actual_source!r}, manifest_path={actual_manifest!r}, "
            f"version={actual_version!r}; expected source=null, manifest_path={str(expected_manifest)!r}, "
            f"and version={upstream_version!r}"
        )
    return dependency_ids[0], str(expected_manifest)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True, type=Path, help="absolute path to the rs-web checkout")
    parser.add_argument("--downstream", required=True, type=Path, help="absolute path to a disposable rs-websocket checkout")
    args = parser.parse_args()
    upstream = require_checkout(args.upstream, "upstream")
    downstream = require_checkout(args.downstream, "downstream")
    if upstream == downstream or upstream in downstream.parents or downstream in upstream.parents:
        fail("--upstream and --downstream must be independent checkout directories")
    with (upstream / "Cargo.toml").open("rb") as manifest_file:
        upstream_manifest = tomllib.load(manifest_file)
    if upstream_manifest.get("package", {}).get("name") != DEPENDENCY_NAME:
        fail(f"--upstream must contain package {DEPENDENCY_NAME}")
    upstream_version = upstream_manifest["package"]["version"]
    check_dependency_declaration(downstream, upstream_version)

    with tempfile.TemporaryDirectory(prefix="rs-web-downstream-interop-") as temp_name:
        temp_dir = Path(temp_name)
        config_path = temp_dir / "cargo-config.toml"
        config_path.write_text(
            f'[patch."{EXPECTED_GIT_URL}"]\nqubit-web = {{ path = {json.dumps(str(upstream))} }}\n',
            encoding="utf-8",
        )
        target_dir = temp_dir / "cargo-target"
        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(target_dir)
        command = ["cargo", "--config", str(config_path), "metadata", "--format-version", "1"]
        result = run(command, cwd=downstream, env=env, capture_output=True)
        if result.returncode:
            return result.returncode
        try:
            metadata = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            fail(f"cargo metadata returned invalid JSON: {error}")
        _, manifest_path = assert_source(metadata, upstream, downstream, upstream_version)
        print(f"upstream SHA: {cargo_package_sha(upstream)}")
        print(f"downstream SHA: {cargo_package_sha(downstream)}")
        print(f"resolved {DEPENDENCY_NAME} manifest: {manifest_path}")
        test_command = ["cargo", "--config", str(config_path), "test", "--no-default-features", "--test", "web_interop_tests"]
        test_result = run(test_command, cwd=downstream, env=env)
        print(f"downstream interoperability test exit code: {test_result.returncode}")
        return test_result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
