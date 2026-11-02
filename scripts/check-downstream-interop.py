#!/usr/bin/env python3
"""Run the fixed rs-websocket integration tests against an explicit rs-web checkout."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib

EXPECTED_GIT_URL = "https://github.com/qubit-ltd/rs-web"


class InteropError(RuntimeError):
    """Raised when the disposable checkout or resolved dependency is unsafe."""


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _read_manifest(directory: Path) -> dict:
    manifest_path = directory / "Cargo.toml"
    try:
        with manifest_path.open("rb") as manifest_file:
            return tomllib.load(manifest_file)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise InteropError(f"cannot read {manifest_path}: {error}") from error


def _validate_paths(upstream_arg: str, downstream_arg: str, marker_arg: str):
    raw_paths = [Path(value) for value in (upstream_arg, downstream_arg, marker_arg)]
    if any(not path.is_absolute() for path in raw_paths):
        raise InteropError("upstream, downstream, and disposable-marker paths must be absolute")
    if raw_paths[2].is_symlink():
        raise InteropError("disposable marker must not be a symbolic link")
    upstream, downstream, marker = (path.resolve() for path in raw_paths)
    if not upstream.is_dir():
        raise InteropError(f"upstream directory does not exist: {upstream}")
    if not downstream.is_dir():
        raise InteropError(f"downstream directory does not exist: {downstream}")
    if upstream == downstream or _is_within(upstream, downstream) or _is_within(downstream, upstream):
        raise InteropError(f"upstream and downstream directories overlap: {upstream} ; {downstream}")
    if _is_within(marker, upstream) or _is_within(marker, downstream):
        raise InteropError("disposable marker must be outside both repository trees")
    try:
        marked_path = Path(marker.read_text(encoding="utf-8").strip()).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise InteropError(f"cannot read disposable marker {marker}: {error}") from error
    if marked_path != downstream:
        raise InteropError(
            f"disposable marker names {marked_path}, expected resolved downstream {downstream}"
        )

    upstream_manifest = _read_manifest(upstream)
    downstream_manifest = _read_manifest(downstream)
    upstream_name = upstream_manifest.get("package", {}).get("name")
    downstream_name = downstream_manifest.get("package", {}).get("name")
    if upstream_name != "qubit-web":
        raise InteropError(f"upstream package.name must be qubit-web, got {upstream_name!r}")
    if downstream_name != "qubit-websocket":
        raise InteropError(f"downstream package.name must be qubit-websocket, got {downstream_name!r}")
    dependency = downstream_manifest.get("dev-dependencies", {}).get("qubit-web")
    if not isinstance(dependency, dict) or dependency.get("git", "").removesuffix(".git") != EXPECTED_GIT_URL:
        raise InteropError(
            "downstream dev-dependency qubit-web must use git URL "
            f"{EXPECTED_GIT_URL}; found {dependency!r}"
        )
    return upstream, downstream


def assert_resolved_source(metadata: dict, downstream: Path, upstream: Path) -> Path:
    """Assert qubit-websocket's direct edge resolves to the supplied local source."""
    packages_by_id = {package.get("id"): package for package in metadata.get("packages", [])}
    consumers = [
        package for package in metadata.get("packages", [])
        if package.get("name") == "qubit-websocket"
        and Path(package.get("manifest_path", "")).resolve() == downstream / "Cargo.toml"
    ]
    if len(consumers) != 1:
        raise InteropError(f"metadata did not identify one qubit-websocket consumer at {downstream}")
    consumer_id = consumers[0].get("id")
    nodes = {node.get("id"): node for node in metadata.get("resolve", {}).get("nodes", [])}
    consumer_node = nodes.get(consumer_id)
    if consumer_node is None:
        raise InteropError("metadata resolve graph has no node for the qubit-websocket consumer")
    direct_ids = {
        dependency.get("pkg")
        for dependency in consumer_node.get("deps", [])
        if dependency.get("name", "").replace("-", "_") == "qubit_web"
    }
    resolved_packages = [
        packages_by_id[package_id]
        for package_id in direct_ids
        if package_id in packages_by_id and packages_by_id[package_id].get("name") == "qubit-web"
    ]
    expected_manifest = (upstream / "Cargo.toml").resolve()
    if len(resolved_packages) != 1:
        observed = [
            {
                "id": package_id,
                "manifest_path": packages_by_id.get(package_id, {}).get("manifest_path"),
                "source": packages_by_id.get(package_id, {}).get("source"),
            }
            for package_id in sorted(direct_ids)
        ]
        raise InteropError(f"consumer direct qubit-web dependency did not resolve uniquely: {observed}")
    package = resolved_packages[0]
    actual_manifest = Path(package.get("manifest_path", "")).resolve()
    actual_source = package.get("source")
    if actual_manifest != expected_manifest or actual_source is not None:
        raise InteropError(
            "consumer qubit-web resolved to the wrong source: "
            f"manifest_path={actual_manifest}, source={actual_source!r}; "
            f"expected manifest_path={expected_manifest}, source=None"
        )
    return actual_manifest


def _git_revision(directory: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=directory, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    if result.returncode != 0:
        return "unavailable"
    return result.stdout.strip()


def run(upstream_arg: str, downstream_arg: str, marker_arg: str) -> int:
    upstream, downstream = _validate_paths(upstream_arg, downstream_arg, marker_arg)
    with tempfile.TemporaryDirectory(prefix="rs-web-downstream-interop-") as temporary:
        temporary_path = Path(temporary)
        config_path = temporary_path / "cargo-config.toml"
        target_path = temporary_path / "target"
        config_path.write_text(
            '[patch."https://github.com/qubit-ltd/rs-web"]\n'
            f"qubit-web = {{ path = {json.dumps(str(upstream))} }}\n",
            encoding="utf-8",
        )
        update_command = ["cargo", "--config", str(config_path), "update", "-p", "qubit-web"]
        update_result = subprocess.run(
            update_command, cwd=downstream, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if update_result.returncode != 0:
            raise InteropError(
                f"cargo update -p qubit-web failed with exit {update_result.returncode}:\n"
                f"{update_result.stderr}"
            )
        metadata_command = [
            "cargo", "--config", str(config_path), "metadata", "--format-version", "1",
        ]
        metadata_result = subprocess.run(
            metadata_command, cwd=downstream, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if metadata_result.returncode != 0:
            raise InteropError(
                f"cargo metadata failed with exit {metadata_result.returncode}:\n{metadata_result.stderr}"
            )
        try:
            metadata = json.loads(metadata_result.stdout)
        except json.JSONDecodeError as error:
            raise InteropError(f"cargo metadata returned invalid JSON: {error}") from error
        resolved_manifest = assert_resolved_source(metadata, downstream, upstream)
        print(f"upstream SHA: {_git_revision(upstream)}")
        print(f"downstream SHA: {_git_revision(downstream)}")
        print(f"resolved qubit-web manifest: {resolved_manifest}")
        test_command = [
            "cargo", "--config", str(config_path), "test", "--no-default-features",
            "--test", "web_interop_tests",
        ]
        environment = os.environ.copy()
        environment["CARGO_TARGET_DIR"] = str(target_path)
        result = subprocess.run(test_command, cwd=downstream, env=environment, check=False)
        print(f"web_interop_tests exit code: {result.returncode}")
        return result.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True, help="absolute rs-web checkout path")
    parser.add_argument("--downstream", required=True, help="absolute disposable rs-websocket checkout path")
    parser.add_argument("--disposable-marker", required=True, help="external marker naming the disposable checkout")
    arguments = parser.parse_args(argv)
    try:
        return run(arguments.upstream, arguments.downstream, arguments.disposable_marker)
    except InteropError as error:
        print(f"downstream interop check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
