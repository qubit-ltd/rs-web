#!/usr/bin/env python3
"""Check that qubit-web and Axum use the same Tungstenite package."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def _describe(package: dict[str, Any]) -> str:
    return (
        f"{package['name']} {package['version']} (id={package['id']}, "
        f"source={package.get('source') or 'path'}, "
        f"manifest={package.get('manifest_path', '<unknown>')})"
    )


def validate_backend(metadata: dict[str, Any], manifest_path: Path) -> dict[str, Any]:
    """Validate the finite qubit-web -> Axum -> tokio-tungstenite chain.

    Returned package records include Cargo's full package IDs so callers can
    retain useful evidence when a dependency source or version drifts.
    """
    packages = {package["id"]: package for package in metadata.get("packages", [])}
    resolve = metadata.get("resolve")
    if not isinstance(resolve, dict) or not isinstance(resolve.get("nodes"), list):
        raise ValueError("Cargo metadata has no complete resolve graph")
    nodes = {node["id"]: node for node in resolve["nodes"]}

    expected_manifest = manifest_path.resolve()
    targets = [
        package
        for package in packages.values()
        if package.get("name") == "qubit-web"
        and Path(package.get("manifest_path", "")).resolve() == expected_manifest
    ]
    if len(targets) != 1:
        raise ValueError(
            f"expected one qubit-web package for manifest {expected_manifest}; "
            f"found {len(targets)}"
        )
    target = targets[0]

    def outgoing(package: dict[str, Any], dependency_name: str) -> list[dict[str, Any]]:
        node = nodes.get(package["id"])
        if node is None:
            raise ValueError(f"resolve graph is missing node for {_describe(package)}")
        result = []
        cargo_edge_name = dependency_name.replace("-", "_")
        for edge in node.get("deps", []):
            if edge.get("name") not in {dependency_name, cargo_edge_name}:
                continue
            package_ids = edge.get("pkg", [])
            if isinstance(package_ids, str):
                package_ids = [package_ids]
            for package_id in package_ids:
                dependency = packages.get(package_id)
                if dependency is not None and dependency.get("name") == dependency_name:
                    result.append(dependency)
        unique = {item["id"]: item for item in result}
        return list(unique.values())

    def exactly_one(parent: dict[str, Any], dependency_name: str) -> dict[str, Any]:
        found = outgoing(parent, dependency_name)
        if len(found) != 1:
            raise ValueError(
                f"expected exactly one {dependency_name} edge from {_describe(parent)}; "
                f"found {len(found)}"
            )
        return found[0]

    axum = exactly_one(target, "axum")
    direct_tungstenite = exactly_one(target, "tungstenite")
    backend = exactly_one(axum, "tokio-tungstenite")
    backend_tungstenite = exactly_one(backend, "tungstenite")
    if direct_tungstenite["id"] != backend_tungstenite["id"]:
        raise ValueError(
            "Tungstenite package ID mismatch: qubit-web uses "
            f"{_describe(direct_tungstenite)}; Axum backend uses "
            f"{_describe(backend_tungstenite)}"
        )
    return {
        "target": target,
        "axum": axum,
        "backend": backend,
        "tungstenite": direct_tungstenite,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "Cargo.toml",
        help="Cargo manifest to inspect (default: repository Cargo.toml)",
    )
    args = parser.parse_args(argv)
    manifest = args.manifest_path.resolve()
    command = [
        "cargo",
        "metadata",
        "--format-version",
        "1",
        "--locked",
        "--features",
        "ws",
        "--manifest-path",
        str(manifest),
    ]
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=False)
    except OSError as error:
        print(f"failed to run {' '.join(command)}: {error}", file=sys.stderr)
        return 1
    if result.returncode:
        print(f"command failed ({result.returncode}): {' '.join(command)}", file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="" if result.stderr.endswith("\n") else "\n")
        return result.returncode
    try:
        summary = validate_backend(json.loads(result.stdout), manifest)
    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
        print(f"WebSocket backend contract failed: {error}", file=sys.stderr)
        return 1
    print("WebSocket backend package IDs are aligned:")
    for label, package in summary.items():
        print(f"  {label}: {_describe(package)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
