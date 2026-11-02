#!/usr/bin/env python3
"""Set the disposable consumer checkout's rs-web version requirement."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import tomllib


EXPECTED_GIT_URL = "https://github.com/qubit-ltd/rs-web"
DEPENDENCY_NAME = "qubit-web"


def prepare_manifest_text(manifest_text: str, *, expected_git: str, upstream_version: str) -> str:
    manifest = tomllib.loads(manifest_text)
    dependency = manifest.get("dev-dependencies", {}).get(DEPENDENCY_NAME)
    actual_git = dependency.get("git", "").removesuffix(".git") if isinstance(dependency, dict) else None
    if actual_git != expected_git:
        raise ValueError(f"{DEPENDENCY_NAME} must use git URL {expected_git}; found {actual_git!r}")

    lines = manifest_text.splitlines(keepends=True)
    section = None
    dependency_line = None
    for index, line in enumerate(lines):
        section_match = re.fullmatch(r"\s*\[([^]]+)\]\s*(?:#.*)?(?:\r?\n)?", line)
        if section_match:
            section = section_match.group(1)
        if section == "dev-dependencies" and re.match(r"\s*qubit-web\s*=\s*\{", line):
            if dependency_line is not None:
                raise ValueError(f"multiple {DEPENDENCY_NAME} entries found in [dev-dependencies]")
            dependency_line = index
    if dependency_line is None:
        raise ValueError(f"{DEPENDENCY_NAME} must be an inline table in [dev-dependencies]")

    line = lines[dependency_line]
    match = re.fullmatch(r"(\s*qubit-web\s*=\s*\{)(.*)(\}\s*(?:#.*)?)(\r?\n)?", line)
    if match is None:
        raise ValueError(f"{DEPENDENCY_NAME} dependency must fit on one line")
    prefix, body, suffix, newline = match.groups()
    version_field = re.compile(r'\bversion\s*=\s*"[^"]*"')
    if version_field.search(body):
        body = version_field.sub(f'version = "{upstream_version}"', body, count=1)
    else:
        body = f' version = "{upstream_version}", {body.lstrip()}'
    lines[dependency_line] = f"{prefix}{body}{suffix}{newline or ''}"

    prepared = "".join(lines)
    prepared_manifest = tomllib.loads(prepared)
    prepared_dependency = prepared_manifest["dev-dependencies"][DEPENDENCY_NAME]
    if prepared_dependency.get("version") != upstream_version:
        raise ValueError("updated dependency version did not parse as expected")
    return prepared


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True, type=Path, help="absolute path to the current rs-web checkout")
    parser.add_argument("--downstream", required=True, type=Path, help="absolute path to a disposable rs-websocket checkout")
    args = parser.parse_args()
    for name, path in (("upstream", args.upstream), ("downstream", args.downstream)):
        if not path.is_absolute():
            parser.error(f"--{name} must be an absolute path: {path}")
        if not (path / "Cargo.toml").is_file():
            parser.error(f"--{name} must contain Cargo.toml: {path}")

    upstream_manifest = tomllib.loads((args.upstream / "Cargo.toml").read_text(encoding="utf-8"))
    if upstream_manifest.get("package", {}).get("name") != DEPENDENCY_NAME:
        parser.error(f"--upstream must contain package {DEPENDENCY_NAME}")
    upstream_version = upstream_manifest["package"]["version"]
    manifest_path = args.downstream / "Cargo.toml"
    original = manifest_path.read_text(encoding="utf-8")
    try:
        prepared = prepare_manifest_text(original, expected_git=EXPECTED_GIT_URL, upstream_version=upstream_version)
    except (KeyError, TypeError, ValueError) as error:
        parser.error(str(error))
    if prepared != original:
        manifest_path.write_text(prepared, encoding="utf-8")
    print(f"prepared disposable {DEPENDENCY_NAME} dev-dependency version {upstream_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
