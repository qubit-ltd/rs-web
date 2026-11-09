"""Synthetic Cargo resolve graph tests for check-ws-backend.py."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check-ws-backend.py"
SPEC = importlib.util.spec_from_file_location("check_ws_backend", SCRIPT)
assert SPEC and SPEC.loader
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


def graph(*, direct_version="0.29.0", backend_version="0.29.0", direct_source="registry", include_backend=True, include_resolve=True, unrelated=False):
    manifest = "/synthetic/Cargo.toml"
    packages = [
        {"id": "web 0.1.0 (path+file:///synthetic)", "name": "qubit-web", "version": "0.1.0", "manifest_path": manifest, "source": None},
        {"id": "axum 0.8.9 (registry)", "name": "axum", "version": "0.8.9", "manifest_path": "/registry/axum/Cargo.toml", "source": "registry"},
        {"id": "tt 0.26.0 (registry)", "name": "tokio-tungstenite", "version": "0.26.0", "manifest_path": "/registry/tt/Cargo.toml", "source": "registry"},
        {"id": f"tungstenite {direct_version} ({direct_source})", "name": "tungstenite", "version": direct_version, "manifest_path": "/registry/tungstenite/Cargo.toml", "source": direct_source},
    ]
    backend_id = f"tungstenite {backend_version} (registry)"
    if backend_id not in {p["id"] for p in packages}:
        packages.append({"id": backend_id, "name": "tungstenite", "version": backend_version, "manifest_path": "/registry/tungstenite/Cargo.toml", "source": "registry"})
    if unrelated:
        packages.append({"id": "tungstenite 0.28 (other)", "name": "tungstenite", "version": "0.28", "manifest_path": "/other/Cargo.toml", "source": "registry"})
    nodes = [
        {"id": packages[0]["id"], "deps": [{"name": "axum", "pkg": packages[1]["id"]}, {"name": "tungstenite", "pkg": packages[3]["id"]}]},
        {"id": packages[1]["id"], "deps": [{"name": "tokio_tungstenite", "pkg": packages[2]["id"]}]},
        {"id": packages[2]["id"], "deps": ([{"name": "tungstenite", "pkg": backend_id}] if include_backend else [])},
        {"id": packages[3]["id"], "deps": []},
    ]
    if not include_resolve:
        resolve = None
    else:
        resolve = {"nodes": nodes}
    return {"packages": packages, "resolve": resolve}, Path(manifest)


class ValidateBackendTests(unittest.TestCase):
    def test_same_package_id_succeeds(self):
        metadata, manifest = graph()
        result = checker.validate_backend(metadata, manifest)
        self.assertEqual(result["tungstenite"]["id"], "tungstenite 0.29.0 (registry)")

    def test_same_version_different_source_fails(self):
        metadata, manifest = graph(direct_source="git")
        with self.assertRaisesRegex(ValueError, "package ID mismatch"):
            checker.validate_backend(metadata, manifest)

    def test_different_version_fails(self):
        metadata, manifest = graph(backend_version="0.28.2")
        with self.assertRaisesRegex(ValueError, "package ID mismatch"):
            checker.validate_backend(metadata, manifest)

    def test_missing_backend_dependency_fails(self):
        metadata, manifest = graph(include_backend=False)
        with self.assertRaisesRegex(ValueError, "tungstenite edge"):
            checker.validate_backend(metadata, manifest)

    def test_unrelated_tungstenite_does_not_affect_check(self):
        metadata, manifest = graph(unrelated=True)
        checker.validate_backend(metadata, manifest)

    def test_missing_resolve_fails(self):
        metadata, manifest = graph(include_resolve=False)
        with self.assertRaisesRegex(ValueError, "resolve graph"):
            checker.validate_backend(metadata, manifest)


if __name__ == "__main__":
    unittest.main()
