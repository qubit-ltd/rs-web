import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "check-downstream-interop.py"
SPEC = importlib.util.spec_from_file_location("check_downstream_interop", SCRIPT)
checker = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(checker)


class CheckerSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.upstream = self.root / "upstream"
        self.downstream = self.root / "downstream"
        self.upstream.mkdir()
        self.downstream.mkdir()
        self._manifest(self.upstream, "qubit-web", "")
        self._manifest(
            self.downstream,
            "qubit-websocket",
            'qubit-web = { git = "https://github.com/qubit-ltd/rs-web", rev = "3046fa0fbc46c6c4584fdf9710b45649dfebd70a" }',
        )
        self.marker = self.root / "disposable.marker"
        self.marker.write_text(str(self.downstream.resolve()), encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def _manifest(directory, package, dependency):
        body = f'[package]\nname = "{package}"\nversion = "0.1.0"\n'
        if dependency:
            body += f"\n[dev-dependencies]\n{dependency}\n"
        (directory / "Cargo.toml").write_text(body, encoding="utf-8")

    def _args(self, upstream=None, downstream=None, marker=None):
        return [
            "--upstream", str(upstream if upstream is not None else self.upstream),
            "--downstream", str(downstream if downstream is not None else self.downstream),
            "--disposable-marker", str(marker if marker is not None else self.marker),
        ]

    def _assert_rejected_before_test(self, args):
        real_run = checker.subprocess.run
        calls = []

        def record(command, *a, **kw):
            calls.append(command)
            if "update" in command:
                return mock.Mock(returncode=0, stdout="", stderr="")
            if "metadata" in command:
                return mock.Mock(returncode=0, stdout=json.dumps(self._metadata_old_source()), stderr="")
            return real_run(command, *a, **kw)

        with mock.patch.object(checker.subprocess, "run", side_effect=record):
            result = checker.main(args)
        self.assertNotEqual(result, 0)
        self.assertFalse(any("web_interop_tests" in command for command in calls))

    def _metadata_old_source(self):
        up = self.upstream.resolve() / "Cargo.toml"
        return {
            "packages": [
                {"id": "path+file:///old/qubit-web#0.1.0", "name": "qubit-web", "manifest_path": "/old/qubit-web/Cargo.toml", "source": "git+https://github.com/qubit-ltd/rs-web?rev=old"},
                {"id": "path+file:///downstream#0.1.0", "name": "qubit-websocket", "manifest_path": str(self.downstream.resolve() / "Cargo.toml"), "source": None},
            ],
            "resolve": {"nodes": [{"id": "path+file:///downstream#0.1.0", "deps": [{"name": "qubit_web", "pkg": "path+file:///old/qubit-web#0.1.0"}]}]},
        }

    def test_nonexistent_upstream_fails_before_subprocess(self):
        with mock.patch.object(checker.subprocess, "run") as run:
            self.assertNotEqual(checker.main(self._args(upstream=self.root / "missing")), 0)
        run.assert_not_called()

    def test_overlapping_paths_fail_before_subprocess(self):
        with mock.patch.object(checker.subprocess, "run") as run:
            self.assertNotEqual(checker.main(self._args(upstream=self.root)), 0)
        run.assert_not_called()

    def test_missing_disposable_marker_fails_before_subprocess(self):
        self.marker.unlink()
        with mock.patch.object(checker.subprocess, "run") as run:
            self.assertNotEqual(checker.main(self._args()), 0)
        run.assert_not_called()

    def test_wrong_disposable_marker_fails_before_subprocess(self):
        other = self.root / "other"
        other.mkdir()
        self.marker.write_text(str(other.resolve()), encoding="utf-8")
        with mock.patch.object(checker.subprocess, "run") as run:
            self.assertNotEqual(checker.main(self._args()), 0)
        run.assert_not_called()

    def test_marker_symlink_inside_repository_fails_before_subprocess(self):
        marker_link = self.downstream / "marker-link"
        marker_link.symlink_to(self.marker)
        with mock.patch.object(checker.subprocess, "run") as run:
            self.assertNotEqual(checker.main(self._args(marker=marker_link)), 0)
        run.assert_not_called()

    def test_metadata_resolving_to_old_git_package_fails_before_test(self):
        self._assert_rejected_before_test(self._args())

    def test_metadata_must_follow_direct_consumer_dependency(self):
        data = self._metadata_old_source()
        data["packages"][0]["manifest_path"] = str(self.upstream.resolve() / "Cargo.toml")
        data["packages"][0]["source"] = None
        data["resolve"]["nodes"][0]["deps"][0]["pkg"] = "unrelated-package-id"
        with self.assertRaises(checker.InteropError):
            checker.assert_resolved_source(data, self.downstream.resolve(), self.upstream.resolve())

    def test_wss_option_is_explicit_and_off_by_default(self):
        parser = checker.build_parser()
        self.assertFalse(parser.parse_args(self._args()).with_wss)
        self.assertTrue(parser.parse_args(self._args() + ["--with-wss"]).with_wss)

    def test_wss_manifest_overrides_only_the_two_direct_local_dependencies(self):
        fixture = self.root / "wss-fixture"
        source = SCRIPT.parents[1] / "tests" / "fixtures" / "wss_consumer"
        generated = checker.prepare_wss_fixture(source, fixture, self.upstream, self.downstream)
        manifest = checker._read_manifest(generated)
        self.assertEqual(manifest["dependencies"]["qubit-web"]["path"], str(self.upstream.resolve()))
        self.assertEqual(manifest["dependencies"]["qubit-websocket"]["path"], str(self.downstream.resolve()))
        self.assertEqual(manifest["dependencies"]["qubit-web"]["features"], ["ws", "tls"])
        self.assertFalse((generated / "Cargo.lock").exists())
        self.assertTrue((generated / "tests" / "wss_interop.rs").is_file())
        self.assertTrue((generated.parent / "tls" / "https-h2-cert.pem").is_file())

    def test_wss_metadata_checks_both_direct_paths_and_sources(self):
        upstream_manifest = str((self.upstream / "Cargo.toml").resolve())
        downstream_manifest = str((self.downstream / "Cargo.toml").resolve())
        metadata = {
            "packages": [
                {"id": "up", "name": "qubit-web", "manifest_path": upstream_manifest, "source": None},
                {"id": "down", "name": "qubit-websocket", "manifest_path": downstream_manifest, "source": None},
                {"id": "fixture", "name": "qubit-web-wss-consumer", "manifest_path": "/tmp/wss/Cargo.toml", "source": None},
            ],
            "resolve": {"nodes": [{"id": "fixture", "deps": [
                {"name": "qubit_web", "pkg": "up"},
                {"name": "qubit_websocket", "pkg": "down"},
            ]}]},
        }
        paths = checker.assert_wss_resolved_sources(metadata, Path("/tmp/wss/Cargo.toml"), self.upstream, self.downstream)
        self.assertEqual(paths, (self.upstream.resolve() / "Cargo.toml", self.downstream.resolve() / "Cargo.toml"))
        metadata["packages"][1]["source"] = "git+https://example.invalid"
        with self.assertRaises(checker.InteropError):
            checker.assert_wss_resolved_sources(metadata, Path("/tmp/wss/Cargo.toml"), self.upstream, self.downstream)
        metadata["packages"][1].update(manifest_path=downstream_manifest, source=None)
        metadata["resolve"]["nodes"][0]["deps"].pop()
        with self.assertRaises(checker.InteropError):
            checker.assert_wss_resolved_sources(metadata, Path("/tmp/wss/Cargo.toml"), self.upstream, self.downstream)

    def test_ws_failure_propagates_without_running_wss(self):
        calls = []

        def fake_run(command, *args, **kwargs):
            calls.append(command)
            if "update" in command:
                return mock.Mock(returncode=0, stdout="", stderr="")
            if "metadata" in command:
                return mock.Mock(returncode=0, stdout=json.dumps(self._metadata_old_source()), stderr="")
            return mock.Mock(returncode=17)

        metadata = self._metadata_old_source()
        metadata["packages"][0].update(manifest_path=str((self.upstream / "Cargo.toml").resolve()), source=None)
        with mock.patch.object(checker, "_validate_paths", return_value=(self.upstream.resolve(), self.downstream.resolve())), \
             mock.patch.object(checker.subprocess, "run", side_effect=fake_run):
            with mock.patch.object(checker.json, "loads", return_value=metadata):
                self.assertEqual(checker.run(str(self.upstream), str(self.downstream), str(self.marker), True), 17)
        cargo_calls = [command for command in calls if command[0] == "cargo"]
        self.assertEqual(len(cargo_calls), 3)
        self.assertNotIn("wss_interop", str(cargo_calls))


if __name__ == "__main__":
    unittest.main()
