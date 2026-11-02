import contextlib
import importlib.util
import io
import unittest
from pathlib import Path

from scripts.prepare_downstream_interop import prepare_manifest_text


CHECKER_PATH = Path(__file__).with_name("check-downstream-interop.py")
CHECKER_SPEC = importlib.util.spec_from_file_location("downstream_interop_checker", CHECKER_PATH)
CHECKER = importlib.util.module_from_spec(CHECKER_SPEC)
CHECKER_SPEC.loader.exec_module(CHECKER)


class PrepareDownstreamInteropTests(unittest.TestCase):
    def test_adds_upstream_version_to_fixed_git_dev_dependency(self):
        manifest = '''[dev-dependencies]\nqubit-web = { git = "https://github.com/qubit-ltd/rs-web", rev = "c9c61db", default-features = false, features = ["ws"] }\n'''

        prepared = prepare_manifest_text(
            manifest,
            expected_git="https://github.com/qubit-ltd/rs-web",
            upstream_version="0.3.0",
        )

        self.assertIn('version = "0.3.0"', prepared)
        self.assertIn('rev = "c9c61db"', prepared)
        self.assertIn('features = ["ws"]', prepared)

    def test_replaces_existing_version_without_changing_git_source(self):
        manifest = '''[dev-dependencies]\nqubit-web = { version = "0.2.0", git = "https://github.com/qubit-ltd/rs-web", rev = "c9c61db" }\n'''

        prepared = prepare_manifest_text(
            manifest,
            expected_git="https://github.com/qubit-ltd/rs-web",
            upstream_version="0.3.0",
        )

        self.assertIn('version = "0.3.0"', prepared)
        self.assertIn('git = "https://github.com/qubit-ltd/rs-web"', prepared)
        self.assertIn('rev = "c9c61db"', prepared)

    def test_rejects_an_unexpected_git_source(self):
        manifest = '''[dev-dependencies]\nqubit-web = { git = "https://example.invalid/rs-web", rev = "c9c61db" }\n'''

        with self.assertRaisesRegex(ValueError, "must use git URL"):
            prepare_manifest_text(
                manifest,
                expected_git="https://github.com/qubit-ltd/rs-web",
                upstream_version="0.3.0",
            )

    def test_rejects_metadata_that_keeps_the_git_source(self):
        metadata = {
            "packages": [
                {
                    "id": "qubit-websocket 0.3.0 (path+file:///downstream)",
                    "name": "qubit-websocket",
                    "manifest_path": "/downstream/Cargo.toml",
                },
                {
                    "id": "qubit-web 0.2.0 (git+https://github.com/qubit-ltd/rs-web)",
                    "name": "qubit-web",
                    "version": "0.2.0",
                    "manifest_path": "/cargo/git/checkouts/rs-web/Cargo.toml",
                    "source": "git+https://github.com/qubit-ltd/rs-web",
                },
            ],
            "resolve": {
                "nodes": [
                    {
                        "id": "qubit-websocket 0.3.0 (path+file:///downstream)",
                        "deps": [
                            {
                                "pkg": "qubit-web 0.2.0 (git+https://github.com/qubit-ltd/rs-web)",
                                "dep_kinds": [{"kind": "dev"}],
                            }
                        ],
                    }
                ]
            },
        }

        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                CHECKER.assert_source(
                    metadata,
                    Path("/upstream"),
                    Path("/downstream"),
                    upstream_version="0.3.0",
                )


if __name__ == "__main__":
    unittest.main()
