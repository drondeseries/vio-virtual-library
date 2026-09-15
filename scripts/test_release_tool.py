#!/usr/bin/env python3
"""Unit and integration tests for release_tool.py using only the Python standard library."""

import json
import os
import sys
import subprocess
import tempfile
import hashlib
import shutil
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import release_tool as rt


class TestSemVerValidation(unittest.TestCase):
    def test_valid_semver(self):
        valid_versions = [
            "0.0.0",
            "0.4.100",
            "1.0.0",
            "1.2.3-alpha",
            "1.2.3-alpha.1",
            "1.2.3-0.3.7",
            "1.2.3-x.7.z.92",
            "1.2.3+build.7",
            "1.2.3-rc.1+build.5",
            "10.20.30-alpha.1.beta.2",
        ]
        for v in valid_versions:
            self.assertTrue(rt.validate_semver(v), f"Expected {v} to be valid SemVer")

    def test_invalid_semver(self):
        invalid_versions = [
            "1",
            "1.2",
            "1.2.3-",
            "01.2.3",
            "1.02.3",
            "1.2.03",
            "1.2.3-alpha..1",
            "1.2.3-alpha.01",
            "1.2.3+",
            "1.2.3+build..1",
            "",
            "abc",
            "1.2.3-a..b",
            "1.2.3-a.",
            "1.2.3-.a",
            "1.2.3-01",
        ]
        for v in invalid_versions:
            self.assertFalse(rt.validate_semver(v), f"Expected {v!r} to be invalid SemVer")


class TestSemVerPrecedence(unittest.TestCase):
    def test_precedence_order(self):
        order = [
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-alpha.beta",
            "1.0.0-beta",
            "1.0.0-beta.2",
            "1.0.0-beta.11",
            "1.0.0-rc.1",
            "1.0.0-rc.10",
            "1.0.0",
        ]
        for i in range(1, len(order)):
            self.assertLess(
                rt.SemVer(order[i - 1]),
                rt.SemVer(order[i]),
                f"{order[i-1]} should be < {order[i]}",
            )

    def test_semver_semantics(self):
        # Numeric < alphanumeric within same position
        self.assertLess(rt.SemVer("1.2.3-1"), rt.SemVer("1.2.3-alpha"))
        # Pre-release < release
        self.assertLess(rt.SemVer("1.0.0-rc.1"), rt.SemVer("1.0.0"))
        # Build metadata ignored for precedence
        self.assertEqual(rt.SemVer("1.2.3+1"), rt.SemVer("1.2.3+build.5"))
        self.assertEqual(rt.compare_semver("1.0.0-rc.10", "1.0.0-rc.2"), "GREATER")
        self.assertEqual(rt.compare_semver("0.4.99", "0.4.100"), "LESS")
        self.assertEqual(rt.compare_semver("0.4.100", "0.4.100"), "EQUAL")


class TestFileUtils(unittest.TestCase):
    def test_sha256_and_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_bytes = b"fake plugin binary content"
            p1 = os.path.join(tmp, "plugin-linux-amd64")
            with open(p1, "wb") as f:
                f.write(plugin_bytes)

            prov = rt.generate_provenance(tmp, "v0.4.100", "abc123", "go1.26.0", "2026-08-29T13:00:00+00:00")
            prov_again = rt.generate_provenance(tmp, "v0.4.100", "abc123", "go1.26.0", "2026-08-29T13:00:00+00:00")
            self.assertEqual(prov, prov_again)
            self.assertEqual(prov["version"], "0.4.100")
            self.assertEqual(prov["tag"], "v0.4.100")
            self.assertEqual(prov["commit_sha"], "abc123")
            self.assertEqual(prov["toolchain"], "go1.26.0")
            expected = hashlib.sha256(plugin_bytes).hexdigest()
            self.assertEqual(prov["artifacts"]["plugin-linux-amd64"]["sha256"], expected)

    def _make_dirs(self):
        base = tempfile.mkdtemp()
        existing = os.path.join(base, "existing")
        built = os.path.join(base, "built")
        os.makedirs(existing)
        os.makedirs(built)
        self.addCleanup(shutil.rmtree, base)
        return existing, built

    def _fill_assets(self, existing, built):
        for fname in rt.REQUIRED_ASSETS:
            with open(os.path.join(existing, fname), "w") as f:
                f.write(f"content-{fname}")
            with open(os.path.join(built, fname), "w") as f:
                f.write(f"content-{fname}")

    def test_verify_asset_set_exact_match(self):
        existing, built = self._make_dirs()
        self._fill_assets(existing, built)
        ok, msg = rt.verify_existing_assets(existing, built)
        self.assertTrue(ok, msg)

    def test_verify_asset_set_missing_asset_rejected(self):
        existing, built = self._make_dirs()
        self._fill_assets(existing, built)
        os.remove(os.path.join(existing, "provenance.json"))
        ok, msg = rt.verify_existing_assets(existing, built)
        self.assertFalse(ok)
        self.assertIn("Asset set mismatch", msg)

    def test_verify_asset_set_extra_asset_rejected(self):
        existing, built = self._make_dirs()
        self._fill_assets(existing, built)
        with open(os.path.join(existing, "unexpected-asset.txt"), "w") as f:
            f.write("extra")
        ok, msg = rt.verify_existing_assets(existing, built)
        self.assertFalse(ok)
        self.assertIn("Asset set mismatch", msg)

    def test_verify_asset_checksum_mismatch_rejected(self):
        existing, built = self._make_dirs()
        self._fill_assets(existing, built)
        with open(os.path.join(built, "plugin-linux-amd64"), "w") as f:
            f.write("DIFFERENT CONTENT")
        ok, msg = rt.verify_existing_assets(existing, built)
        self.assertFalse(ok)
        self.assertIn("Checksum mismatch", msg)


class TestCatalogUpdate(unittest.TestCase):
    def test_update_catalog_json(self):
        catalog = {
            "plugins": [
                {
                    "manifest": {
                        "plugin_id": "com.drondeseries.vio-virtual-library",
                        "version": "0.4.99",
                        "global_config_schema": [
                            {
                                "admin_form": {
                                    "fields": [
                                        {"name": "api_key", "control": "ADMIN_FORM_CONTROL_PASSWORD"}
                                    ]
                                }
                            }
                        ],
                    },
                    "checksums_url": "https://old/checksums.txt",
                    "binaries": {},
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "catalog.json")
            with open(path, "w") as f:
                json.dump(catalog, f)

            hashes = {
                "linux/amd64": "a" * 64,
                "linux/arm64": "b" * 64,
                "darwin/arm64": "c" * 64,
            }
            rt.update_catalog_json(path, "v0.4.100", hashes)

            with open(path) as f:
                updated = json.load(f)

            plugin = updated["plugins"][0]
            self.assertEqual(plugin["manifest"]["version"], "0.4.100")
            self.assertEqual(
                plugin["checksums_url"],
                "https://github.com/drondeseries/vio-virtual-library/releases/download/v0.4.100/checksums.txt",
            )
            self.assertEqual(plugin["binaries"]["linux/amd64"]["checksum"], "a" * 64)
            self.assertEqual(plugin["binaries"]["linux/arm64"]["checksum"], "b" * 64)
            self.assertEqual(plugin["binaries"]["darwin/arm64"]["checksum"], "c" * 64)
            field = plugin["manifest"]["global_config_schema"][0]["admin_form"]["fields"][0]
            self.assertEqual(field["control"], 3)


class TestCLI(unittest.TestCase):
    def test_cli_roundtrip(self):
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "release_tool.py")
        r = subprocess.run([sys.executable, script, "validate-semver", "0.4.100"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout.strip(), "VALID")

        r = subprocess.run([sys.executable, script, "compare-semver", "0.4.99", "0.4.100"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout.strip(), "LESS")


if __name__ == "__main__":
    unittest.main()
