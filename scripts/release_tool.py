#!/usr/bin/env python3
"""
Release engineering tool for vio-virtual-library.
Implements strict SemVer 2.0 validation & comparison, provenance generation,
asset integrity checking, and catalog update transformations.
"""

import sys
import re
import json
import hashlib
import os

SEMVER_REGEX = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)

REQUIRED_ASSETS = [
    "plugin-linux-amd64",
    "plugin-linux-arm64",
    "plugin-darwin-arm64",
    "checksums.txt",
    "provenance.json",
]


class SemVer:
    def __init__(self, version_str: str):
        match = SEMVER_REGEX.match(version_str.strip())
        if not match:
            raise ValueError(f"Invalid SemVer 2.0 version string: {version_str!r}")
        self.raw = version_str.strip()
        self.major = int(match.group(1))
        self.minor = int(match.group(2))
        self.patch = int(match.group(3))
        self.prerelease = match.group(4)
        self.build = match.group(5)

    def _prerelease_identifiers(self):
        if self.prerelease is None:
            return None
        parts = []
        for part in self.prerelease.split("."):
            if part.isdigit():
                parts.append((0, int(part)))  # numeric
            else:
                parts.append((1, part))  # alphanumeric
        return parts

    def __eq__(self, other):
        if not isinstance(other, SemVer):
            return NotImplemented
        return (
            (self.major, self.minor, self.patch, self._prerelease_identifiers())
            == (other.major, other.minor, other.patch, other._prerelease_identifiers())
        )

    def __lt__(self, other):
        if not isinstance(other, SemVer):
            return NotImplemented

        # 1. Compare major, minor, patch numerically
        if (self.major, self.minor, self.patch) != (other.major, other.minor, other.patch):
            return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

        # 2. Normal version > pre-release version
        if self.prerelease is None and other.prerelease is not None:
            return False
        if self.prerelease is not None and other.prerelease is None:
            return True
        if self.prerelease is None and other.prerelease is None:
            return False

        # 3. Compare pre-release identifiers
        self_parts = self._prerelease_identifiers()
        other_parts = other._prerelease_identifiers()

        for (s_type, s_val), (o_type, o_val) in zip(self_parts, other_parts):
            if (s_type, s_val) != (o_type, o_val):
                if s_type != o_type:
                    # Numeric identifiers have lower precedence than non-numeric
                    return s_type < o_type
                return s_val < o_val

        # 4. Larger set of pre-release fields has higher precedence
        return len(self_parts) < len(other_parts)

    def __gt__(self, other):
        if not isinstance(other, SemVer):
            return NotImplemented
        return other < self

    def __le__(self, other):
        return self < other or self == other

    def __ge__(self, other):
        return self > other or self == other

    def __str__(self):
        return self.raw


def validate_semver(version_str: str) -> bool:
    try:
        SemVer(version_str)
        return True
    except ValueError:
        return False


def compare_semver(v1: str, v2: str) -> str:
    """Returns 'GREATER' if v1 > v2, 'EQUAL' if v1 == v2, 'LESS' if v1 < v2."""
    sv1 = SemVer(v1)
    sv2 = SemVer(v2)
    if sv1 > sv2:
        return "GREATER"
    if sv1 < sv2:
        return "LESS"
    return "EQUAL"


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def generate_provenance(
    release_dir: str,
    tag: str,
    commit_sha: str,
    go_version: str,
    built_at: str | None = None,
) -> dict:
    if built_at is None:
        built_at = ""
    provenance = {
        "version": tag.lstrip("v"),
        "tag": tag,
        "commit_sha": commit_sha,
        "toolchain": go_version,
        "built_at": built_at,
        "artifacts": {},
    }
    for fname in ["plugin-linux-amd64", "plugin-linux-arm64", "plugin-darwin-arm64"]:
        fpath = os.path.join(release_dir, fname)
        if os.path.isfile(fpath):
            provenance["artifacts"][fname] = {
                "sha256": sha256_file(fpath),
                "size_bytes": os.path.getsize(fpath),
            }
    return provenance


def verify_existing_assets(existing_dir: str, built_dir: str) -> tuple[bool, str]:
    """
    Asserts exact asset set matches REQUIRED_ASSETS and every file has matching SHA-256.
    """
    existing_files = set(os.listdir(existing_dir))
    expected_files = set(REQUIRED_ASSETS)

    if existing_files != expected_files:
        return False, f"Asset set mismatch: existing={sorted(existing_files)}, expected={sorted(expected_files)}"

    for fname in REQUIRED_ASSETS:
        existing_path = os.path.join(existing_dir, fname)
        built_path = os.path.join(built_dir, fname)
        if not os.path.exists(built_path):
            return False, f"Built directory missing required asset {fname}"
        h_exist = sha256_file(existing_path)
        h_built = sha256_file(built_path)
        if h_exist != h_built:
            return False, f"Checksum mismatch for {fname}: existing={h_exist}, built={h_built}"

    return True, "All asset checksums and exact asset set match perfectly."


def update_catalog_json(catalog_path: str, tag: str, hashes: dict[str, str]) -> None:
    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    ver = tag.lstrip("v")
    plugin = catalog["plugins"][0]
    plugin["manifest"]["version"] = ver
    plugin["checksums_url"] = f"https://github.com/drondeseries/vio-virtual-library/releases/download/{tag}/checksums.txt"

    binaries = plugin.get("binaries", {})
    binaries["linux/amd64"] = {
        "url": f"https://github.com/drondeseries/vio-virtual-library/releases/download/{tag}/plugin-linux-amd64",
        "checksum": hashes["linux/amd64"],
    }
    binaries["linux/arm64"] = {
        "url": f"https://github.com/drondeseries/vio-virtual-library/releases/download/{tag}/plugin-linux-arm64",
        "checksum": hashes["linux/arm64"],
    }
    binaries["darwin/arm64"] = {
        "url": f"https://github.com/drondeseries/vio-virtual-library/releases/download/{tag}/plugin-darwin-arm64",
        "checksum": hashes["darwin/arm64"],
    }
    plugin["binaries"] = binaries

    # Normalize admin form control enum strings to numbers if needed
    for cap in plugin.get("manifest", {}).get("global_config_schema", []):
        admin_form = cap.get("admin_form")
        if isinstance(admin_form, dict):
            for field in admin_form.get("fields", []):
                ctrl = field.get("control")
                if isinstance(ctrl, str):
                    ctrl_map = {
                        "ADMIN_FORM_CONTROL_UNSPECIFIED": 0,
                        "ADMIN_FORM_CONTROL_TEXT": 1,
                        "ADMIN_FORM_CONTROL_TEXTAREA": 2,
                        "ADMIN_FORM_CONTROL_PASSWORD": 3,
                        "ADMIN_FORM_CONTROL_NUMBER": 4,
                        "ADMIN_FORM_CONTROL_SWITCH": 5,
                        "ADMIN_FORM_CONTROL_SELECT": 6,
                        "ADMIN_FORM_CONTROL_MULTI_SELECT": 7,
                    }
                    field["control"] = ctrl_map.get(ctrl, 0)

    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2)
        f.write("\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: release_tool.py <command> [args...]", file=sys.stderr)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "validate-semver":
        if len(sys.argv) != 3:
            print("Usage: release_tool.py validate-semver <version>", file=sys.stderr)
            sys.exit(1)
        v = sys.argv[2]
        if not validate_semver(v):
            print(f"Invalid SemVer 2.0 version: {v}", file=sys.stderr)
            sys.exit(1)
        print("VALID")

    elif cmd == "compare-semver":
        if len(sys.argv) != 4:
            print("Usage: release_tool.py compare-semver <v1> <v2>", file=sys.stderr)
            sys.exit(1)
        try:
            rel = compare_semver(sys.argv[2], sys.argv[3])
            print(rel)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    elif cmd == "generate-provenance":
        if len(sys.argv) not in (6, 7):
            print("Usage: release_tool.py generate-provenance <release_dir> <tag> <commit_sha> <go_version> [built_at]", file=sys.stderr)
            sys.exit(1)
        release_dir, tag, commit_sha, go_version = sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
        built_at = sys.argv[6] if len(sys.argv) == 7 else ""
        prov = generate_provenance(release_dir, tag, commit_sha, go_version, built_at)
        prov_path = os.path.join(release_dir, "provenance.json")
        with open(prov_path, "w", encoding="utf-8") as f:
            json.dump(prov, f, indent=2)
            f.write("\n")
        print(f"Generated {prov_path}")

    elif cmd == "verify-existing-assets":
        if len(sys.argv) != 4:
            print("Usage: release_tool.py verify-existing-assets <existing_dir> <built_dir>", file=sys.stderr)
            sys.exit(1)
        ok, msg = verify_existing_assets(sys.argv[2], sys.argv[3])
        if not ok:
            print(f"ERROR: {msg}", file=sys.stderr)
            sys.exit(1)
        print(msg)

    elif cmd == "update-catalog":
        if len(sys.argv) != 7:
            print("Usage: release_tool.py update-catalog <catalog_json> <tag> <amd64_hash> <arm64_hash> <darwin_hash>", file=sys.stderr)
            sys.exit(1)
        catalog_path, tag, amd64, arm64, darwin = sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]
        hashes = {
            "linux/amd64": amd64,
            "linux/arm64": arm64,
            "darwin/arm64": darwin,
        }
        update_catalog_json(catalog_path, tag, hashes)
        print(f"Updated {catalog_path} for {tag}")

    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
