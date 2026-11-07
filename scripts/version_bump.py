#!/usr/bin/env python3
"""Bump the unified version across all manifests.

Keeps every package in lockstep:
    Cargo.toml                      [workspace.package] version
    python/pyproject.toml           [project] version
    python/light_telemetry/__init__.py  __version__

Usage:
    scripts/version_bump.py patch|minor|major|X.Y.Z [--commit] [--tag] [--dry-run]

--commit creates `Bump version to X.Y.Z`; --tag adds `vX.Y.Z` (implies
--commit when no commit exists for the bump). Stdlib only.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TARGETS: list[tuple[Path, str, str]] = [
    # (file, regex, replacement template)
    (
        ROOT / "Cargo.toml",
        r'(\[workspace\.package\][^\[]*?version\s*=\s*")([^"]+)(")',
        r"\g<1>{v}\g<3>",
    ),
    (
        ROOT / "python" / "pyproject.toml",
        r'(\[project\][^\[]*?version\s*=\s*")([^"]+)(")',
        r"\g<1>{v}\g<3>",
    ),
    (
        ROOT / "python" / "light_telemetry" / "__init__.py",
        r'(__version__\s*=\s*")([^"]+)(")',
        r"\g<1>{v}\g<3>",
    ),
]

VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


def current_version() -> str:
    """Read the workspace version — single source of truth."""
    text = (ROOT / "Cargo.toml").read_text()
    m = re.search(r'\[workspace\.package\][^\[]*?version\s*=\s*"([^"]+)"', text)
    if not m:
        sys.exit("cannot read workspace version from Cargo.toml")
    return m.group(1)


def bump(cur: str, part: str) -> str:
    m = VERSION_RE.match(cur)
    if not m:
        sys.exit(f"current version {cur!r} is not X.Y.Z")
    major, minor, patch = map(int, m.groups())
    return {
        "major": f"{major + 1}.0.0",
        "minor": f"{major}.{minor + 1}.0",
        "patch": f"{major}.{minor}.{patch + 1}",
    }[part]


def apply(new: str, dry_run: bool) -> list[Path]:
    changed: list[Path] = []
    for path, pat, tmpl in TARGETS:
        text = path.read_text()
        out, n = re.subn(pat, tmpl.format(v=new), text, count=1, flags=re.S)
        if n != 1:
            sys.exit(f"pattern not found in {path}")
        if out != text:
            changed.append(path.relative_to(ROOT))
            if not dry_run:
                path.write_text(out)
        print(f"  {path.relative_to(ROOT)} -> {new}")
    return changed


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("version", help="patch|minor|major or explicit X.Y.Z")
    ap.add_argument("--commit", action="store_true")
    ap.add_argument("--tag", action="store_true", help="create vX.Y.Z tag")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cur = current_version()
    if args.version in ("patch", "minor", "major"):
        new = bump(cur, args.version)
    elif VERSION_RE.match(args.version):
        new = args.version
    else:
        ap.error("version must be patch|minor|major or X.Y.Z")
        return 2

    print(f"{cur} -> {new}")
    changed = apply(new, args.dry_run)
    if args.dry_run:
        return 0

    if args.commit or args.tag:
        subprocess.run(
            ["git", "add", *[str(p) for p in changed]], cwd=ROOT, check=True
        )
        subprocess.run(
            ["git", "commit", "-m", f"Bump version to {new}"], cwd=ROOT, check=True
        )
    if args.tag:
        subprocess.run(["git", "tag", f"v{new}"], cwd=ROOT, check=True)
        print(f"tagged v{new}")

    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
