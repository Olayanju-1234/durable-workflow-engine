"""Prints the SDKs' shared version, or exits 1 if the packages disagree.

Checks sdks/typescript/package.json, sdks/python/pyproject.toml, sdks/java/pom.xml and
the SDK_VERSION strings each SDK reports to the engine. With --release, also requires
a "## [<version>]" section in CHANGELOG.md.

    python3 scripts/sdk-version.py [--release]
"""

import json
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(__file__).resolve().parent.parent
sdks = root / "sdks"


def first(pattern: str, path: pathlib.Path) -> str:
    m = re.search(pattern, path.read_text(), re.M)
    if not m:
        sys.exit(f"sdk-version: no match for {pattern!r} in {path.relative_to(root)}")
    return m.group(1)


pom = ET.parse(sdks / "java" / "pom.xml").getroot()
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
found = {
    "typescript/package.json": json.loads((sdks / "typescript" / "package.json").read_text())["version"],
    "python/pyproject.toml": first(r'^version\s*=\s*"([^"]+)"', sdks / "python" / "pyproject.toml"),
    "java/pom.xml": pom.find("m:version", ns).text,
    "typescript/workflowd.cjs": first(r"typescript/(\d+\.\d+\.\d+[^'\"]*)", sdks / "typescript" / "workflowd.cjs"),
    "python/workflowd.py": first(r"python/(\d+\.\d+\.\d+[^'\"]*)", sdks / "python" / "workflowd.py"),
    "java/Workflowd.java": first(r"java/(\d+\.\d+\.\d+[^'\"]*)", sdks / "java" / "Workflowd.java"),
}
if len(set(found.values())) != 1:
    for where, v in found.items():
        print(f"  {where}: {v}", file=sys.stderr)
    sys.exit("sdk-version: the SDK versions disagree")

version = next(iter(found.values()))
if "--release" in sys.argv[1:]:
    # Anchored to a line start: prose that merely mentions the heading must not count.
    if not re.search(rf"^## \[{re.escape(version)}\]", (root / "CHANGELOG.md").read_text(), re.M):
        sys.exit(f"sdk-version: CHANGELOG.md has no '## [{version}]' section")
print(version)
