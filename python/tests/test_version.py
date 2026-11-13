"""The version the SDK reports (__version__, and the x-retrace-client fallback used when package
metadata is unavailable) must match pyproject.toml, which is what PyPI publishes."""

import re
from pathlib import Path

import retrace

ROOT = Path(__file__).resolve().parents[1]


def _pyproject_version() -> str:
    text = (ROOT / "pyproject.toml").read_text()
    match = re.search(r'^\[project\][^\[]*?^version\s*=\s*"([^"]+)"', text, re.M | re.S)
    assert match, "no [project] version in pyproject.toml"
    return match.group(1)


def test_dunder_version_matches_pyproject():
    assert retrace.__version__ == _pyproject_version()


def test_client_id_fallback_matches_pyproject():
    source = (ROOT / "src" / "retrace" / "transport.py").read_text()
    fallback = re.search(r'^\s+_SDK_VERSION = "([^"]+)"', source, re.M)
    assert fallback, "transport.py fallback version not found"
    assert fallback.group(1) == _pyproject_version()
