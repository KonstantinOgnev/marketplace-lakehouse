#!/usr/bin/env python3
"""Fast repository checks used locally and by Azure Pipelines."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_DIRECTORIES = ["src", "notebooks", "tests", "resources", "config", "docs"]
SECRET_PATTERNS = {
    "Databricks PAT": re.compile(r"\bdapi[0-9a-fA-F]{32,}\b"),
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "hardcoded secret": re.compile(
        r"(?i)\b(?:password|secret|token|client_secret|api_key)\b\s*[:=]\s*[\"'][^\"'${}]{8,}[\"']"
    ),
}


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


for directory in REQUIRED_DIRECTORIES:
    if not (ROOT / directory).is_dir():
        fail(f"Missing required directory: {directory}")

bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
if bundle.get("bundle", {}).get("name") != "marketplace_lakehouse":
    fail("Unexpected or missing bundle.name")
if set(bundle.get("targets", {})) != {"dev", "prod"}:
    fail("Bundle must define exactly dev and prod targets")

for notebook in (ROOT / "notebooks").rglob("*.py"):
    text = notebook.read_text(encoding="utf-8", errors="replace")
    if not text.startswith("# Databricks notebook source"):
        fail(f"Not a Databricks source notebook: {notebook.relative_to(ROOT)}")

for path in ROOT.rglob("*"):
    if not path.is_file() or any(part in {".git", ".venv", "__pycache__"} for part in path.parts):
        continue
    if path.suffix.lower() not in {".py", ".sql", ".yml", ".yaml", ".json", ".toml", ".md"}:
        continue
    text = path.read_text(encoding="utf-8", errors="replace")
    for rule, pattern in SECRET_PATTERNS.items():
        if pattern.search(text):
            fail(f"Potential {rule} in {path.relative_to(ROOT)}")

print("Repository validation passed")
