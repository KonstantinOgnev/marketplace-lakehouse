from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_required_repository_directories_exist():
    for directory in ["src", "notebooks", "tests", "resources", "config", "docs"]:
        assert (ROOT / directory).is_dir(), f"Missing repository directory: {directory}"


def test_bundle_root_file_exists():
    assert (ROOT / "databricks.yml").is_file()


def test_smoke_notebook_is_databricks_source():
    text = (ROOT / "notebooks" / "ci" / "smoke_test.py").read_text(encoding="utf-8")
    assert text.startswith("# Databricks notebook source")
