"""Tests for Jupyter and Databricks notebook parsing."""

from __future__ import annotations

import json
from pathlib import Path
import pytest

from dlt_doctor.notebook import extract_notebook_code, is_notebook_file


def test_is_notebook_file() -> None:
    assert is_notebook_file("pipeline.ipynb") is True
    assert is_notebook_file(Path("sub/dir/notebook.IPYNB")) is True
    assert is_notebook_file("pipeline.py") is False
    assert is_notebook_file("query.sql") is False


def test_extract_notebook_code(tmp_path: Path) -> None:
    nb_content = {
        "cells": [
            {
                "cell_type": "markdown",
                "source": ["# Silver Orders Ingestion\n", "Documentation cell\n"],
            },
            {
                "cell_type": "code",
                "source": [
                    "%pip install dlt-doctor\n",
                    "import dlt\n",
                    "from pyspark.sql import functions as F\n",
                ],
            },
            {
                "cell_type": "code",
                "source": (
                    "@dlt.table\n"
                    "def silver_orders():\n"
                    "    return spark.readStream.table('bronze_orders')\n"
                ),
            },
        ],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 2,
    }

    nb_file = tmp_path / "test_pipeline.ipynb"
    nb_file.write_text(json.dumps(nb_content), encoding="utf-8")

    code = extract_notebook_code(nb_file)
    assert "# [magic skipped] %pip install dlt-doctor" in code
    assert "import dlt" in code
    assert "def silver_orders():" in code


def test_extract_notebook_missing_file() -> None:
    with pytest.raises(FileNotFoundError):
        extract_notebook_code("non_existent_notebook.ipynb")


def test_extract_notebook_invalid_json(tmp_path: Path) -> None:
    corrupted_file = tmp_path / "corrupted.ipynb"
    corrupted_file.write_text("NOT A VALID JSON", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid JSON"):
        extract_notebook_code(corrupted_file)
