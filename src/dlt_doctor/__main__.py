"""Entrypoint for running dlt-doctor via python -m dlt_doctor."""

from __future__ import annotations

import sys

from dlt_doctor.cli.main import main

if __name__ == "__main__":
    sys.exit(main())
