"""Tests for dlt-doctor command line interface."""

from pathlib import Path
from unittest.mock import patch

import pytest

from dlt_doctor.cli import main


def test_cli_analyze_clean_file(tmp_path: Path) -> None:
    test_file = tmp_path / "clean_pipeline.py"
    test_file.write_text("df = spark.table('orders').select('id', 'amount')", encoding="utf-8")

    test_args = ["dlt-doctor", "analyze", str(test_file)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_analyze_critical_udf(tmp_path: Path) -> None:
    test_file = tmp_path / "bad_pipeline.py"
    test_file.write_text(
        """
from pyspark.sql.functions import udf
@udf
def clean_it(x):
    return x
""",
        encoding="utf-8",
    )

    test_args = ["dlt-doctor", "analyze", str(test_file)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 1


def test_cli_analyze_fail_on_none(tmp_path: Path) -> None:
    test_file = tmp_path / "bad_pipeline.py"
    test_file.write_text(
        """
from pyspark.sql.functions import udf
@udf
def clean_it(x):
    return x
""",
        encoding="utf-8",
    )

    test_args = ["dlt-doctor", "analyze", str(test_file), "--fail-on", "NONE"]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_analyze_custom_config_flag(tmp_path: Path) -> None:
    test_file = tmp_path / "bad_pipeline.py"
    test_file.write_text(
        """
from pyspark.sql.functions import udf
@udf
def clean_it(x):
    return x
""",
        encoding="utf-8",
    )
    cfg_file = tmp_path / "my_config.toml"
    cfg_file.write_text(
        """[tool.dlt-doctor]
ignore = ["PYSPARK-001"]
""",
        encoding="utf-8",
    )

    test_args = ["dlt-doctor", "analyze", str(test_file), "--config", str(cfg_file)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_restate() -> None:
    test_args = [
        "dlt-doctor",
        "restate",
        "--table",
        "main.gold.fact_orders",
        "--late-date",
        "2026-10-25",
        "--window",
        "7",
        "--source",
        "source_recalculated",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_cdc() -> None:
    test_args = [
        "dlt-doctor",
        "cdc",
        "--table",
        "main.silver.orders",
        "--keys",
        "order_id",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_compact() -> None:
    test_args = [
        "dlt-doctor",
        "compact",
        "--table",
        "main.gold.orders",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_uc_migrate(tmp_path: Path) -> None:
    test_file = tmp_path / "legacy.py"
    test_file.write_text("df = spark.read.json('/mnt/raw/orders')", encoding="utf-8")

    test_args = ["dlt-doctor", "uc-migrate", str(test_file)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 1  # critical finding for dbfs mount


def test_cli_cluster() -> None:
    test_args = [
        "dlt-doctor",
        "cluster",
        "--table",
        "main.gold.orders",
        "--cols",
        "customer_id",
        "order_date",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_vacuum() -> None:
    test_args = [
        "dlt-doctor",
        "vacuum",
        "--table",
        "main.silver.orders",
        "--retention",
        "168",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_schema() -> None:
    test_args = [
        "dlt-doctor",
        "schema",
        "--table",
        "main.silver.orders",
        "--incoming-cols",
        "id:bigint",
        "email:string",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_cost() -> None:
    test_args = [
        "dlt-doctor",
        "cost",
        "--files-per-hour",
        "1000",
        "--avg-file-mb",
        "5.0",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_spill() -> None:
    test_args = [
        "dlt-doctor",
        "spill",
        "--dataset-gb",
        "50.0",
        "--cores",
        "16",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_history() -> None:
    test_args = [
        "dlt-doctor",
        "history",
        "--table",
        "main.silver.orders",
        "--commits-per-hour",
        "60",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_checkpoint() -> None:
    test_args = [
        "dlt-doctor",
        "checkpoint",
        "--dir",
        "/Volumes/checkpoints/orders",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_analyze_directory(tmp_path: Path) -> None:
    pipeline_dir = tmp_path / "pipelines"
    pipeline_dir.mkdir()
    (pipeline_dir / "p1.py").write_text("df = spark.table('t1')", encoding="utf-8")
    (pipeline_dir / "p2.py").write_text("df = spark.table('t2')", encoding="utf-8")

    test_args = ["dlt-doctor", "analyze", str(pipeline_dir)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_analyze_notebook(tmp_path: Path) -> None:
    import json

    nb_file = tmp_path / "orders.ipynb"
    nb_file.write_text(
        json.dumps({"cells": [{"cell_type": "code", "source": ["df = spark.table('orders')\n"]}]}),
        encoding="utf-8",
    )

    test_args = ["dlt-doctor", "analyze", str(nb_file)]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_analyze_auto_heal_preview_and_write(tmp_path: Path) -> None:
    code = """import dlt
@dlt.table(name="test_heal_tbl")
@dlt.expect_or_drop("valid_id", "id > 0")
def test_heal_tbl():
    return dlt.read_stream("bronze")
"""
    pipeline_file = tmp_path / "heal_pipeline.py"
    pipeline_file.write_text(code, encoding="utf-8")

    # Preview by default (does not modify file)
    test_args = ["dlt-doctor", "analyze", str(pipeline_file), "--auto-heal"]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0
    assert pipeline_file.read_text(encoding="utf-8") == code

    # With --write and --quarantine-suffix, modifies file
    test_args_apply = [
        "dlt-doctor",
        "analyze",
        str(pipeline_file),
        "--auto-heal",
        "--write",
        "--quarantine-suffix",
        "_dlq",
    ]
    with patch("sys.argv", test_args_apply):
        exit_code = main()
        assert exit_code == 0
    healed = pipeline_file.read_text(encoding="utf-8")
    assert "_failed_rules" in healed
    assert "test_heal_tbl_dlq" in healed


def test_cli_analyze_auto_heal_refusal_exits_nonzero(tmp_path: Path) -> None:
    code = """import dlt
@dlt.table
@dlt.expect_all_or_drop(fetch_remote_rules())
def dynamic_pipe():
    return dlt.read_stream("bronze")
"""
    pipeline_file = tmp_path / "dynamic.py"
    pipeline_file.write_text(code, encoding="utf-8")

    test_args = ["dlt-doctor", "analyze", str(pipeline_file), "--auto-heal"]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code != 0


def test_cli_heal_check_and_custom_suffix(tmp_path: Path) -> None:
    code = """import dlt
@dlt.table(name="users")
@dlt.expect_or_drop("valid_u", "id IS NOT NULL")
def users():
    return dlt.read_stream("raw")
"""
    pipeline_file = tmp_path / "users.py"
    pipeline_file.write_text(code, encoding="utf-8")

    # --check exits 1 when healing is needed
    check_args = ["dlt-doctor", "heal", str(pipeline_file), "--check"]
    with patch("sys.argv", check_args):
        exit_code = main()
        assert exit_code == 1

    # heal with custom suffix
    write_args = [
        "dlt-doctor",
        "heal",
        str(pipeline_file),
        "--write",
        "--quarantine-suffix",
        "_dlq",
    ]
    with patch("sys.argv", write_args):
        exit_code = main()
        assert exit_code == 0
    healed = pipeline_file.read_text(encoding="utf-8")
    assert "users_dlq" in healed

    # Running --check on healed file now exits 0
    with patch("sys.argv", check_args):
        exit_code = main()
        assert exit_code == 0


def test_cli_heal_exit_code_3_cannot_heal(tmp_path: Path) -> None:
    """When a file has dynamic expectations that cannot be resolved, exit code is 3."""
    code = """import dlt
@dlt.table
@dlt.expect_all_or_drop(fetch_remote_rules())
def dynamic_pipe():
    return dlt.read_stream("bronze")
"""
    pipeline_file = tmp_path / "unhealable.py"
    pipeline_file.write_text(code, encoding="utf-8")

    check_args = ["dlt-doctor", "heal", str(pipeline_file), "--check"]
    with patch("sys.argv", check_args):
        exit_code = main()
        assert exit_code == 3


def test_cli_heal_directory_support(tmp_path: Path) -> None:
    """heal command must support directories for CI gates."""
    pipe_dir = tmp_path / "pipes"
    pipe_dir.mkdir()
    (pipe_dir / "p1.py").write_text(
        """import dlt
@dlt.table
@dlt.expect_or_drop("v1", "x > 0")
def p1():
    return dlt.read_stream("s1")
""",
        encoding="utf-8",
    )
    (pipe_dir / "p2.py").write_text("import dlt\n# already compliant", encoding="utf-8")

    # In check mode, directory with unhealed file exits 1
    with patch("sys.argv", ["dlt-doctor", "heal", str(pipe_dir), "--check"]):
        assert main() == 1

    # In write mode, directory heals files and exits 0
    with patch("sys.argv", ["dlt-doctor", "heal", str(pipe_dir), "--write"]):
        assert main() == 0

    # Running --check on now-healed directory exits 0
    with patch("sys.argv", ["dlt-doctor", "heal", str(pipe_dir), "--check"]):
        assert main() == 0


def test_cli_heal_atomic_write_failure_leaves_disk_untouched(tmp_path: Path) -> None:
    """If any file in directory cannot be healed, zero files are written and exit code is 3."""
    pipe_dir = tmp_path / "pipes"
    pipe_dir.mkdir()
    good_code = """import dlt
@dlt.table
@dlt.expect_or_drop("valid_val", "v > 0")
def good():
    return dlt.read_stream("s1")
"""
    bad_code = """import dlt
@dlt.table
@dlt.expect_all_or_drop(fetch_remote_rules())
def bad():
    return dlt.read_stream("s2")
"""
    f_good = pipe_dir / "p_good.py"
    f_bad = pipe_dir / "p_bad.py"
    f_good.write_text(good_code, encoding="utf-8")
    f_bad.write_text(bad_code, encoding="utf-8")

    # In write mode, should fail with 3 and leave p_good.py completely untouched
    with patch("sys.argv", ["dlt-doctor", "heal", str(pipe_dir), "--write"]):
        exit_code = main()
        assert exit_code == 3

    assert f_good.read_text(encoding="utf-8") == good_code
    assert f_bad.read_text(encoding="utf-8") == bad_code


def test_cli_heal_skips_ignored_directories(tmp_path: Path) -> None:
    """Directories like .venv, node_modules, build should be skipped by discovery."""
    root_dir = tmp_path / "project"
    root_dir.mkdir()

    venv_dir = root_dir / ".venv" / "lib"
    venv_dir.mkdir(parents=True)
    (venv_dir / "bad.py").write_text(
        """import dlt
@dlt.table
@dlt.expect_all_or_drop(fetch_remote_rules())
def bad():
    return dlt.read_stream("s2")
""",
        encoding="utf-8",
    )

    clean_file = root_dir / "pipeline.py"
    clean_file.write_text("import dlt\n# all good", encoding="utf-8")

    with patch("sys.argv", ["dlt-doctor", "heal", str(root_dir), "--check"]):
        assert main() == 0


def test_cli_heal_handles_unicode_decode_error(tmp_path: Path) -> None:
    """Non-UTF-8 files report cannot auto-heal and exit code 3."""
    pipe_dir = tmp_path / "pipes"
    pipe_dir.mkdir()
    bad_encoding_file = pipe_dir / "corrupted.py"
    bad_encoding_file.write_bytes(b"\xff\xfe\x00\x00\x80")

    with patch("sys.argv", ["dlt-doctor", "heal", str(pipe_dir), "--check"]):
        exit_code = main()
        assert exit_code == 3


def test_cli_heal_suppressions(tmp_path: Path) -> None:
    """Files with inline suppressions, config ignores, or un-regulated tables are compliant."""
    pipe_dir = tmp_path / "pipes"
    pipe_dir.mkdir()

    # Inline file-level ignore
    f_suppressed = pipe_dir / "suppressed.py"
    f_suppressed.write_text(
        """# dlt-doctor: ignore[DD001]
import dlt
@dlt.table
@dlt.expect_or_drop("v1", "x > 0")
def s1():
    return dlt.read_stream("source")
""",
        encoding="utf-8",
    )

    with patch("sys.argv", ["dlt-doctor", "heal", str(f_suppressed), "--check"]):
        assert main() == 0

    # pyproject.toml ignore rule
    proj_dir = tmp_path / "proj_with_config"
    proj_dir.mkdir()
    (proj_dir / "pyproject.toml").write_text(
        """[tool.dlt-doctor]
ignore = ["DD001"]
""",
        encoding="utf-8",
    )
    f_proj = proj_dir / "pipe.py"
    f_proj.write_text(
        """import dlt
@dlt.table
@dlt.expect_or_drop("v1", "x > 0")
def s1():
    return dlt.read_stream("source")
""",
        encoding="utf-8",
    )
    with patch("sys.argv", ["dlt-doctor", "heal", str(proj_dir), "--check"]):
        assert main() == 0


def test_cli_heal_sql_files(tmp_path: Path) -> None:
    """SQL files with ON VIOLATION DROP ROW exit 3; compliant or suppressed SQL exit 0."""
    sql_dir = tmp_path / "sql_pipes"
    sql_dir.mkdir()

    # 1. SQL with ON VIOLATION DROP ROW
    f_drop = sql_dir / "drop_pipe.sql"
    f_drop.write_text(
        """CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
) AS SELECT * FROM STREAM(bronze_orders);
""",
        encoding="utf-8",
    )

    with patch("sys.argv", ["dlt-doctor", "heal", str(f_drop), "--check"]):
        assert main() == 3

    # 2. SQL with inline suppression
    f_drop_suppressed = sql_dir / "drop_suppressed.sql"
    f_drop_suppressed.write_text(
        """-- dlt-doctor: ignore[DD001]
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
) AS SELECT * FROM STREAM(bronze_orders);
""",
        encoding="utf-8",
    )
    with patch("sys.argv", ["dlt-doctor", "heal", str(f_drop_suppressed), "--check"]):
        assert main() == 0

    # 3. SQL with FAIL UPDATE (already compliant)
    f_clean = sql_dir / "clean_pipe.sql"
    f_clean.write_text(
        """CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE
) AS SELECT * FROM STREAM(bronze_orders);
""",
        encoding="utf-8",
    )
    with patch("sys.argv", ["dlt-doctor", "heal", str(f_clean), "--check"]):
        assert main() == 0


def test_cli_heal_target_under_build_ancestor(tmp_path: Path) -> None:
    """Ancestors named 'build' or '.venv' must not cause target files to be skipped."""
    build_dir = tmp_path / "build" / "pipelines"
    build_dir.mkdir(parents=True)
    pipe = build_dir / "pipe.py"
    pipe.write_text(
        """import dlt
@dlt.table
@dlt.expect_or_drop("valid_val", "v > 0")
def t():
    return dlt.read_stream("s")
""",
        encoding="utf-8",
    )

    # Must be discovered and report needing heal (exit 1), not exit 0 (no files found)
    with patch("sys.argv", ["dlt-doctor", "heal", str(build_dir), "--check"]):
        assert main() == 1


def test_cli_heal_per_file_ignores_glob(tmp_path: Path) -> None:
    """per_file_ignores with path globs (e.g. 'pipes/*') must suppress heal check."""
    pipes_dir = tmp_path / "pipes"
    pipes_dir.mkdir()
    pipe = pipes_dir / "orders.py"
    pipe.write_text(
        """import dlt
@dlt.table
@dlt.expect_or_drop("valid_val", "v > 0")
def orders():
    return dlt.read_stream("s")
""",
        encoding="utf-8",
    )

    cfg_file = tmp_path / "custom.toml"
    cfg_file.write_text(
        """[tool.dlt-doctor.per_file_ignores]
"pipes/*" = ["DD001"]
""",
        encoding="utf-8",
    )

    # With glob ignore matching pipes/*, heal --check exits 0
    with patch(
        "sys.argv",
        ["dlt-doctor", "heal", str(pipes_dir), "--check", "--config", str(cfg_file)],
    ):
        assert main() == 0


def test_cli_heal_preserves_crlf_line_endings(tmp_path: Path) -> None:
    """Heal --write must preserve CRLF line endings without introducing double returns."""
    pipe = tmp_path / "crlf_pipe.py"
    code_crlf = (
        "import dlt\r\n"
        "@dlt.table\r\n"
        '@dlt.expect_or_drop("valid_val", "v > 0")\r\n'
        "def t():\r\n"
        '    return dlt.read_stream("s")\r\n'
    )
    pipe.write_bytes(code_crlf.encode("utf-8"))

    with patch("sys.argv", ["dlt-doctor", "heal", str(pipe), "--write"]):
        assert main() == 0

    raw_bytes = pipe.read_bytes()
    assert b"\r\n" in raw_bytes
    assert b"\r\r\n" not in raw_bytes
    assert raw_bytes.count(b"\r\n") == raw_bytes.count(b"\n")


def test_cli_heal_multitable_sql_regulated_compliance(tmp_path: Path) -> None:
    """Multi-table SQL files evaluate DROP ROW per table against regulated_tables."""
    sql_file = tmp_path / "multi_table.sql"
    sql_file.write_text(
        """CREATE OR REFRESH STREAMING TABLE unregulated_t (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
) AS SELECT * FROM STREAM(bronze_s);

CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
) AS SELECT * FROM STREAM(bronze_orders);
""",
        encoding="utf-8",
    )

    cfg_file = tmp_path / "custom.toml"
    cfg_file.write_text(
        """[tool.dlt-doctor]
regulated_tables = ["silver_orders"]
""",
        encoding="utf-8",
    )

    # Table 2 is regulated and has ON VIOLATION DROP ROW -> exits 3
    with patch(
        "sys.argv",
        ["dlt-doctor", "heal", str(sql_file), "--check", "--config", str(cfg_file)],
    ):
        assert main() == 3


def test_cli_analyze_fix_honors_config_ignore(tmp_path: Path) -> None:
    """analyze --fix must honor ignore rules in config without making changes."""
    pipe = tmp_path / "orders.py"
    original_code = (
        "import dlt\n"
        "@dlt.table\n"
        '@dlt.expect_or_drop("v1", "x > 0")\n'
        "def orders():\n"
        '    return dlt.read_stream("s")\n'
    )
    pipe.write_text(original_code, encoding="utf-8")

    cfg_file = tmp_path / "custom.toml"
    cfg_file.write_text(
        """[tool.dlt-doctor]
ignore = ["DD001"]
""",
        encoding="utf-8",
    )

    with patch(
        "sys.argv",
        [
            "dlt-doctor",
            "analyze",
            str(pipe),
            "--fix",
            "--write",
            "--config",
            str(cfg_file),
        ],
    ):
        assert main() == 0

    # Code on disk unchanged because rule was ignored
    assert pipe.read_text(encoding="utf-8") == original_code


def test_cli_analyze_fix_honors_inline_suppression(tmp_path: Path) -> None:
    """analyze --fix must honor inline suppression comments without modifying file."""
    pipe = tmp_path / "suppressed.py"
    original_code = (
        "# dlt-doctor: ignore[DD001]\n"
        "import dlt\n"
        "@dlt.table\n"
        '@dlt.expect_or_drop("v1", "x > 0")\n'
        "def suppressed():\n"
        '    return dlt.read_stream("s")\n'
    )
    pipe.write_text(original_code, encoding="utf-8")

    with patch(
        "sys.argv",
        ["dlt-doctor", "analyze", str(pipe), "--fix", "--write"],
    ):
        assert main() == 0

    assert pipe.read_text(encoding="utf-8") == original_code


def test_cli_analyze_fix_honors_per_file_ignores(tmp_path: Path) -> None:
    """analyze --fix must honor per_file_ignores with path globs."""
    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    pipe = legacy_dir / "old_pipe.py"
    original_code = (
        "import dlt\n"
        "@dlt.table\n"
        '@dlt.expect_or_drop("v1", "x > 0")\n'
        "def old_pipe():\n"
        '    return dlt.read_stream("s")\n'
    )
    pipe.write_text(original_code, encoding="utf-8")

    cfg_file = tmp_path / "custom.toml"
    cfg_file.write_text(
        """[tool.dlt-doctor.per_file_ignores]
"legacy/*" = ["DD001"]
""",
        encoding="utf-8",
    )

    with patch(
        "sys.argv",
        [
            "dlt-doctor",
            "analyze",
            str(pipe),
            "--fix",
            "--write",
            "--config",
            str(cfg_file),
        ],
    ):
        assert main() == 0

    assert pipe.read_text(encoding="utf-8") == original_code


def test_cli_replay_partition_val_typed(capsys: pytest.CaptureFixture[str]) -> None:
    """CLI replay must cast partition_val based on --partition-val-type."""
    # 1. Integer partition value
    with patch(
        "sys.argv",
        [
            "dlt-doctor",
            "replay",
            "--quarantine",
            "main.bronze.orders_quarantine",
            "--target",
            "main.silver.orders",
            "--keys",
            "id",
            "--valid-filter",
            "amount > 0",
            "--allow-resurrection",
            "--partition-col",
            "batch_id",
            "--partition-val",
            "42",
            "--partition-val-type",
            "int",
        ],
    ):
        assert main() == 0

    out = capsys.readouterr().out
    assert "target.batch_id = 42" in out
    assert "target.batch_id = '42'" not in out

    # 2. Boolean partition value
    with patch(
        "sys.argv",
        [
            "dlt-doctor",
            "replay",
            "--quarantine",
            "main.bronze.orders_quarantine",
            "--target",
            "main.silver.orders",
            "--keys",
            "id",
            "--valid-filter",
            "amount > 0",
            "--allow-resurrection",
            "--partition-col",
            "is_active",
            "--partition-val",
            "true",
            "--partition-val-type",
            "bool",
        ],
    ):
        assert main() == 0

    out_bool = capsys.readouterr().out
    assert "target.is_active = true" in out_bool

    # 3. Invalid integer partition value exits 2
    with patch(
        "sys.argv",
        [
            "dlt-doctor",
            "replay",
            "--quarantine",
            "main.bronze.orders_quarantine",
            "--target",
            "main.silver.orders",
            "--keys",
            "id",
            "--valid-filter",
            "amount > 0",
            "--allow-resurrection",
            "--partition-col",
            "batch_id",
            "--partition-val",
            "not_an_int",
            "--partition-val-type",
            "int",
        ],
    ):
        assert main() == 2
