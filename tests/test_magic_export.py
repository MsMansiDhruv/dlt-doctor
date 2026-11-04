"""Tests for lazy imports and IPython extension export (WP4)."""

import sys
from typing import Any


def test_import_dlt_doctor_does_not_eagerly_load_connect() -> None:
    # Ensure dlt_doctor and dlt_doctor.connect are not already imported
    sys.modules.pop("dlt_doctor", None)
    sys.modules.pop("dlt_doctor.connect", None)

    import dlt_doctor as doc

    assert doc is not None
    assert "dlt_doctor" in sys.modules
    # connect should NOT be imported eagerly
    assert "dlt_doctor.connect" not in sys.modules


def test_load_ipython_extension_registered_on_fake_ipython() -> None:
    import dlt_doctor

    assert hasattr(dlt_doctor, "load_ipython_extension")

    class FakeIPython:
        def __init__(self) -> None:
            self.registered: list[dict[str, Any]] = []

        def register_magic_function(self, func: Any, magic_kind: str, magic_name: str) -> None:
            self.registered.append(
                {
                    "func": func,
                    "magic_kind": magic_kind,
                    "magic_name": magic_name,
                }
            )

    fake_shell = FakeIPython()
    dlt_doctor.load_ipython_extension(fake_shell)

    assert len(fake_shell.registered) == 1
    assert fake_shell.registered[0]["magic_name"] == "dlt_doctor"
    assert fake_shell.registered[0]["magic_kind"] == "cell"


def test_lazy_access_workspace_inspector() -> None:
    sys.modules.pop("dlt_doctor.connect", None)
    import dlt_doctor

    # Accessing WorkspaceInspector triggers lazy import
    cls = dlt_doctor.WorkspaceInspector
    assert cls is not None
    assert "dlt_doctor.connect" in sys.modules
