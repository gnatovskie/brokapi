"""The log of the library: `brokapi.enable_logging` turns on the Rust core's lines, to stderr."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

import brokapi


def run(code: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    """A separate interpreter: the log of a process is turned on once, and the tests share one process."""
    full = {k: v for k, v in os.environ.items() if k != "BROKAPI_LOG"}
    full.update(env or {})
    return subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=full, timeout=60, check=True
    )


def test_a_level_that_cannot_be_read_is_an_error() -> None:
    with pytest.raises(ValueError, match="bad log directive"):
        brokapi.enable_logging("loud")


def test_the_log_is_turned_on_once() -> None:
    out = run("import brokapi; print(brokapi.enable_logging(), brokapi.enable_logging('debug'))")
    assert out.stdout.split() == ["True", "False"]


def test_a_wrong_variable_is_reported_and_the_level_is_info() -> None:
    out = run("import brokapi; brokapi.enable_logging()", env={"BROKAPI_LOG": "debgu"})
    assert "log variable ignored" in out.stderr
    assert "debgu" in out.stderr
    # the source of the line is shown
    assert "brokapi::logging" in out.stderr
