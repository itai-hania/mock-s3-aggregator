"""CLI package for interacting with the mock S3 aggregator service."""

from importlib import import_module
from types import ModuleType


def __getattr__(name: str) -> ModuleType:
    if name == "app":
        return import_module("cli.app")
    raise AttributeError(name)

# The Typer application lives in ``cli.app``.  We intentionally avoid re-exporting
# it from the package root so that ``cli.app`` continues to resolve to the module
# itself.  Several tests (and downstream tooling) rely on patching attributes on
# that module path, so shadowing it with the Typer instance would break those
# expectations.

__all__ = []
