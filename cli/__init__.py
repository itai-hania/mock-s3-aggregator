"""CLI package for interacting with the mock S3 aggregator service."""

from importlib import import_module
from types import ModuleType


def __getattr__(name: str) -> ModuleType:
    if name == "app":
        return import_module("cli.app")
    raise AttributeError(name)


__all__ = ["app"]
