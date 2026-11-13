"""Patch only the provider SDKs the app actually imports.

The first trace used to import openai, anthropic and google-genai to patch them, which cost
0.9-2 s on the first ``record()`` even for an app that used one or none of them (pass 7). Now:

- a provider the app has already imported is patched at once;
- a provider imported later is patched right after its import finishes, through a finder on
  ``sys.meta_path`` that wraps only that provider's loader for that one import.

Explicit ``install_*_interceptor()`` calls still import and patch unconditionally.
"""
from __future__ import annotations

import importlib.abc
import logging
import sys
import threading
from types import ModuleType
from typing import Any, Callable, Optional, Sequence

_lock = threading.Lock()
_pending: dict[str, Callable[[], None]] = {}
_finder: Optional["_PostImportFinder"] = None
_resolving = threading.local()
logger = logging.getLogger("retrace")


def _imported(name: str) -> bool:
    module = sys.modules.get(name)
    if module is None:
        return False
    # A module still executing its own import is not ready to patch; the finder handles it.
    return not getattr(getattr(module, "__spec__", None), "_initializing", False)


def _run(name: str) -> None:
    with _lock:
        install = _pending.pop(name, None)
    if install is None:
        return
    try:
        install()
    except Exception as e:  # a failed patch must never break the app's own import
        logger.debug("retrace: could not patch %s: %s", name, e)


class _NotifyingLoader(importlib.abc.Loader):
    """Runs the provider's own loader, then patches it. Restores the original loader afterwards,
    so the imported module carries no trace of the wrapper."""

    def __init__(self, loader: Any, name: str) -> None:
        self._loader = loader
        self._name = name

    def create_module(self, spec: Any) -> Optional[ModuleType]:
        module: Optional[ModuleType] = self._loader.create_module(spec)
        return module

    def exec_module(self, module: ModuleType) -> None:
        try:
            self._loader.exec_module(module)
        finally:
            spec = getattr(module, "__spec__", None)
            if spec is not None and spec.loader is self:
                spec.loader = self._loader
            if getattr(module, "__loader__", None) is self:
                module.__loader__ = self._loader
        _run(self._name)

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._loader, attr)


class _PostImportFinder(importlib.abc.MetaPathFinder):
    def find_spec(
        self, fullname: str, path: Optional[Sequence[str]] = None, target: Optional[ModuleType] = None
    ) -> Any:
        if fullname not in _pending or getattr(_resolving, "active", False):
            return None
        _resolving.active = True
        try:
            for finder in sys.meta_path:
                if finder is self or not hasattr(finder, "find_spec"):
                    continue
                spec = finder.find_spec(fullname, path, target)
                if spec is not None:
                    if spec.loader is not None:
                        spec.loader = _NotifyingLoader(spec.loader, fullname)
                    return spec
            return None
        finally:
            _resolving.active = False


def when_imported(name: str, install: Callable[[], None]) -> None:
    """Calls ``install`` now if module ``name`` is imported, else right after the app imports it.
    Idempotent per name: a later registration replaces a pending one."""
    global _finder
    if _imported(name):
        with _lock:
            _pending[name] = install
        _run(name)
        return
    with _lock:
        _pending[name] = install
        if _finder is None:
            _finder = _PostImportFinder()
            sys.meta_path.insert(0, _finder)
    # The module may have finished importing between the check and the registration.
    if _imported(name):
        _run(name)
