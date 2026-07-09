#!/usr/bin/env python3
"""Import shim: full implementation lives at repo-root ``register_mimo.py``.

Do NOT execute the root script as ``__main__`` on import — that would re-parse
argv and break callers like ``ck_lifecycle.try_replace``.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent / "register_mimo.py"
_spec = importlib.util.spec_from_file_location("mimo_register_mimo_root", _ROOT)
if _spec is None or _spec.loader is None:
    raise ImportError(f"cannot load {_ROOT}")
_mod = importlib.util.module_from_spec(_spec)
# Avoid polluting sys.modules['register_mimo'] with this package shim name clash
sys.modules.setdefault("mimo_register_mimo_root", _mod)
_spec.loader.exec_module(_mod)

# Re-export public API
for _name in dir(_mod):
    if _name.startswith("_") and _name not in ("__all__",):
        continue
    globals()[_name] = getattr(_mod, _name)

if __name__ == "__main__":
    # Delegate CLI to root script properly
    import runpy

    runpy.run_path(str(_ROOT), run_name="__main__")
