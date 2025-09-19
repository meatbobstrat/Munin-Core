"""
Munin-Core handler package.

Handlers register themselves via the @register decorator in registry.py.
See registry.py for details on how to add a new handler.
"""

from . import registry

__all__ = ["registry"]
