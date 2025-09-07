# src/munin/parsers/__init__.py
# import parsers to register them
from . import csvlog, jsonl, plaintext, syslog  # noqa
from .base import REGISTRY, NormalizedEvent, Parser, register
