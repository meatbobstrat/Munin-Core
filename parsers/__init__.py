# src/munin/parsers/__init__.py
from .base import REGISTRY, register, Parser, NormalizedEvent
# import parsers to register them
from . import jsonl, syslog, csvlog, plaintext  # noqa
