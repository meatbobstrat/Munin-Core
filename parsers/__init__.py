# Import parser modules for their side effects (they register themselves).
# Mark as intentionally unused to satisfy Ruff.
from . import csvlog as _csvlog  # noqa: F401
from . import jsonl as _jsonl  # noqa: F401
from . import plaintext as _plaintext  # noqa: F401
from . import syslog as _syslog  # noqa: F401

# Explicit re-exports for library users.
from .base import (
    REGISTRY as REGISTRY,
)
from .base import (
    NormalizedEvent as NormalizedEvent,
)
from .base import (
    Parser as Parser,
)
from .base import (
    register as register,
)

__all__ = ["REGISTRY", "NormalizedEvent", "Parser", "register"]
