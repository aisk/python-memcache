from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .meta_command import MetaResult


class MemcacheError(Exception):
    pass


class SerializeError(MemcacheError):
    pass


class CasMismatchError(MemcacheError):
    """The item changed since it was read; re-read and retry the operation."""


class AmbiguousWriteError(MemcacheError):
    """A request was sent but its terminal response was not received."""

    def __init__(self, result: Any | None = None) -> None:
        super().__init__("operation outcome is ambiguous")
        self.result = result


class ProtocolError(MemcacheError):
    """The server returned a malformed or unsupported protocol response."""


class PipelineError(MemcacheError):
    """A pipeline failed after a possibly partial write or response sequence."""

    def __init__(
        self,
        written: int,
        responses: list[MetaResult],
        cause: BaseException,
    ) -> None:
        super().__init__(str(cause))
        self.written = written
        self.responses = responses
        self.cause = cause
