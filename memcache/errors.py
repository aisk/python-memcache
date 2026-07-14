from typing import Any, Optional


class MemcacheError(Exception):
    pass


class SerializeError(MemcacheError):
    pass


class AmbiguousWriteError(MemcacheError):
    """A request was sent but its terminal response was not received."""

    def __init__(self, result: Optional[Any] = None) -> None:
        super().__init__("operation outcome is ambiguous")
        self.result = result


class ProtocolError(MemcacheError):
    """The server returned a malformed or unsupported protocol response."""
