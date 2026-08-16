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


class NotFoundError(MemcacheError):
    """The operation required an existing item and the key was not there."""


class AlreadyExistsError(MemcacheError):
    """An add-style write found the key already present."""


class ConflictError(MemcacheError):
    """An optimistic concurrency loop exhausted its retries.

    Raised by ``update`` and ``pop`` when every attempt kept colliding with
    concurrent writes to the same key. This signals abnormal contention, not
    a normal answer; the item itself is fine.
    """


class OperationFailedError(MemcacheError):
    """An operation did not reach a usable answer from the server.

    Raised for infrastructure trouble such as a refused connection, a timeout,
    a malformed response, or a value that could not be deserialized. The
    underlying cause is attached as ``__cause__``.
    """

    def __init__(self, key: Any) -> None:
        super().__init__("operation on key %r failed" % (key,))
        self.key = key


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
        confirmed: int = 0,
    ) -> None:
        super().__init__(str(cause))
        self.written = written
        self.responses = responses
        self.cause = cause
        self.confirmed = confirmed
        """How many leading commands already cleared a barrier of their own.

        A large pipeline is written and read in chunks, so an early chunk can
        be fully answered before a later one fails. Those commands are settled
        and must not be reported as ambiguous.
        """
