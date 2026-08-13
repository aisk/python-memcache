from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, IntFlag, auto
from typing import Any, Generic, TypeVar
from collections.abc import Callable, Iterator, Sequence

from ..errors import (
    AlreadyExistsError,
    AmbiguousWriteError,
    CasMismatchError,
    MemcacheError,
    NotFoundError,
    OperationFailedError,
)

T = TypeVar("T")
Key = str | bytes
_NO_VALUE = object()


class GetStatus(Enum):
    HIT = auto()
    MISS = auto()
    PENDING = auto()
    UNCHANGED = auto()
    FAILED = auto()
    AMBIGUOUS = auto()


class MutationStatus(Enum):
    STORED = auto()
    NOT_FOUND = auto()
    ALREADY_EXISTS = auto()
    CAS_MISMATCH = auto()
    FAILED = auto()
    AMBIGUOUS = auto()


class ValueState(Enum):
    FRESH = auto()
    STALE = auto()
    MISSING = auto()


class LeaseState(Enum):
    NONE = auto()
    GRANTED = auto()
    BUSY = auto()


class Field(IntFlag):
    """Item metadata a read asks the server to return alongside the value.

    Each flag maps to one mg return flag and lands on :class:`ItemFields`, so
    ``Field.CAS`` fills ``result.item.cas``.
    """

    NONE = 0
    CAS = 1 << 0
    TTL = 1 << 1
    SIZE = 1 << 2
    LAST_ACCESS = 1 << 3
    HIT_BEFORE = 1 << 4


@dataclass(frozen=True)
class ItemFields:
    cas: int | None = None
    ttl: int | None = None
    size: int | None = None
    last_access: int | None = None
    hit_before: bool | None = None


class ResultValueError(MemcacheError):
    """Raised when a result does not carry a usable value."""


def failure_error(result: Any) -> MemcacheError | None:
    """The exception for a result that never reached a usable answer.

    Returns ``None`` when the server did answer, whatever that answer was.
    Callers raise it ``from result.error`` so the original cause survives.
    """
    if result.status in (GetStatus.AMBIGUOUS, MutationStatus.AMBIGUOUS):
        return AmbiguousWriteError(result)
    if result.status in (GetStatus.FAILED, MutationStatus.FAILED):
        return OperationFailedError(result.key)
    return None


def check_result(result: Any) -> Any:
    """Raise unless the operation reached the outcome the caller asked for."""
    error = failure_error(result)
    if error is not None:
        raise error from result.error
    status = result.status
    if status is MutationStatus.CAS_MISMATCH:
        raise CasMismatchError("cas token for key %r no longer matches" % (result.key,))
    if status is MutationStatus.ALREADY_EXISTS:
        raise AlreadyExistsError("key %r already exists" % (result.key,))
    if status in (MutationStatus.NOT_FOUND, GetStatus.MISS, GetStatus.PENDING):
        raise NotFoundError("key %r was not found" % (result.key,))
    return result


_NO_VALUE_REASONS = {
    GetStatus.MISS: "it was not found",
    GetStatus.UNCHANGED: "it was unchanged, so the server skipped the payload",
    GetStatus.PENDING: "another caller holds the lease and has not filled it yet",
}


class GetResult(Generic[T]):
    def __init__(
        self,
        *,
        key: Key,
        status: GetStatus,
        value: Any = _NO_VALUE,
        item: ItemFields | None = None,
        value_state: ValueState = ValueState.MISSING,
        lease_state: LeaseState = LeaseState.NONE,
        error: BaseException | None = None,
    ) -> None:
        self.key = key
        self.status = status
        self.item = item or ItemFields()
        self.value_state = value_state
        self.lease_state = lease_state
        self.error = error
        self._value = value

    @property
    def value(self) -> T:
        if self.status is not GetStatus.HIT:
            # Chained so a FAILED result surfaces the ConnectionRefusedError or
            # TimeoutError behind it. Without this the traceback reads like a
            # usage mistake and the real cause is lost exactly when it matters.
            raise ResultValueError(
                "key %r has no value to read: %s"
                % (self.key, _NO_VALUE_REASONS.get(self.status, "the read failed"))
            ) from self.error
        if self._value is _NO_VALUE:
            raise ResultValueError(
                "key %r was a hit but the read did not ask for the value; "
                "pass value=True, or use get() rather than inspect()" % (self.key,)
            )
        return self._value  # type: ignore[no-any-return]

    def value_or(self, default: T) -> T:
        if self.status is GetStatus.HIT and self._value is not _NO_VALUE:
            return self._value  # type: ignore[no-any-return]
        return default

    def check(self) -> GetResult[T]:
        """Return self, or raise when the read did not produce a value.

        ``UNCHANGED`` passes: an ``unless_cas`` read that skipped the payload
        is a successful answer, not a missing one.
        """
        return check_result(self)  # type: ignore[no-any-return]

    def __bool__(self) -> bool:
        return self.status is GetStatus.HIT

    # Transitional metadata aliases. They cost nothing and make migration from
    # the first experimental prototype less abrupt.
    @property
    def cas_token(self) -> int | None:
        return self.item.cas

    @property
    def ttl(self) -> int | None:
        return self.item.ttl

    @property
    def size(self) -> int | None:
        return self.item.size

    @property
    def last_access(self) -> int | None:
        return self.item.last_access

    @property
    def hit_before(self) -> bool | None:
        return self.item.hit_before

    @property
    def is_stale(self) -> bool:
        return self.value_state is ValueState.STALE

    @property
    def won_recache(self) -> bool:
        return self.lease_state is LeaseState.GRANTED

    @property
    def already_won(self) -> bool:
        return self.lease_state is LeaseState.BUSY


class LeaseResult(GetResult[T]):
    def __init__(self, *, fulfill: Callable[..., Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._fulfill = fulfill

    def fulfill(
        self,
        value: T,
        *,
        ttl: int | None = None,
        version: int | None = None,
        return_cas: bool = False,
        timeout: float | None = None,
    ) -> Any:
        if self.lease_state is not LeaseState.GRANTED:
            raise MemcacheError("only the lease winner can fulfill a refresh")
        return self._fulfill(
            value,
            ttl=ttl,
            version=version,
            return_cas=return_cas,
            timeout=timeout,
        )


@dataclass(frozen=True)
class MutationResult:
    key: Key
    status: MutationStatus
    cas: int | None = None
    error: BaseException | None = None

    def check(self) -> MutationResult:
        """Return self, or raise when the write did not happen."""
        return check_result(self)  # type: ignore[no-any-return]

    def __bool__(self) -> bool:
        return self.status is MutationStatus.STORED


@dataclass(frozen=True)
class ArithmeticResult:
    key: Key
    status: MutationStatus
    value: int | None = None
    item: ItemFields = ItemFields()
    error: BaseException | None = None

    def check(self) -> ArithmeticResult:
        """Return self, or raise when the counter was not updated."""
        return check_result(self)  # type: ignore[no-any-return]

    def __bool__(self) -> bool:
        return self.status is MutationStatus.STORED


Result = GetResult[Any] | MutationResult | ArithmeticResult


class BatchResult(Sequence[Result]):
    def __init__(self, results: Sequence[Result]) -> None:
        self.results = tuple(results)

    def __getitem__(self, index: Any) -> Any:
        return self.results[index]

    def __len__(self) -> int:
        return len(self.results)

    def __iter__(self) -> Iterator[Result]:
        return iter(self.results)

    @property
    def failures(self) -> tuple[Result, ...]:
        """Results whose operation never reached a usable answer.

        Semantic outcomes such as a miss or a CAS mismatch are not failures;
        call :meth:`Result.check` on an individual result to assert those.
        """
        return tuple(item for item in self.results if failure_error(item) is not None)

    def raise_for_failures(self) -> BatchResult:
        """Return self, or raise for the first operation that failed."""
        for item in self.results:
            error = failure_error(item)
            if error is not None:
                raise error from item.error
        return self
