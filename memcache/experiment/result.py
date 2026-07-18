from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, IntFlag, auto
from typing import Any, Generic, TypeVar
from collections.abc import Callable, Iterator, Sequence

from ..errors import MemcacheError


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


class Meta(IntFlag):
    NONE = 0
    CAS = 1 << 0
    TTL = 1 << 1
    SIZE = 1 << 2
    LAST_ACCESS = 1 << 3
    HIT_BEFORE = 1 << 4


@dataclass(frozen=True)
class ItemMeta:
    cas: int | None = None
    ttl: int | None = None
    size: int | None = None
    last_access: int | None = None
    hit_before: bool | None = None


class ResultValueError(MemcacheError):
    """Raised when a result does not carry a usable value."""


class GetResult(Generic[T]):
    def __init__(
        self,
        *,
        key: Key,
        status: GetStatus,
        value: Any = _NO_VALUE,
        item: ItemMeta | None = None,
        value_state: ValueState = ValueState.MISSING,
        lease_state: LeaseState = LeaseState.NONE,
        error: BaseException | None = None,
    ) -> None:
        self.key = key
        self.status = status
        self.item = item or ItemMeta()
        self.value_state = value_state
        self.lease_state = lease_state
        self.error = error
        self._value = value

    @property
    def value(self) -> T:
        if self.status is not GetStatus.HIT or self._value is _NO_VALUE:
            raise ResultValueError(
                "value is only available on a HIT result which requested the value"
            )
        return self._value  # type: ignore[no-any-return]

    def value_or(self, default: T) -> T:
        if self.status is GetStatus.HIT and self._value is not _NO_VALUE:
            return self._value  # type: ignore[no-any-return]
        return default

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

    def __bool__(self) -> bool:
        return self.status is MutationStatus.STORED


@dataclass(frozen=True)
class ArithmeticResult:
    key: Key
    status: MutationStatus
    value: int | None = None
    item: ItemMeta = ItemMeta()
    error: BaseException | None = None

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
