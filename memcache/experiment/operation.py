from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any, Optional, Union

from .result import Key, Meta


class _Absent:
    def __repr__(self) -> str:
        return "ABSENT"


class _Present:
    def __repr__(self) -> str:
        return "PRESENT"


ABSENT = _Absent()
PRESENT = _Present()


@dataclass(frozen=True)
class IfCas:
    token: int

    def __post_init__(self) -> None:
        if self.token < 0:
            raise ValueError("CAS token must be non-negative")


Condition = Union[_Absent, _Present, IfCas]


@dataclass(frozen=True)
class Get:
    key: Key
    meta: Meta = Meta.NONE
    touch: Optional[int] = None
    no_lru_bump: bool = False
    unless_cas: Optional[int] = None
    value: bool = True
    lease_ttl: Optional[int] = None
    refresh_before: Optional[int] = None


@dataclass(frozen=True)
class Set:
    key: Key
    if sys.version_info < (3, 9):
        # Python 3.8's mypy rejects Any in dataclass-generated methods.
        value: object
    else:
        value: Any
    ttl: Optional[int] = None
    condition: Optional[Condition] = None
    version: Optional[int] = None
    return_cas: bool = False
    mode: str = "set"
    vivify_ttl: Optional[int] = None


@dataclass(frozen=True)
class Delete:
    key: Key
    condition: Optional[IfCas] = None
    invalidate: bool = False
    stale_for: Optional[int] = None


@dataclass(frozen=True)
class Increment:
    key: Key
    delta: int = 1
    initial: Optional[int] = None
    initial_ttl: Optional[int] = None
    ttl: Optional[int] = None
    decrement: bool = False
    condition: Optional[IfCas] = None
    version: Optional[int] = None
    return_cas: bool = False


Operation = Union[Get, Set, Delete, Increment]
