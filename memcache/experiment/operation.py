from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any, Optional, Union

from .result import Key, Meta


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
    mode: str = "set"
    compare_cas: Optional[int] = None
    version: Optional[int] = None
    return_cas: bool = False
    vivify_ttl: Optional[int] = None


@dataclass(frozen=True)
class Delete:
    key: Key
    compare_cas: Optional[int] = None
    invalidate: bool = False
    stale_for: Optional[int] = None


@dataclass(frozen=True)
class Arithmetic:
    key: Key
    delta: int = 1
    decrement: bool = False
    initial: Optional[int] = None
    initial_ttl: Optional[int] = None
    ttl: Optional[int] = None
    compare_cas: Optional[int] = None
    version: Optional[int] = None
    return_cas: bool = False
    return_ttl: bool = False


Operation = Union[Get, Set, Delete, Arithmetic]
