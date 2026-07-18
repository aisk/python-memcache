from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .result import Key, Meta


@dataclass(frozen=True)
class Get:
    key: Key
    meta: Meta = Meta.NONE
    touch: int | None = None
    no_lru_bump: bool = False
    unless_cas: int | None = None
    value: bool = True
    lease_ttl: int | None = None
    refresh_before: int | None = None


@dataclass(frozen=True)
class Set:
    key: Key
    value: Any
    ttl: int | None = None
    mode: str = "set"
    compare_cas: int | None = None
    version: int | None = None
    return_cas: bool = False
    vivify_ttl: int | None = None


@dataclass(frozen=True)
class Delete:
    key: Key
    compare_cas: int | None = None
    invalidate: bool = False
    stale_for: int | None = None


@dataclass(frozen=True)
class Arithmetic:
    key: Key
    delta: int = 1
    decrement: bool = False
    initial: int | None = None
    initial_ttl: int | None = None
    ttl: int | None = None
    compare_cas: int | None = None
    version: int | None = None
    return_cas: bool = False
    return_ttl: bool = False


Operation = Get | Set | Delete | Arithmetic
