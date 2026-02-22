from dataclasses import dataclass
from typing import Generic, Optional, TypeVar


T = TypeVar("T")


@dataclass
class GetResult(Generic[T]):
    value: Optional[T] = None
    key: Optional[str] = None
    cas_token: Optional[int] = None
    ttl: Optional[int] = None
    last_access: Optional[int] = None
    size: Optional[int] = None
    hit_before: Optional[bool] = None
    is_stale: bool = False
    won_recache: bool = False
    already_won: bool = False
