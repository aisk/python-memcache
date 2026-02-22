from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class GetResult:
    value: Optional[Any] = None
    key: Optional[str] = None
    cas_token: Optional[int] = None
    ttl: Optional[int] = None
    last_access: Optional[int] = None
    size: Optional[int] = None
    hit_before: Optional[bool] = None
    is_stale: bool = False
    won_recache: bool = False
    already_won: bool = False
