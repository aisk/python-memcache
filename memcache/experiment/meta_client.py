from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from typing import Any, Coroutine, List, Optional, Sequence, TypeVar

from ..meta_command import MetaCommand, MetaResult
from .async_meta_client import AsyncMetaClient
from .operation import ABSENT, PRESENT, Delete, Get, IfCas, Increment, Operation, Set
from .result import (
    ArithmeticResult,
    BatchResult,
    GetResult,
    Key,
    LeaseResult,
    Meta,
    MutationResult,
)


R = TypeVar("R")


class RawClient:
    def __init__(self, client: "MetaClient") -> None:
        self._client = client

    def execute(self, **command: Any) -> MetaResult:
        return self._client._run(self._client._async.raw.execute(**command))

    def batch(
        self, commands: Sequence[MetaCommand], *, timeout: Optional[float] = None
    ) -> List[MetaResult]:
        return self._client._run(
            self._client._async.raw.batch(commands, timeout=timeout)
        )


class MetaClient:
    """Synchronous view over the sole async protocol implementation."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(
            target=self._serve_loop,
            name="memcache-meta-client",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()
        self._async = AsyncMetaClient(*args, **kwargs)
        self.raw = RawClient(self)
        self._closed = False

    def _serve_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()
        self._loop.close()

    def _run(self, awaitable: Coroutine[Any, Any, R]) -> R:
        if self._closed:
            if hasattr(awaitable, "close"):
                awaitable.close()
            raise RuntimeError("client is closed")
        future: concurrent.futures.Future[R] = asyncio.run_coroutine_threadsafe(
            awaitable, self._loop
        )
        return future.result()

    def __enter__(self) -> "MetaClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._run(self._async.close())
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    def execute_meta_command(
        self, command: MetaCommand, *, timeout: Optional[float] = None
    ) -> MetaResult:
        return self._run(self._async.execute_meta_command(command, timeout=timeout))

    def batch(
        self,
        operations: Sequence[Operation],
        *,
        timeout: Optional[float] = None,
    ) -> BatchResult:
        return self._run(self._async.batch(operations, timeout=timeout))

    def get(
        self,
        key: Key,
        *,
        meta: Meta = Meta.NONE,
        touch: Optional[int] = None,
        no_lru_bump: bool = False,
        unless_cas: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> GetResult[Any]:
        return self._run(
            self._async.get(
                key,
                meta=meta,
                touch=touch,
                no_lru_bump=no_lru_bump,
                unless_cas=unless_cas,
                timeout=timeout,
            )
        )

    def inspect(
        self,
        key: Key,
        *,
        meta: Meta = Meta.CAS | Meta.TTL | Meta.SIZE,
        no_lru_bump: bool = True,
        timeout: Optional[float] = None,
    ) -> GetResult[Any]:
        return self._run(
            self._async.inspect(
                key, meta=meta, no_lru_bump=no_lru_bump, timeout=timeout
            )
        )

    def get_with_lease(
        self,
        key: Key,
        *,
        lease_ttl: int,
        refresh_before: Optional[int] = None,
        meta: Meta = Meta.NONE,
        timeout: Optional[float] = None,
    ) -> LeaseResult[Any]:
        async_result = self._run(
            self._async.get_with_lease(
                key,
                lease_ttl=lease_ttl,
                refresh_before=refresh_before,
                meta=meta,
                timeout=timeout,
            )
        )
        # Replace the async cursor callback with a blocking callback while
        # retaining all response data.
        async_fulfill = async_result._fulfill
        async_result._fulfill = lambda *a, **kw: self._run(async_fulfill(*a, **kw))
        return async_result

    def get_many(
        self,
        keys: Sequence[Key],
        *,
        meta: Meta = Meta.NONE,
        timeout: Optional[float] = None,
    ) -> BatchResult:
        return self._run(self._async.get_many(keys, meta=meta, timeout=timeout))

    def set(
        self,
        key: Key,
        value: Any,
        *,
        ttl: Optional[int] = None,
        condition: Any = None,
        version: Optional[int] = None,
        return_cas: bool = False,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        return self._run(
            self._async.set(
                key,
                value,
                ttl=ttl,
                condition=condition,
                version=version,
                return_cas=return_cas,
                timeout=timeout,
            )
        )

    def add(self, key: Key, value: Any, **options: Any) -> MutationResult:
        options["condition"] = ABSENT
        return self.set(key, value, **options)

    def replace(self, key: Key, value: Any, **options: Any) -> MutationResult:
        options["condition"] = PRESENT
        return self.set(key, value, **options)

    def cas(
        self, key: Key, value: Any, cas_token: int, **options: Any
    ) -> MutationResult:
        options["condition"] = IfCas(cas_token)
        return self.set(key, value, **options)

    def append_bytes(self, key: Key, value: bytes, **options: Any) -> MutationResult:
        return self._run(self._async.append_bytes(key, value, **options))

    def prepend_bytes(self, key: Key, value: bytes, **options: Any) -> MutationResult:
        return self._run(self._async.prepend_bytes(key, value, **options))

    def delete(
        self,
        key: Key,
        *,
        condition: Optional[IfCas] = None,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        return self._run(self._async.delete(key, condition=condition, timeout=timeout))

    def invalidate(
        self,
        key: Key,
        *,
        stale_for: Optional[int] = None,
        condition: Optional[IfCas] = None,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        return self._run(
            self._async.invalidate(
                key,
                stale_for=stale_for,
                condition=condition,
                timeout=timeout,
            )
        )

    def increment(self, key: Key, delta: int = 1, **options: Any) -> ArithmeticResult:
        return self._run(self._async.increment(key, delta, **options))

    def decrement(self, key: Key, delta: int = 1, **options: Any) -> ArithmeticResult:
        return self._run(self._async.decrement(key, delta, **options))

    def touch(
        self, key: Key, ttl: int, *, timeout: Optional[float] = None
    ) -> MutationResult:
        return self._run(self._async.touch(key, ttl, timeout=timeout))

    def flush_all(self, delay: int = 0) -> None:
        self._run(self._async.flush_all(delay))


__all__ = [
    "ABSENT",
    "PRESENT",
    "Delete",
    "Get",
    "IfCas",
    "Increment",
    "MetaClient",
    "Set",
]
