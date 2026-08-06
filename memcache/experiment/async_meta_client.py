from __future__ import annotations

from collections import deque
from contextlib import asynccontextmanager
from typing import (
    Any,
    cast,
)
from collections.abc import AsyncIterator, Sequence

import anyio
import hashring

from ..async_connection import AsyncConnection
from ..connection import Addr
from ..errors import PipelineError, ProtocolError
from ..meta_command import MetaCommand, MetaResult
from ..serialize import Serializer, StrictSerializer
from ._meta_core import (
    MetaProtocol,
    Prepared,
    fill_meta_responses,
    finalize_batch,
    finalize_meta_batch,
    group_meta_commands,
    group_prepared,
    normalize_addresses,
    raise_if_ambiguous,
    routing_key,
    touch_result,
)
from .meta_api import (
    MetaCommandResult,
    Token,
    build_arithmetic,
    build_debug,
    build_delete,
    build_get,
    build_set,
    key_bytes,
    parse_debug_result,
    parse_meta_result,
    positive,
)
from .operation import Arithmetic, Delete, Get, Operation, Set
from .result import (
    ArithmeticResult,
    BatchResult,
    GetResult,
    Key,
    LeaseResult,
    Meta,
    MutationResult,
    Result,
)


class _Server:
    """A memcached backend with an elastic FIFO pool of idle connections.

    Async counterpart of :class:`memcache.experiment.meta_client._Server`;
    see that class for the pooling contract. Pool bookkeeping happens
    between awaits within a single event loop, so no lock is needed.
    """

    def __init__(
        self,
        addr: Addr,
        username: str | None,
        password: str | None,
        max_idle: int | None,
    ) -> None:
        self.addr = addr
        self._username = username
        self._password = password
        self._max_idle = max_idle
        self._idle: deque[AsyncConnection] = deque()
        self._closed = False

    def __repr__(self) -> str:
        return "%s:%d" % self.addr

    @asynccontextmanager
    async def _borrow(self) -> AsyncIterator[AsyncConnection]:
        if self._closed:
            raise RuntimeError("client is closed")
        if self._idle:
            connection = self._idle.popleft()
        else:
            connection = AsyncConnection(
                self.addr, username=self._username, password=self._password
            )
        try:
            yield connection
        except BaseException:
            try:
                await connection.close()
            except BaseException:
                pass
            raise
        else:
            await self._release(connection)

    async def _release(self, connection: AsyncConnection) -> None:
        if not self._closed and (
            self._max_idle is None or len(self._idle) < self._max_idle
        ):
            self._idle.append(connection)
            return
        try:
            await connection.close()
        except BaseException:
            pass

    async def pipeline(
        self, commands: list[MetaCommand], timeout: float | None
    ) -> list[MetaResult]:
        async with self._borrow() as connection:
            if timeout is None:
                return await connection.execute_pipeline(commands)
            with anyio.fail_after(timeout):
                return await connection.execute_pipeline(commands)

    async def execute(self, command: MetaCommand, timeout: float | None) -> MetaResult:
        async with self._borrow() as connection:
            if timeout is None:
                return await connection.execute_meta_command(command)
            with anyio.fail_after(timeout):
                return await connection.execute_meta_command(command)

    async def flush(self, delay: int) -> None:
        async with self._borrow() as connection:
            await connection.flush_all(delay)

    async def close(self) -> None:
        self._closed = True
        while self._idle:
            connection = self._idle.popleft()
            try:
                await connection.close()
            except BaseException:
                pass


class AsyncMetaNamespace:
    """Typed, flag-for-flag surface over the meta protocol commands.

    Async counterpart of :class:`memcache.experiment.meta_client.MetaNamespace`;
    see that class for the layer's contract.
    """

    def __init__(self, client: AsyncMetaClient) -> None:
        self._client = client

    async def _run(
        self, command: MetaCommand, timeout: float | None
    ) -> MetaCommandResult:
        return parse_meta_result(
            await self._client.execute_meta_command(command, timeout=timeout)
        )

    async def get(
        self,
        key: Key,
        *,
        value: bool = True,
        return_cas: bool = False,
        return_ttl: bool = False,
        return_size: bool = False,
        return_last_access: bool = False,
        return_hit_before: bool = False,
        return_client_flags: bool = False,
        return_key: bool = False,
        touch: int | None = None,
        vivify_ttl: int | None = None,
        recache_ttl: int | None = None,
        unless_cas: int | None = None,
        new_cas: int | None = None,
        no_lru_bump: bool = False,
        opaque: Token | None = None,
        timeout: float | None = None,
    ) -> MetaCommandResult:
        command = build_get(
            key,
            value=value,
            return_cas=return_cas,
            return_ttl=return_ttl,
            return_size=return_size,
            return_last_access=return_last_access,
            return_hit_before=return_hit_before,
            return_client_flags=return_client_flags,
            return_key=return_key,
            touch=touch,
            vivify_ttl=vivify_ttl,
            recache_ttl=recache_ttl,
            unless_cas=unless_cas,
            new_cas=new_cas,
            no_lru_bump=no_lru_bump,
            opaque=opaque,
        )
        return await self._run(command, timeout)

    async def set(
        self,
        key: Key,
        value: bytes,
        *,
        client_flags: int | None = None,
        ttl: int | None = None,
        mode: str = "set",
        compare_cas: int | None = None,
        new_cas: int | None = None,
        invalidate: bool = False,
        vivify_ttl: int | None = None,
        return_cas: bool = False,
        return_size: bool = False,
        return_key: bool = False,
        opaque: Token | None = None,
        timeout: float | None = None,
    ) -> MetaCommandResult:
        command = build_set(
            key,
            value,
            client_flags=client_flags,
            ttl=ttl,
            mode=mode,
            compare_cas=compare_cas,
            new_cas=new_cas,
            invalidate=invalidate,
            vivify_ttl=vivify_ttl,
            return_cas=return_cas,
            return_size=return_size,
            return_key=return_key,
            opaque=opaque,
        )
        return await self._run(command, timeout)

    async def delete(
        self,
        key: Key,
        *,
        compare_cas: int | None = None,
        new_cas: int | None = None,
        invalidate: bool = False,
        ttl: int | None = None,
        drop_value: bool = False,
        return_key: bool = False,
        opaque: Token | None = None,
        timeout: float | None = None,
    ) -> MetaCommandResult:
        command = build_delete(
            key,
            compare_cas=compare_cas,
            new_cas=new_cas,
            invalidate=invalidate,
            ttl=ttl,
            drop_value=drop_value,
            return_key=return_key,
            opaque=opaque,
        )
        return await self._run(command, timeout)

    async def arithmetic(
        self,
        key: Key,
        *,
        delta: int | None = None,
        decrement: bool = False,
        initial: int | None = None,
        initial_ttl: int | None = None,
        ttl: int | None = None,
        compare_cas: int | None = None,
        new_cas: int | None = None,
        return_value: bool = True,
        return_ttl: bool = False,
        return_cas: bool = False,
        return_key: bool = False,
        opaque: Token | None = None,
        timeout: float | None = None,
    ) -> MetaCommandResult:
        command = build_arithmetic(
            key,
            delta=delta,
            decrement=decrement,
            initial=initial,
            initial_ttl=initial_ttl,
            ttl=ttl,
            compare_cas=compare_cas,
            new_cas=new_cas,
            return_value=return_value,
            return_ttl=return_ttl,
            return_cas=return_cas,
            return_key=return_key,
            opaque=opaque,
        )
        return await self._run(command, timeout)

    async def debug(
        self, key: Key, *, timeout: float | None = None
    ) -> dict[str, str] | None:
        return parse_debug_result(
            await self._client.execute_meta_command(build_debug(key), timeout=timeout)
        )

    async def execute(
        self,
        *,
        command: str | bytes,
        key: Key,
        flags: Sequence[bytes] = (),
        value: bytes | None = None,
        timeout: float | None = None,
    ) -> MetaResult:
        cm = command.encode("ascii") if isinstance(command, str) else command
        if len(cm) != 2:
            raise ValueError("meta command must be exactly two bytes")
        if value is not None and cm != b"ms":
            raise ValueError("only a raw ms command accepts a value payload")
        meta = MetaCommand(
            cm=cm,
            key=key_bytes(key),
            datalen=len(value) if value is not None else None,
            flags=list(flags),
            value=value,
        )
        return await self._client.execute_meta_command(meta, timeout=timeout)

    async def batch(
        self,
        commands: Sequence[MetaCommand],
        *,
        timeout: float | None = None,
    ) -> list[MetaResult]:
        # Meta batch does not infer quiet outcomes; require one response per
        # command so output can retain input order across server shards.
        if self._client._closed:
            raise RuntimeError("client is closed")
        grouped = group_meta_commands(commands, self._client._server_for)
        output: list[MetaResult | None] = [None] * len(commands)
        async with anyio.create_task_group() as tasks:
            for server, group in grouped.items():
                tasks.start_soon(
                    self._run_group,
                    server,
                    group,
                    output,
                    self._client._timeout(timeout),
                )
        return finalize_meta_batch(output)

    @staticmethod
    async def _run_group(
        server: _Server,
        group: list[tuple[int, MetaCommand]],
        output: list[MetaResult | None],
        timeout: float | None,
    ) -> None:
        responses = await server.pipeline([command for _, command in group], timeout)
        fill_meta_responses(output, group, responses)


class AsyncMetaClient(MetaProtocol):
    """High-level meta protocol client with a batch-first executor."""

    def __init__(
        self,
        addr: Addr | list[Addr] | None = None,
        *,
        max_idle: int | None = 23,
        timeout: float | None = 1.0,
        serializer: Serializer | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        super().__init__(serializer if serializer is not None else StrictSerializer())
        self.default_timeout = timeout
        self._servers = [
            _Server(server, username=username, password=password, max_idle=max_idle)
            for server in normalize_addresses(addr)
        ]
        self._ring = hashring.HashRing(self._servers)
        self.meta = AsyncMetaNamespace(self)
        self._closed = False

    async def __aenter__(self) -> AsyncMetaClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def _timeout(self, timeout: float | None) -> float | None:
        return self.default_timeout if timeout is None else timeout

    def _server_for(self, key: Key) -> _Server:
        return cast(_Server, self._ring.get_node(routing_key(key)))

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for server in self._servers:
            await server.close()

    async def execute_meta_command(
        self, command: MetaCommand, *, timeout: float | None = None
    ) -> MetaResult:
        if self._closed:
            raise RuntimeError("client is closed")
        return await self._server_for(command.key).execute(
            command, self._timeout(timeout)
        )

    def _lease_fulfill(self, key: Key, cas: int | None) -> Any:
        async def fulfill(value: Any, **options: Any) -> MutationResult:
            if cas is None:
                raise ProtocolError("lease response did not include CAS")
            return await self.set(key, value, compare_cas=cas, **options)

        return fulfill

    async def _run_group(
        self,
        server: _Server,
        prepared: list[Prepared],
        output: list[Result | None],
        timeout: float | None,
    ) -> None:
        commands = [self._pipeline_command(item) for item in prepared]
        responses: list[MetaResult] = []
        written = 0
        failure: BaseException | None = None
        barrier = False
        try:
            responses = await server.pipeline(commands, timeout)
            written = len(prepared)
            barrier = True
        except PipelineError as exc:
            responses = exc.responses
            written = exc.written
            failure = exc.cause
        except BaseException as exc:
            failure = exc
        self._resolve_group(prepared, output, responses, written, barrier, failure)

    async def batch(
        self,
        operations: Sequence[Operation],
        *,
        timeout: float | None = None,
    ) -> BatchResult:
        if self._closed:
            raise RuntimeError("client is closed")
        prepared = [self._prepare(index, op) for index, op in enumerate(operations)]
        grouped = group_prepared(prepared, self._server_for)
        output: list[Result | None] = [None] * len(prepared)
        async with anyio.create_task_group() as tasks:
            for server, group in grouped.items():
                tasks.start_soon(
                    self._run_group,
                    server,
                    group,
                    output,
                    self._timeout(timeout),
                )
        return finalize_batch(output)

    async def _one(self, operation: Operation, timeout: float | None) -> Result:
        return raise_if_ambiguous(
            cast(Result, (await self.batch([operation], timeout=timeout))[0])
        )

    async def get(
        self,
        key: Key,
        *,
        value: bool = True,
        meta: Meta = Meta.NONE,
        touch: int | None = None,
        no_lru_bump: bool = False,
        unless_cas: int | None = None,
        lease_ttl: int | None = None,
        refresh_before: int | None = None,
        timeout: float | None = None,
    ) -> GetResult[Any]:
        """Read a key; covers every mg capability.

        With ``lease_ttl`` the result is a :class:`LeaseResult`; use
        :meth:`get_with_lease` for the statically typed variant.
        """
        return await self._one(  # type: ignore[return-value]
            Get(
                key,
                meta=meta,
                touch=touch,
                no_lru_bump=no_lru_bump,
                unless_cas=unless_cas,
                value=value,
                lease_ttl=lease_ttl,
                refresh_before=refresh_before,
            ),
            timeout,
        )

    async def inspect(
        self,
        key: Key,
        *,
        meta: Meta = Meta.CAS | Meta.TTL | Meta.SIZE,
        no_lru_bump: bool = True,
        timeout: float | None = None,
    ) -> GetResult[Any]:
        return await self._one(  # type: ignore[return-value]
            Get(key, meta=meta, no_lru_bump=no_lru_bump, value=False), timeout
        )

    async def get_with_lease(
        self,
        key: Key,
        *,
        lease_ttl: int,
        refresh_before: int | None = None,
        meta: Meta = Meta.NONE,
        timeout: float | None = None,
    ) -> LeaseResult[Any]:
        return await self._one(  # type: ignore[return-value]
            Get(
                key,
                meta=meta,
                lease_ttl=lease_ttl,
                refresh_before=refresh_before,
            ),
            timeout,
        )

    async def set(
        self,
        key: Key,
        value: Any,
        *,
        ttl: int | None = None,
        mode: str = "set",
        compare_cas: int | None = None,
        version: int | None = None,
        return_cas: bool = False,
        vivify_ttl: int | None = None,
        timeout: float | None = None,
    ) -> MutationResult:
        """Store a key; covers every ms capability.

        ``mode`` is one of ``set``/``add``/``replace``/``append``/``prepend``;
        the concatenation modes take bytes only and skip serialization.
        """
        return await self._one(  # type: ignore[return-value]
            Set(
                key,
                value,
                ttl=ttl,
                mode=mode,
                compare_cas=compare_cas,
                version=version,
                return_cas=return_cas,
                vivify_ttl=vivify_ttl,
            ),
            timeout,
        )

    async def add(
        self,
        key: Key,
        value: Any,
        *,
        ttl: int | None = None,
        version: int | None = None,
        return_cas: bool = False,
        timeout: float | None = None,
    ) -> MutationResult:
        """Store only if the key does not exist; ALREADY_EXISTS otherwise."""
        return await self.set(
            key,
            value,
            ttl=ttl,
            mode="add",
            version=version,
            return_cas=return_cas,
            timeout=timeout,
        )

    async def cas(
        self,
        key: Key,
        value: Any,
        cas_token: int,
        *,
        ttl: int | None = None,
        version: int | None = None,
        return_cas: bool = False,
        timeout: float | None = None,
    ) -> MutationResult:
        """Store only if the item's CAS still matches; CAS_MISMATCH otherwise."""
        return await self.set(
            key,
            value,
            ttl=ttl,
            compare_cas=cas_token,
            version=version,
            return_cas=return_cas,
            timeout=timeout,
        )

    async def append(
        self,
        key: Key,
        value: bytes,
        *,
        vivify_ttl: int | None = None,
        timeout: float | None = None,
    ) -> MutationResult:
        """Append bytes to an existing value; serialization is skipped."""
        return await self.set(
            key, value, mode="append", vivify_ttl=vivify_ttl, timeout=timeout
        )

    async def prepend(
        self,
        key: Key,
        value: bytes,
        *,
        vivify_ttl: int | None = None,
        timeout: float | None = None,
    ) -> MutationResult:
        """Prepend bytes to an existing value; serialization is skipped."""
        return await self.set(
            key, value, mode="prepend", vivify_ttl=vivify_ttl, timeout=timeout
        )

    async def delete(
        self,
        key: Key,
        *,
        compare_cas: int | None = None,
        timeout: float | None = None,
    ) -> MutationResult:
        return await self._one(  # type: ignore[return-value]
            Delete(key, compare_cas=compare_cas), timeout
        )

    async def invalidate(
        self,
        key: Key,
        *,
        stale_for: int | None = None,
        compare_cas: int | None = None,
        timeout: float | None = None,
    ) -> MutationResult:
        return await self._one(  # type: ignore[return-value]
            Delete(key, compare_cas=compare_cas, invalidate=True, stale_for=stale_for),
            timeout,
        )

    async def increment(
        self,
        key: Key,
        delta: int = 1,
        *,
        initial: int | None = None,
        initial_ttl: int | None = None,
        ttl: int | None = None,
        compare_cas: int | None = None,
        version: int | None = None,
        return_cas: bool = False,
        return_ttl: bool = False,
        timeout: float | None = None,
    ) -> ArithmeticResult:
        """Increment a counter; overflows wrap around (unsigned 64-bit)."""
        return await self._one(  # type: ignore[return-value]
            Arithmetic(
                key,
                delta,
                initial=initial,
                initial_ttl=initial_ttl,
                ttl=ttl,
                compare_cas=compare_cas,
                version=version,
                return_cas=return_cas,
                return_ttl=return_ttl,
            ),
            timeout,
        )

    async def decrement(
        self,
        key: Key,
        delta: int = 1,
        *,
        initial: int | None = None,
        initial_ttl: int | None = None,
        ttl: int | None = None,
        compare_cas: int | None = None,
        version: int | None = None,
        return_cas: bool = False,
        return_ttl: bool = False,
        timeout: float | None = None,
    ) -> ArithmeticResult:
        """Decrement a counter; saturates at zero instead of underflowing."""
        return await self._one(  # type: ignore[return-value]
            Arithmetic(
                key,
                delta,
                decrement=True,
                initial=initial,
                initial_ttl=initial_ttl,
                ttl=ttl,
                compare_cas=compare_cas,
                version=version,
                return_cas=return_cas,
                return_ttl=return_ttl,
            ),
            timeout,
        )

    async def touch(
        self, key: Key, ttl: int, *, timeout: float | None = None
    ) -> MutationResult:
        return touch_result(
            key, await self._one(Get(key, touch=ttl, value=False), timeout)
        )

    async def flush_all(self, delay: int = 0) -> None:
        if self._closed:
            raise RuntimeError("client is closed")
        positive("delay", delay)
        async with anyio.create_task_group() as tasks:
            for server in self._servers:
                tasks.start_soon(server.flush, delay)
