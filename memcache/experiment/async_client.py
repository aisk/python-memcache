"""Asynchronous scenario-level memcached client.

The same table of verbs as :class:`~memcache.experiment.Memcache` plus
``await``. The one behavioral difference is the factory refresh path: an
async winner returns the current value immediately and recomputes as a
background task owned by the client, so no request pays the refresh
latency. Background tasks require the client to be used as an async context
manager; without one the winner recomputes inline.
"""

from __future__ import annotations

from collections import deque
from contextlib import asynccontextmanager
from datetime import timedelta
from inspect import isawaitable
from typing import Any, cast
from collections.abc import (
    AsyncIterator,
    Callable,
    Iterable,
    Mapping,
    Sequence,
)

import anyio
import hashring

from ..async_connection import AsyncConnection
from ..connection import Addr
from ..errors import (
    AmbiguousWriteError,
    ConflictError,
    MemcacheError,
    NotFoundError,
    OperationFailedError,
    PipelineError,
    ProtocolError,
)
from ..meta_command import MetaCommand, MetaResult
from ..serialize import Serializer
from ._core import (
    FOREVER as FOREVER,
    MISSING,
    RAISE,
    UPDATE_ATTEMPTS,
    WAIT_BACKOFF,
    Call,
    ItemInfo,
    Key,
    ReadView,
    ScenarioBase,
    Ttl,
    WireOp,
    WireOutcome,
    dump_value,
    finalize_outcomes,
    key_bytes,
    normalize_addresses,
    pipeline_command,
    plan_election,
    plan_erase,
    plan_probe,
    plan_return_win,
    plan_store,
    resolve_group,
    routing_key,
    settle,
    wire_ttl,
)
from .client import Deferred
from .meta_api import (
    MetaCommandResult,
    Token,
    build_arithmetic,
    build_debug,
    build_delete,
    build_get,
    build_set,
    parse_debug_result,
    parse_meta_result,
    positive,
)


class _Server:
    """A memcached backend with an elastic FIFO pool of idle connections.

    Async counterpart of :class:`memcache.experiment.client._Server`; see
    that class for the pooling contract. Pool bookkeeping happens between
    awaits within a single event loop, so no lock is needed.
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
            except Exception:
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
        except Exception:
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

    async def flush(self, delay: int, timeout: float | None) -> None:
        async with self._borrow() as connection:
            if timeout is None:
                await connection.flush_all(delay)
                return
            with anyio.fail_after(timeout):
                await connection.flush_all(delay)

    async def close(self) -> None:
        self._closed = True
        while self._idle:
            connection = self._idle.popleft()
            try:
                await connection.close()
            except Exception:
                pass


class _AsyncFlight:
    """In-process merge point for one key's factory run.

    The winner publishes its result (or its exception) here and every
    same-process waiter shares it, so a process runs at most one factory
    per key. A waiter's own cancellation only stops its wait, never the
    factory.
    """

    __slots__ = ("_event", "_value", "_error")

    def __init__(self) -> None:
        self._event = anyio.Event()
        self._value: Any = None
        self._error: BaseException | None = None

    def resolve(self, value: Any) -> None:
        self._value = value
        self._event.set()

    def fail(self, error: BaseException) -> None:
        self._error = error
        self._event.set()

    async def wait(self) -> Any:
        await self._event.wait()
        if self._error is not None:
            raise self._error
        return self._value


class AsyncMetaNamespace:
    """Typed, flag-for-flag surface over the meta protocol commands.

    Async counterpart of :class:`memcache.experiment.client.MetaNamespace`;
    see that class for the layer's contract.
    """

    def __init__(self, client: "AsyncMemcache") -> None:
        self._client = client

    async def _run(
        self, command: MetaCommand, timeout: float | None
    ) -> MetaCommandResult:
        return parse_meta_result(await self._client._execute_meta(command, timeout))

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
            self._client._wire_key(key),
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
            self._client._wire_key(key),
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
            self._client._wire_key(key),
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
            self._client._wire_key(key),
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
            await self._client._execute_meta(
                build_debug(self._client._wire_key(key)), timeout
            )
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
            key=key_bytes(self._client._wire_key(key)),
            datalen=len(value) if value is not None else None,
            flags=list(flags),
            value=value,
        )
        return await self._client._execute_meta(meta, timeout)


class AsyncPipeline:
    """Collects independent scenario operations for one round trip per server.

    Async counterpart of :class:`memcache.experiment.client.Pipeline`: verbs
    record synchronously and return :class:`Deferred` results; execution
    happens when the ``async with`` block exits.
    """

    def __init__(self, client: "AsyncMemcache") -> None:
        self._client = client
        self._calls: list[Call] = []
        self._results: list[Deferred] = []
        self._done = False

    async def __aenter__(self) -> "AsyncPipeline":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            return
        await self._execute()

    def _push(self, call: Call) -> Deferred:
        if self._done:
            raise RuntimeError("pipeline was already executed")
        deferred = Deferred()
        self._calls.append(call)
        self._results.append(deferred)
        return deferred

    async def _execute(self) -> None:
        self._done = True
        outcomes = await self._client._run_ops([call.op for call in self._calls])
        for call, outcome, deferred in zip(self._calls, outcomes, self._results):
            try:
                deferred._value = await self._client._settle_call(call, outcome)
            except Exception as error:
                deferred._error = error

    def get(
        self, key: Key, default: Any = None, *, extend_ttl: Ttl | None = None
    ) -> Deferred:
        return self._push(self._client._call_get(key, default, extend_ttl))

    def set(self, key: Key, value: Any, ttl: Ttl) -> Deferred:
        return self._push(self._client._call_set(key, value, ttl))

    def delete(self, key: Key, grace: Ttl = 0) -> Deferred:
        return self._push(self._client._call_delete(key, grace))

    def add(self, key: Key, value: Any, ttl: Ttl) -> Deferred:
        return self._push(self._client._call_store_if(key, value, ttl, "add"))

    def replace(self, key: Key, value: Any, ttl: Ttl) -> Deferred:
        return self._push(self._client._call_store_if(key, value, ttl, "replace"))

    def incr(self, key: Key, delta: int = 1, *, ttl: Ttl) -> Deferred:
        return self._push(self._client._call_counter(key, delta, False, ttl))

    def decr(self, key: Key, delta: int = 1, *, ttl: Ttl) -> Deferred:
        return self._push(self._client._call_counter(key, delta, True, ttl))

    def touch(self, key: Key, ttl: Ttl) -> Deferred:
        return self._push(self._client._call_touch(key, ttl))

    def append(self, key: Key, fragment: Any, ttl: Ttl) -> Deferred:
        return self._push(self._client._call_concat(key, fragment, ttl, "append"))

    def prepend(self, key: Key, fragment: Any, ttl: Ttl) -> Deferred:
        return self._push(self._client._call_concat(key, fragment, ttl, "prepend"))

    def inspect(self, key: Key) -> Deferred:
        return self._push(self._client._call_inspect(key))


class AsyncMemcache(ScenarioBase):
    """Scenario-level asynchronous client; the sync table plus ``await``.

    ``factory`` and ``fn`` arguments accept sync or async callables. Use it
    as an async context manager so background refreshes and other
    housekeeping run as tasks owned by the client and are cancelled on
    close; without it they run inline in the calling coroutine.
    """

    def __init__(
        self,
        *servers: Addr,
        serializer: Serializer | None = None,
        prefix: str = "",
        on_error: str = "raise",
        on_failure: Callable[[BaseException], None] | None = None,
        timeout: float | None = 1.0,
        username: str | None = None,
        password: str | None = None,
        max_idle: int | None = 23,
    ) -> None:
        super().__init__(serializer, prefix, on_error, on_failure, timeout)
        self._servers = [
            _Server(addr, username=username, password=password, max_idle=max_idle)
            for addr in normalize_addresses(servers)
        ]
        self._ring = hashring.HashRing(self._servers)
        self._flights: dict[bytes, _AsyncFlight] = {}
        self._task_group: Any = None
        self.meta = AsyncMetaNamespace(self)

    async def __aenter__(self) -> "AsyncMemcache":
        self._task_group = anyio.create_task_group()
        await self._task_group.__aenter__()
        return self

    async def __aexit__(self, *exc: Any) -> Any:
        task_group = self._task_group
        self._task_group = None
        if task_group is None:
            await self.close()
            return None
        task_group.cancel_scope.cancel()
        try:
            suppress = await task_group.__aexit__(*exc)
        finally:
            await self.close()
        # The cancelled task group reports its own shutdown cancellation as
        # handled; that must never suppress the caller's real exception.
        return suppress and exc[0] is None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._task_group is not None:
            self._task_group.cancel_scope.cancel()
        for server in self._servers:
            await server.close()

    def _spawn(self, func: Callable[..., Any], *args: Any) -> bool:
        """Start background work under the client's task group, if it has one."""
        if self._task_group is None or self._closed:
            return False
        self._task_group.start_soon(func, *args)
        return True

    # ------------------------------------------------------------------
    # engine

    def _server_for(self, key: bytes) -> _Server:
        return cast(_Server, self._ring.get_node(routing_key(key)))

    async def _execute_meta(
        self, command: MetaCommand, timeout: float | None = None
    ) -> MetaResult:
        self._check_open()
        return await self._server_for(command.key).execute(
            command, self._timeout if timeout is None else timeout
        )

    async def _run_ops(self, ops: Sequence[WireOp]) -> list[WireOutcome]:
        self._check_open()
        grouped: dict[_Server, list[tuple[int, WireOp]]] = {}
        for index, op in enumerate(ops):
            grouped.setdefault(self._server_for(op.command.key), []).append((index, op))
        output: list[WireOutcome | None] = [None] * len(ops)
        async with anyio.create_task_group() as tasks:
            for server, group in grouped.items():
                tasks.start_soon(self._run_group, server, group, output)
        return finalize_outcomes(output)

    async def _run_group(
        self,
        server: _Server,
        group: list[tuple[int, WireOp]],
        output: list[WireOutcome | None],
    ) -> None:
        commands = [pipeline_command(index, op) for index, op in group]
        try:
            responses = await server.pipeline(commands, self._timeout)
        except PipelineError as exc:
            resolve_group(
                group,
                output,
                exc.responses,
                exc.written,
                False,
                exc.cause,
                exc.confirmed,
            )
        except TimeoutError as exc:
            # The timeout cancelled the pipeline mid-flight and the precise
            # written count is lost, so every side-effect command counts as
            # possibly landed.
            resolve_group(group, output, [], len(group), False, exc, 0)
        except Exception as exc:
            resolve_group(group, output, [], 0, False, exc, 0)
        else:
            resolve_group(group, output, responses, len(group), True, None, 0)

    async def _run_one(self, op: WireOp) -> WireOutcome:
        return (await self._run_ops([op]))[0]

    # ------------------------------------------------------------------
    # shared call plumbing

    async def _settle_call(self, call: Call, outcome: WireOutcome) -> Any:
        try:
            value, win = call.finish(outcome)
        except OperationFailedError as exc:
            if call.absorb is not RAISE and self._absorb(exc):
                return call.absorb
            raise
        if win is not None:
            await self._return_win(call.key, win)
        return value

    async def _run_call(self, call: Call) -> Any:
        return await self._settle_call(call, await self._run_one(call.op))

    async def _return_win(self, key: Key, view: ReadView) -> None:
        """Hand an accidentally consumed stale-recache token back."""
        if view.cas is None:
            return
        op = plan_return_win(self._wire_key(key), view)
        if not self._spawn(self._run_win, key, op):
            await self._run_win(key, op)

    async def _run_win(self, key: Key, op: WireOp) -> None:
        try:
            settle(key, await self._run_one(op))
        except Exception as exc:
            self._report(exc)

    # ------------------------------------------------------------------
    # reading (S1/S3/S4/S9)

    async def get(
        self,
        key: Key,
        default: Any = None,
        *,
        factory: Callable[[], Any] | None = None,
        ttl: Ttl | None = None,
        refresh_ahead: int | timedelta | None = None,
        extend_ttl: Ttl | None = None,
    ) -> Any:
        """Read a value; a miss returns ``default``.

        See :meth:`memcache.experiment.Memcache.get`. The async refresh
        paths (refresh_ahead window and stale grace) return the current
        value immediately and recompute in a background task.
        """
        self._check_open()
        factory_args = self._factory_args(factory, ttl, refresh_ahead, extend_ttl)
        if factory_args is not None:
            assert factory is not None
            return await self._factory_get(key, factory, *factory_args)
        return await self._run_call(self._call_get(key, default, extend_ttl))

    # ------------------------------------------------------------------
    # writing and invalidation (S1/S5)

    async def set(self, key: Key, value: Any, ttl: Ttl) -> None:
        """Store a value for ``ttl``: seconds, a timedelta, or an aware
        datetime for the absolute moment of expiry (:data:`FOREVER` for no
        expiry). Every lifetime parameter accepts the same forms."""
        self._check_open()
        await self._run_call(self._call_set(key, value, ttl))

    async def delete(self, key: Key, grace: Ttl = 0) -> bool:
        """Invalidate a key; see :meth:`memcache.experiment.Memcache.delete`."""
        self._check_open()
        return cast(bool, await self._run_call(self._call_delete(key, grace)))

    # ------------------------------------------------------------------
    # aggregation (S2)

    async def get_many(self, keys: Iterable[Key]) -> dict[Key, Any]:
        """Read many keys in one round trip per server; hits only."""
        self._check_open()
        calls = [self._call_get(key, MISSING, None) for key in keys]
        outcomes = await self._run_ops([call.op for call in calls])
        found: dict[Key, Any] = {}
        for call, outcome in zip(calls, outcomes):
            value = await self._settle_call(call, outcome)
            if value is not MISSING:
                found[call.key] = value
        return found

    async def set_many(self, mapping: Mapping[Key, Any], ttl: Ttl) -> None:
        """Store many values in one round trip per server."""
        self._check_open()
        calls = [self._call_set(key, value, ttl) for key, value in mapping.items()]
        await self._finish_writes(calls)

    async def delete_many(self, keys: Iterable[Key]) -> None:
        """Erase many keys in one round trip per server."""
        self._check_open()
        calls = [self._call_delete(key, 0) for key in keys]
        await self._finish_writes(calls)

    async def _finish_writes(self, calls: list[Call]) -> None:
        outcomes = await self._run_ops([call.op for call in calls])
        first_error: BaseException | None = None
        for call, outcome in zip(calls, outcomes):
            try:
                await self._settle_call(call, outcome)
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    # ------------------------------------------------------------------
    # concurrent mutation (S6) and event buffers (S11)

    async def update(
        self,
        key: Key,
        fn: Callable[[Any], Any],
        *,
        default: Any = MISSING,
        ttl: Ttl,
    ) -> Any:
        """Atomically transform a value and store the result for ``ttl``.

        See :meth:`memcache.experiment.Memcache.update`; ``fn`` may be sync
        or async and must be pure either way.
        """
        self._check_open()
        ttl = wire_ttl(ttl)
        wire_key = self._wire_key(key)
        probe_op, probe = plan_probe(wire_key, key)
        for _ in range(UPDATE_ATTEMPTS):
            view = probe(await self._run_one(probe_op))
            stale_win = view.stale and view.won
            compare_cas = view.cas if view.hit else None
            if view.hit and view.data and not view.stale:
                current = self._serializer.load(key, view.data, view.client_flags)
            elif default is MISSING:
                if stale_win:
                    await self._return_win(key, view)
                raise NotFoundError(
                    "key %r was not found and update() got no default" % (key,)
                )
            else:
                current = default
            try:
                new_value = fn(current)
                if isawaitable(new_value):
                    new_value = await new_value
                raw, client_flags = dump_value(self._serializer, key, new_value)
            except BaseException:
                if stale_win:
                    await self._return_win(key, view)
                raise
            if compare_cas is not None:
                write_op, stored = plan_store(
                    wire_key, key, raw, client_flags, ttl, compare_cas=compare_cas
                )
            else:
                write_op, stored = plan_store(
                    wire_key, key, raw, client_flags, ttl, mode="add"
                )
            if stored(await self._run_one(write_op)):
                return new_value
        raise ConflictError(
            "update of key %r kept conflicting with concurrent writes" % (key,)
        )

    async def pop(self, key: Key, default: Any = None) -> Any:
        """Atomically take a value and delete it; a miss returns ``default``."""
        self._check_open()
        wire_key = self._wire_key(key)
        probe_op, probe = plan_probe(wire_key, key)
        for _ in range(UPDATE_ATTEMPTS):
            view = probe(await self._run_one(probe_op))
            if not view.hit or not view.data:
                if view.stale and view.won:
                    await self._return_win(key, view)
                return default
            if view.cas is None:
                error = OperationFailedError(key)
                error.__cause__ = ProtocolError("read omitted the requested version")
                raise error
            value = self._serializer.load(key, view.data, view.client_flags)
            erase_op, erased = plan_erase(wire_key, key, compare_cas=view.cas)
            if erased(await self._run_one(erase_op)):
                return value
        raise ConflictError(
            "pop of key %r kept conflicting with concurrent writes" % (key,)
        )

    async def append(self, key: Key, fragment: Any, ttl: Ttl) -> None:
        """Append bytes to a byte-stream value, creating it on a miss."""
        self._check_open()
        await self._run_call(self._call_concat(key, fragment, ttl, "append"))

    async def prepend(self, key: Key, fragment: Any, ttl: Ttl) -> None:
        """Prepend bytes to a byte-stream value; see :meth:`append`."""
        self._check_open()
        await self._run_call(self._call_concat(key, fragment, ttl, "prepend"))

    # ------------------------------------------------------------------
    # counters (S7)

    async def incr(self, key: Key, delta: int = 1, *, ttl: Ttl) -> int:
        """Atomically add ``delta``; a miss counts from zero."""
        self._check_open()
        return cast(
            int, await self._run_call(self._call_counter(key, delta, False, ttl))
        )

    async def decr(self, key: Key, delta: int = 1, *, ttl: Ttl) -> int:
        """Atomically subtract ``delta``, saturating at zero."""
        self._check_open()
        return cast(
            int, await self._run_call(self._call_counter(key, delta, True, ttl))
        )

    # ------------------------------------------------------------------
    # conditional blind writes (S8/S9)

    async def add(self, key: Key, value: Any, ttl: Ttl) -> bool:
        """Store only if the key does not exist; True when this call won."""
        self._check_open()
        return cast(
            bool, await self._run_call(self._call_store_if(key, value, ttl, "add"))
        )

    async def replace(self, key: Key, value: Any, ttl: Ttl) -> bool:
        """Store only if the key still exists; never resurrects a deleted key."""
        self._check_open()
        return cast(
            bool,
            await self._run_call(self._call_store_if(key, value, ttl, "replace")),
        )

    async def touch(self, key: Key, ttl: Ttl) -> bool:
        """Extend a key's ttl without transferring its value."""
        self._check_open()
        return cast(bool, await self._run_call(self._call_touch(key, ttl)))

    # ------------------------------------------------------------------
    # observation (S13) and batching (S12)

    async def inspect(self, key: Key) -> ItemInfo | None:
        """Read an item's metadata without transferring the value."""
        self._check_open()
        return cast(ItemInfo | None, await self._run_call(self._call_inspect(key)))

    def pipeline(self) -> AsyncPipeline:
        """Batch independent operations into one round trip per server."""
        self._check_open()
        return AsyncPipeline(self)

    # ------------------------------------------------------------------
    # factory machinery (S3/S4/S5)

    async def _call_factory(self, factory: Callable[[], Any]) -> Any:
        value = factory()
        if isawaitable(value):
            value = await value
        return value

    def _claim_flight(self, merge_key: bytes) -> tuple[_AsyncFlight, bool]:
        flight = self._flights.get(merge_key)
        if flight is not None:
            return flight, False
        flight = _AsyncFlight()
        self._flights[merge_key] = flight
        return flight, True

    def _finish_flight(
        self,
        merge_key: bytes,
        flight: _AsyncFlight,
        value: Any = None,
        error: BaseException | None = None,
    ) -> None:
        if self._flights.get(merge_key) is flight:
            del self._flights[merge_key]
        if error is not None:
            flight.fail(error)
        else:
            flight.resolve(value)

    async def _factory_get(
        self,
        key: Key,
        factory: Callable[[], Any],
        ttl: int,
        refresh_ahead: int | None,
    ) -> Any:
        op, election = plan_election(self._wire_key(key), key, refresh_ahead)
        for attempt in range(len(WAIT_BACKOFF) + 1):
            try:
                view = election(await self._run_one(op))
            except (OperationFailedError, AmbiguousWriteError) as exc:
                if self._degrade:
                    # Cache outage is not a site outage: compute locally and
                    # skip the cache, keeping the failure observable.
                    self._report(exc)
                    return await self._local_compute(key, factory)
                raise
            if view.hit and view.data:
                value = self._serializer.load(key, view.data, view.client_flags)
                if view.won and view.cas is not None:
                    # Refresh-ahead or stale-grace win: hand the current
                    # value back now and recompute in the background, so no
                    # request pays the refresh latency.
                    await self._refresh(key, factory, ttl, view.cas)
                return value
            if view.hit and view.cas is not None and (view.won or not view.busy):
                # The vivified placeholder this call won, or a zero-byte
                # entry nobody else is rewriting: recompute and replace it
                # through its version.
                return await self._lead(key, factory, ttl, view.cas, release=True)
            if not view.hit:
                # A miss despite vivify offers no coordination; compute
                # without write-back, merged with any same-process run.
                return await self._local_compute(key, factory)
            # Another caller holds the lease. Same-process callers share its
            # pending result; cross-process losers wait briefly and re-read.
            flight = self._flights.get(self._merge_key(key))
            if flight is not None:
                return await flight.wait()
            if attempt >= len(WAIT_BACKOFF):
                return await self._local_compute(key, factory)
            await anyio.sleep(WAIT_BACKOFF[attempt])
        raise AssertionError("unreachable")

    async def _lead(
        self,
        key: Key,
        factory: Callable[[], Any],
        ttl: int,
        cas: int,
        release: bool,
    ) -> Any:
        """Run the factory as the elected miss-path winner.

        The factory runs as a task owned by the client rather than inside
        the winning caller's cancel scope: its result is shared by every
        same-process waiter, and one short-deadline caller must not cancel
        it for everyone. Each waiter's own cancellation only ends its wait.
        """
        merge_key = self._merge_key(key)
        flight, leader = self._claim_flight(merge_key)
        if leader and not self._spawn(
            self._run_load, key, merge_key, flight, factory, ttl, cas, release
        ):
            await self._run_load(key, merge_key, flight, factory, ttl, cas, release)
        return await flight.wait()

    async def _run_load(
        self,
        key: Key,
        merge_key: bytes,
        flight: _AsyncFlight,
        factory: Callable[[], Any],
        ttl: int,
        cas: int,
        release: bool,
    ) -> None:
        try:
            value = await self._call_factory(factory)
        except BaseException as exc:
            cancelled = isinstance(exc, anyio.get_cancelled_exc_class())
            published: BaseException = (
                MemcacheError("factory run for %r was cancelled" % (key,))
                if cancelled
                else exc
            )
            self._finish_flight(merge_key, flight, error=published)
            if release:
                with anyio.CancelScope(shield=True):
                    await self._release_lease(key, cas)
            if cancelled:
                raise
            return
        await self._write_back(key, value, ttl, cas, release)
        self._finish_flight(merge_key, flight, value=value)

    async def _refresh(
        self, key: Key, factory: Callable[[], Any], ttl: int, cas: int
    ) -> None:
        merge_key = self._merge_key(key)
        flight, leader = self._claim_flight(merge_key)
        if not leader:
            # A load or refresh for this key is already in flight here.
            return
        if not self._spawn(
            self._run_refresh, key, merge_key, flight, factory, ttl, cas
        ):
            await self._run_refresh(key, merge_key, flight, factory, ttl, cas)

    async def _run_refresh(
        self,
        key: Key,
        merge_key: bytes,
        flight: _AsyncFlight,
        factory: Callable[[], Any],
        ttl: int,
        cas: int,
    ) -> None:
        try:
            value = await self._call_factory(factory)
        except BaseException as exc:
            cancelled = isinstance(exc, anyio.get_cancelled_exc_class())
            published: BaseException = (
                MemcacheError("factory run for %r was cancelled" % (key,))
                if cancelled
                else exc
            )
            self._finish_flight(merge_key, flight, error=published)
            if cancelled:
                raise
            # The background path has no caller; its failures are
            # observability events.
            self._report(exc)
            return
        await self._write_back(key, value, ttl, cas, release=False)
        self._finish_flight(merge_key, flight, value=value)

    async def _local_compute(self, key: Key, factory: Callable[[], Any]) -> Any:
        """Run the factory without write-back, merged per process."""
        merge_key = self._merge_key(key)
        flight, leader = self._claim_flight(merge_key)
        if not leader:
            return await flight.wait()
        try:
            value = await self._call_factory(factory)
        except BaseException as exc:
            cancelled = isinstance(exc, anyio.get_cancelled_exc_class())
            published: BaseException = (
                MemcacheError("factory run for %r was cancelled" % (key,))
                if cancelled
                else exc
            )
            self._finish_flight(merge_key, flight, error=published)
            raise
        self._finish_flight(merge_key, flight, value=value)
        return value

    async def _write_back(
        self, key: Key, value: Any, ttl: int, cas: int, release: bool
    ) -> None:
        """Store a recomputed value conditioned on the election's version.

        See :meth:`memcache.experiment.client.Memcache._write_back`; the
        async version additionally shields against the caller's own
        cancellation once a value exists, so a computed result is not
        thrown away at the last step.
        """
        try:
            raw, client_flags = dump_value(self._serializer, key, value)
        except Exception as exc:
            self._report(exc)
            if release:
                await self._release_lease(key, cas)
            return
        op, stored = plan_store(
            self._wire_key(key), key, raw, client_flags, ttl, compare_cas=cas
        )
        try:
            applied = stored(await self._run_one(op))
        except Exception as exc:
            self._report(exc)
            return
        if not applied:
            self._report(
                MemcacheError(
                    "factory write-back of %r abandoned: the entry changed "
                    "during recomputation" % (key,)
                )
            )

    async def _release_lease(self, key: Key, cas: int) -> None:
        """Delete the zero-byte placeholder whose lease went unpaid."""
        op, erased = plan_erase(self._wire_key(key), key, compare_cas=cas)
        try:
            erased(await self._run_one(op))
        except Exception as exc:
            self._report(exc)

    # ------------------------------------------------------------------
    # maintenance

    async def flush_all(self, delay: int = 0) -> None:
        """Expire every item on every server; an operations tool, not a verb."""
        self._check_open()
        positive("delay", delay)
        async with anyio.create_task_group() as tasks:
            for server in self._servers:
                tasks.start_soon(server.flush, delay, self._timeout)


__all__ = [
    "AsyncMemcache",
    "AsyncMetaNamespace",
    "AsyncPipeline",
    "FOREVER",
    "ItemInfo",
]
