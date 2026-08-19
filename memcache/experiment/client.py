"""Synchronous scenario-level memcached client.

One scenario, one door: every public method exists because a concrete usage
scenario demands it, takes business intent only, and returns a business
value. Protocol capabilities without a scenario stay behind ``cache.meta``,
the 1:1 typed protocol surface.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from time import monotonic
from typing import Any, cast
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

import hashring

from ..connection import Addr, Connection, chunk_pipeline
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
    Ttl as Ttl,
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

_UNSET = object()


def _deadline(timeout: float | None) -> float | None:
    return None if timeout is None else monotonic() + timeout


def _remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise TimeoutError("batch deadline exceeded")
    return remaining


class _Server:
    """A memcached backend with an elastic FIFO pool of idle connections.

    Borrowing takes the oldest idle connection or creates a new one when the
    pool is empty; callers never block waiting for a connection. A connection
    is only returned after a fully completed request/response cycle — one
    that failed mid-cycle may hold unread response bytes, so it is closed
    instead of poisoning the next borrower. FIFO reuse keeps every pooled
    connection recently used, so idle-timeout middleboxes cannot silently
    kill the pool's tail.
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
        self._idle: deque[Connection] = deque()
        self._lock = threading.Lock()
        self._closed = False

    def __repr__(self) -> str:
        return "%s:%d" % self.addr

    def _new_connection(self, timeout: float | None) -> Connection:
        return Connection(
            self.addr,
            username=self._username,
            password=self._password,
            timeout=timeout,
        )

    def _acquire(self, timeout: float | None) -> Connection:
        with self._lock:
            if self._closed:
                raise RuntimeError("client is closed")
            connection = self._idle.popleft() if self._idle else None
        return connection if connection is not None else self._new_connection(timeout)

    def _discard(self, connection: Connection) -> None:
        try:
            connection.close()
        except Exception:
            pass

    @contextmanager
    def _borrow(self, timeout: float | None) -> Iterator[Connection]:
        connection = self._acquire(timeout)
        try:
            yield connection
        except BaseException:
            self._discard(connection)
            raise
        else:
            self._release(connection)

    def _release(self, connection: Connection) -> None:
        with self._lock:
            if not self._closed and (
                self._max_idle is None or len(self._idle) < self._max_idle
            ):
                self._idle.append(connection)
                return
        self._discard(connection)

    def start_pipeline(
        self, commands: list[MetaCommand], deadline: float | None
    ) -> _InFlight:
        """Write the pipeline's first chunk and return it with responses unread.

        Only the first chunk goes out here so the other servers in a batch can
        start theirs right away; the rest is written from :meth:`_InFlight
        .finish`, interleaved with reading, so the request side never outruns
        the socket buffers.
        """
        chunks = chunk_pipeline(commands)
        connection = self._acquire(_remaining(deadline))
        try:
            connection.send_pipeline(chunks[0], _remaining(deadline))
        except BaseException:
            self._discard(connection)
            raise
        return _InFlight(self, connection, chunks)

    def execute(self, command: MetaCommand, timeout: float | None) -> MetaResult:
        with self._borrow(timeout) as connection:
            return connection.execute_meta_command(command, timeout)

    def flush(self, delay: int, timeout: float | None) -> None:
        with self._borrow(timeout) as connection:
            connection.flush_all(delay, timeout)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            idle, self._idle = list(self._idle), deque()
        for connection in idle:
            self._discard(connection)


class _InFlight:
    """A pipeline whose commands are written and whose responses are pending."""

    def __init__(
        self, server: _Server, connection: Connection, chunks: list[list[MetaCommand]]
    ) -> None:
        self._server = server
        self._connection = connection
        self._chunks = chunks
        self.count = sum(len(chunk) for chunk in chunks)

    def finish(self, deadline: float | None) -> list[MetaResult]:
        """Read each chunk's barrier, sending the next one as room frees up."""
        responses: list[MetaResult] = []
        written = len(self._chunks[0])
        confirmed = 0
        try:
            for position, chunk in enumerate(self._chunks):
                responses.extend(
                    self._connection.receive_pipeline(written, _remaining(deadline))
                )
                confirmed += len(chunk)
                if position + 1 < len(self._chunks):
                    following = self._chunks[position + 1]
                    self._connection.send_pipeline(following, _remaining(deadline))
                    written += len(following)
        except BaseException as exc:
            self._server._discard(self._connection)
            if not isinstance(exc, Exception):
                raise
            if isinstance(exc, PipelineError):
                raise PipelineError(
                    max(written, exc.written),
                    responses + exc.responses,
                    exc.cause,
                    confirmed,
                )
            # Everything written so far is on the wire, so an expired deadline
            # here still leaves the unconfirmed side effects ambiguous.
            raise PipelineError(written, responses, exc, confirmed)
        self._server._release(self._connection)
        return responses


@dataclass
class _PipelineRun:
    """What one server's pipeline produced, successful or not."""

    responses: list[MetaResult]
    written: int
    barrier: bool
    error: BaseException | None
    confirmed: int = 0


class _Flight:
    """In-process merge point for one key's factory run.

    The winner publishes its result (or its exception) here and every
    same-process waiter shares it, so a process runs at most one factory
    per key.
    """

    __slots__ = ("_done", "_value", "_error")

    def __init__(self) -> None:
        self._done = threading.Event()
        self._value: Any = None
        self._error: BaseException | None = None

    def resolve(self, value: Any) -> None:
        self._value = value
        self._done.set()

    def fail(self, error: BaseException) -> None:
        self._error = error
        self._done.set()

    def wait(self) -> Any:
        self._done.wait()
        if self._error is not None:
            raise self._error
        return self._value


class MetaNamespace:
    """Typed, flag-for-flag surface over the meta protocol commands.

    Methods map 1:1 to the wire commands (``mg``/``ms``/``md``/``ma``/``me``)
    with one keyword argument per protocol flag. Values are raw bytes and
    responses are lightly parsed :class:`MetaCommandResult` objects; no
    serialization and no semantic interpretation happens here. The
    bytes-level ``execute`` escape hatch remains for anything the typed
    surface does not cover. The client's ``prefix`` applies here too, so the
    escape hatch sees the same keys the scenario layer stores.
    """

    def __init__(self, client: "Memcache") -> None:
        self._client = client

    def _run(self, command: MetaCommand, timeout: float | None) -> MetaCommandResult:
        return parse_meta_result(self._client._execute_meta(command, timeout))

    def get(
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
        return self._run(command, timeout)

    def set(
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
        return self._run(command, timeout)

    def delete(
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
        return self._run(command, timeout)

    def arithmetic(
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
        return self._run(command, timeout)

    def debug(self, key: Key, *, timeout: float | None = None) -> dict[str, str] | None:
        return parse_debug_result(
            self._client._execute_meta(
                build_debug(self._client._wire_key(key)), timeout
            )
        )

    def execute(
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
        return self._client._execute_meta(meta, timeout)


class Deferred:
    """A pipeline operation's result, readable after the with block exits.

    Just a value that arrives late: ``.value`` returns exactly what the
    direct call would have returned, or raises exactly what it would have
    raised.
    """

    __slots__ = ("_value", "_error")

    def __init__(self) -> None:
        self._value: Any = _UNSET
        self._error: BaseException | None = None

    @property
    def value(self) -> Any:
        if self._error is not None:
            raise self._error
        if self._value is _UNSET:
            raise RuntimeError(
                "pipeline results are readable after the with block exits"
            )
        return self._value


class Pipeline:
    """Collects independent scenario operations for one round trip per server.

    The verbs, signatures, and semantics match the client's; the only
    difference is that each call returns a :class:`Deferred` whose ``value``
    becomes readable once the with block exits. One failing operation only
    affects its own deferred result. Operations that are themselves multiple
    round trips (``get`` with a factory, ``update``, ``pop``, the ``_many``
    family) are not available here.
    """

    def __init__(self, client: "Memcache") -> None:
        self._client = client
        self._calls: list[Call] = []
        self._results: list[Deferred] = []
        self._done = False

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            return
        self._execute()

    def _push(self, call: Call) -> Deferred:
        if self._done:
            raise RuntimeError("pipeline was already executed")
        deferred = Deferred()
        self._calls.append(call)
        self._results.append(deferred)
        return deferred

    def _execute(self) -> None:
        self._done = True
        outcomes = self._client._run_ops([call.op for call in self._calls])
        for call, outcome, deferred in zip(self._calls, outcomes, self._results):
            try:
                deferred._value = self._client._settle_call(call, outcome)
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


class Memcache(ScenarioBase):
    """Scenario-level synchronous client.

    Policies (serializer, key prefix, failure behavior) live here in the
    constructor; call sites express business intent only. A ttl is a
    required part of every write, with :data:`FOREVER` (0) for entries that
    must never expire.
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
        self._flights: dict[bytes, _Flight] = {}
        self._flights_lock = threading.Lock()
        self.meta = MetaNamespace(self)

    def __enter__(self) -> "Memcache":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for server in self._servers:
            server.close()

    # ------------------------------------------------------------------
    # engine

    def _server_for(self, key: bytes) -> _Server:
        return cast(_Server, self._ring.get_node(routing_key(key)))

    def _execute_meta(
        self, command: MetaCommand, timeout: float | None = None
    ) -> MetaResult:
        self._check_open()
        return self._server_for(command.key).execute(
            command, self._timeout if timeout is None else timeout
        )

    @staticmethod
    def _failed_run(error: BaseException) -> _PipelineRun:
        if isinstance(error, PipelineError):
            return _PipelineRun(
                error.responses, error.written, False, error, error.confirmed
            )
        return _PipelineRun([], 0, False, error)

    def _run_pipelines(
        self, grouped: dict[_Server, list[MetaCommand]]
    ) -> dict[_Server, _PipelineRun]:
        """Write every server's pipeline, then read them back one by one.

        The servers process their pipelines concurrently while this thread
        drains responses sequentially, so the fan-out needs no worker
        threads and its latency still tracks the slowest server. One server
        failing never disturbs another; the constructor timeout bounds the
        whole batch as a single deadline.
        """
        deadline = _deadline(self._timeout)
        runs: dict[_Server, _PipelineRun] = {}
        pending: list[tuple[_Server, _InFlight]] = []
        for server, commands in grouped.items():
            try:
                pending.append((server, server.start_pipeline(commands, deadline)))
            except Exception as exc:
                runs[server] = self._failed_run(exc)
        for server, flight in pending:
            try:
                responses = flight.finish(deadline)
            except Exception as exc:
                runs[server] = self._failed_run(exc)
            else:
                runs[server] = _PipelineRun(responses, flight.count, True, None)
        return runs

    def _run_ops(self, ops: Sequence[WireOp]) -> list[WireOutcome]:
        self._check_open()
        grouped: dict[_Server, list[tuple[int, WireOp]]] = {}
        for index, op in enumerate(ops):
            grouped.setdefault(self._server_for(op.command.key), []).append((index, op))
        runs = self._run_pipelines(
            {
                server: [pipeline_command(index, op) for index, op in group]
                for server, group in grouped.items()
            }
        )
        output: list[WireOutcome | None] = [None] * len(ops)
        for server, group in grouped.items():
            run = runs[server]
            cause = (
                run.error.cause if isinstance(run.error, PipelineError) else run.error
            )
            resolve_group(
                group,
                output,
                run.responses,
                run.written,
                run.barrier,
                cause,
                run.confirmed,
            )
        return finalize_outcomes(output)

    def _run_one(self, op: WireOp) -> WireOutcome:
        return self._run_ops([op])[0]

    # ------------------------------------------------------------------
    # shared call plumbing

    def _settle_call(self, call: Call, outcome: WireOutcome) -> Any:
        try:
            value, win = call.finish(outcome)
        except OperationFailedError as exc:
            if call.absorb is not RAISE and self._absorb(exc):
                return call.absorb
            raise
        if win is not None:
            self._return_win(call.key, win)
        return value

    def _run_call(self, call: Call) -> Any:
        return self._settle_call(call, self._run_one(call.op))

    def _return_win(self, key: Key, view: ReadView) -> None:
        """Hand an accidentally consumed stale-recache token back."""
        if view.cas is None:
            return
        try:
            settle(key, self._run_one(plan_return_win(self._wire_key(key), view)))
        except Exception as exc:
            self._report(exc)

    # ------------------------------------------------------------------
    # reading (S1/S3/S4/S9)

    def get(
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

        With ``factory`` a miss computes the value, writes it back, and is
        stampede-safe: one winner recomputes per key while other readers
        share its result; ``ttl`` is then required and ``refresh_ahead``
        optionally recomputes ahead of expiry so hot keys never actually
        expire. With ``extend_ttl`` a hit also slides the expiry; the
        extension is memcached's native blind touch and applies to whatever
        it hits, including entries kept stale by ``delete(grace=...)``.
        """
        self._check_open()
        factory_args = self._factory_args(factory, ttl, refresh_ahead, extend_ttl)
        if factory_args is not None:
            assert factory is not None
            return self._factory_get(key, factory, *factory_args)
        return self._run_call(self._call_get(key, default, extend_ttl))

    # ------------------------------------------------------------------
    # writing and invalidation (S1/S5)

    def set(self, key: Key, value: Any, ttl: Ttl) -> None:
        """Store a value for ``ttl``: seconds, a timedelta, or an aware
        datetime for the absolute moment of expiry (:data:`FOREVER` for no
        expiry). Every lifetime parameter accepts the same forms."""
        self._check_open()
        self._run_call(self._call_set(key, value, ttl))

    def delete(self, key: Key, grace: Ttl = 0) -> bool:
        """Invalidate a key; returns whether there was something to erase.

        ``grace=0`` erases outright: the next reader pays a full miss.
        ``grace>0`` marks the entry stale instead: for that many seconds
        plain readers keep the old value while factory readers elect one
        recomputation, after which everyone sees the new value. Soft
        invalidation belongs to factory-managed keys; a revocation that
        must stick (sessions) uses the hard form.
        """
        self._check_open()
        return cast(bool, self._run_call(self._call_delete(key, grace)))

    # ------------------------------------------------------------------
    # aggregation (S2)

    def get_many(self, keys: Iterable[Key]) -> dict[Key, Any]:
        """Read many keys in one round trip per server; hits only.

        The result is keyed by the key objects the caller passed. In
        degrade mode a failing server only removes its own keys from the
        result.
        """
        self._check_open()
        calls = [self._call_get(key, MISSING, None) for key in keys]
        outcomes = self._run_ops([call.op for call in calls])
        found: dict[Key, Any] = {}
        for call, outcome in zip(calls, outcomes):
            value = self._settle_call(call, outcome)
            if value is not MISSING:
                found[call.key] = value
        return found

    def set_many(self, mapping: Mapping[Key, Any], ttl: Ttl) -> None:
        """Store many values in one round trip per server."""
        self._check_open()
        calls = [self._call_set(key, value, ttl) for key, value in mapping.items()]
        self._finish_writes(calls)

    def delete_many(self, keys: Iterable[Key]) -> None:
        """Erase many keys in one round trip per server."""
        self._check_open()
        calls = [self._call_delete(key, 0) for key in keys]
        self._finish_writes(calls)

    def _finish_writes(self, calls: list[Call]) -> None:
        outcomes = self._run_ops([call.op for call in calls])
        first_error: BaseException | None = None
        for call, outcome in zip(calls, outcomes):
            try:
                self._settle_call(call, outcome)
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    # ------------------------------------------------------------------
    # concurrent mutation (S6) and event buffers (S11)

    def update(
        self,
        key: Key,
        fn: Callable[[Any], Any],
        *,
        default: Any = MISSING,
        ttl: Ttl,
    ) -> Any:
        """Atomically transform a value and store the result for ``ttl``.

        Reads with a version, applies ``fn``, writes back only if the entry
        did not change in between, and retries on conflict; version tokens
        never appear in caller code. ``fn`` must be pure: it may run several
        times. Any exception raised by ``fn`` aborts the call, leaving the
        entry unwritten, and propagates unchanged. A miss starts from
        ``default``; without one it raises :class:`NotFoundError`. An entry
        kept stale by ``delete(grace=...)`` counts as a miss: ``fn``
        transforms rather than recomputes, and transforming invalidated
        data would silently launder it back to fresh. Exhausted retries
        raise :class:`ConflictError`.
        """
        self._check_open()
        ttl = wire_ttl(ttl)
        wire_key = self._wire_key(key)
        probe_op, probe = plan_probe(wire_key, key)
        for _ in range(UPDATE_ATTEMPTS):
            view = probe(self._run_one(probe_op))
            stale_win = view.stale and view.won
            # A stale or zero-byte hit reads as a miss, but the entry still
            # exists: only a compare-and-swap write can replace it.
            compare_cas = view.cas if view.hit else None
            if view.hit and view.data and not view.stale:
                current = self._serializer.load(key, view.data, view.client_flags)
            elif default is MISSING:
                if stale_win:
                    self._return_win(key, view)
                raise NotFoundError(
                    "key %r was not found and update() got no default" % (key,)
                )
            else:
                current = default
            try:
                new_value = fn(current)
                raw, client_flags = dump_value(self._serializer, key, new_value)
            except BaseException:
                # Aborting leaves the entry unwritten; an accidentally
                # consumed stale-recache token must go back so the factory
                # election still works for the rest of the grace period.
                if stale_win:
                    self._return_win(key, view)
                raise
            if compare_cas is not None:
                write_op, stored = plan_store(
                    wire_key, key, raw, client_flags, ttl, compare_cas=compare_cas
                )
            else:
                write_op, stored = plan_store(
                    wire_key, key, raw, client_flags, ttl, mode="add"
                )
            if stored(self._run_one(write_op)):
                return new_value
        raise ConflictError(
            "update of key %r kept conflicting with concurrent writes" % (key,)
        )

    def pop(self, key: Key, default: Any = None) -> Any:
        """Atomically take a value and delete it; a miss returns ``default``.

        The read and the delete are fenced by the entry's version, so bytes
        appended in between are never lost: a conflicting write makes pop
        read again instead of deleting data it has not returned.
        """
        self._check_open()
        wire_key = self._wire_key(key)
        probe_op, probe = plan_probe(wire_key, key)
        for _ in range(UPDATE_ATTEMPTS):
            view = probe(self._run_one(probe_op))
            if not view.hit or not view.data:
                if view.stale and view.won:
                    self._return_win(key, view)
                return default
            if view.cas is None:
                error = OperationFailedError(key)
                error.__cause__ = ProtocolError("read omitted the requested version")
                raise error
            value = self._serializer.load(key, view.data, view.client_flags)
            erase_op, erased = plan_erase(wire_key, key, compare_cas=view.cas)
            if erased(self._run_one(erase_op)):
                return value
        raise ConflictError(
            "pop of key %r kept conflicting with concurrent writes" % (key,)
        )

    def append(self, key: Key, fragment: Any, ttl: Ttl) -> None:
        """Append bytes to a byte-stream value, creating it on a miss.

        ``ttl`` applies only when this call creates the key; later appends
        never extend the buffer's life, which makes it a rolling collection
        window. Fragments bypass the serializer; reading the buffer back
        (usually with :meth:`pop`) yields bytes the caller splits.
        """
        self._check_open()
        self._run_call(self._call_concat(key, fragment, ttl, "append"))

    def prepend(self, key: Key, fragment: Any, ttl: Ttl) -> None:
        """Prepend bytes to a byte-stream value; see :meth:`append`."""
        self._check_open()
        self._run_call(self._call_concat(key, fragment, ttl, "prepend"))

    # ------------------------------------------------------------------
    # counters (S7)

    def incr(self, key: Key, delta: int = 1, *, ttl: Ttl) -> int:
        """Atomically add ``delta``; a miss counts from zero.

        ``ttl`` applies only when this call creates the counter; later
        increments never extend its life, which is exactly fixed-window
        counting. Permanent counters state ``ttl=FOREVER`` explicitly.
        """
        self._check_open()
        return cast(int, self._run_call(self._call_counter(key, delta, False, ttl)))

    def decr(self, key: Key, delta: int = 1, *, ttl: Ttl) -> int:
        """Atomically subtract ``delta``, saturating at zero; see :meth:`incr`."""
        self._check_open()
        return cast(int, self._run_call(self._call_counter(key, delta, True, ttl)))

    # ------------------------------------------------------------------
    # conditional blind writes (S8/S9)

    def add(self, key: Key, value: Any, ttl: Ttl) -> bool:
        """Store only if the key does not exist; True when this call won."""
        self._check_open()
        return cast(bool, self._run_call(self._call_store_if(key, value, ttl, "add")))

    def replace(self, key: Key, value: Any, ttl: Ttl) -> bool:
        """Store only if the key still exists; never resurrects a deleted key."""
        self._check_open()
        return cast(
            bool, self._run_call(self._call_store_if(key, value, ttl, "replace"))
        )

    def touch(self, key: Key, ttl: Ttl) -> bool:
        """Extend a key's ttl without transferring its value.

        The extension is blind: it applies to whatever it hits, including
        entries kept stale by ``delete(grace=...)``, so a revocation that
        must stick uses :meth:`delete`.
        """
        self._check_open()
        return cast(bool, self._run_call(self._call_touch(key, ttl)))

    # ------------------------------------------------------------------
    # observation (S13) and batching (S12)

    def inspect(self, key: Key) -> ItemInfo | None:
        """Read an item's metadata without transferring the value or
        bumping its LRU position; None when the key is not cached.

        A diagnostic probe: branching business logic on metadata is
        inherently racy and not a supported pattern.
        """
        self._check_open()
        return cast(ItemInfo | None, self._run_call(self._call_inspect(key)))

    def pipeline(self) -> Pipeline:
        """Batch independent operations into one round trip per server."""
        self._check_open()
        return Pipeline(self)

    # ------------------------------------------------------------------
    # factory machinery (S3/S4/S5)

    def _claim_flight(self, merge_key: bytes) -> tuple[_Flight, bool]:
        with self._flights_lock:
            flight = self._flights.get(merge_key)
            if flight is not None:
                return flight, False
            flight = _Flight()
            self._flights[merge_key] = flight
            return flight, True

    def _finish_flight(
        self,
        merge_key: bytes,
        flight: _Flight,
        value: Any = None,
        error: BaseException | None = None,
    ) -> None:
        with self._flights_lock:
            if self._flights.get(merge_key) is flight:
                del self._flights[merge_key]
        if error is not None:
            flight.fail(error)
        else:
            flight.resolve(value)

    def _factory_get(
        self,
        key: Key,
        factory: Callable[[], Any],
        ttl: int,
        refresh_ahead: int | None,
    ) -> Any:
        op, election = plan_election(self._wire_key(key), key, refresh_ahead)
        for attempt in range(len(WAIT_BACKOFF) + 1):
            try:
                view = election(self._run_one(op))
            except (OperationFailedError, AmbiguousWriteError) as exc:
                if self._degrade:
                    # Cache outage is not a site outage: compute locally and
                    # skip the cache, keeping the failure observable.
                    self._report(exc)
                    return self._local_compute(key, factory)
                raise
            if view.hit and view.data:
                if view.won and view.cas is not None:
                    # Refresh-ahead or stale-grace win. The synchronous
                    # winner recomputes now and returns the fresh value;
                    # who pays how much latency stays predictable because
                    # this client owns no threads.
                    return self._lead(key, factory, ttl, view.cas, release=False)
                return self._serializer.load(key, view.data, view.client_flags)
            if view.hit and view.cas is not None and (view.won or not view.busy):
                # The vivified placeholder this call won, or a zero-byte
                # entry nobody else is rewriting: recompute and replace it
                # through its version.
                return self._lead(key, factory, ttl, view.cas, release=True)
            if not view.hit:
                # A miss despite vivify offers no coordination; compute
                # without write-back, merged with any same-process run.
                return self._local_compute(key, factory)
            # Another caller holds the lease. Same-process callers share its
            # pending result; cross-process losers wait briefly and re-read.
            with self._flights_lock:
                flight = self._flights.get(self._merge_key(key))
            if flight is not None:
                return flight.wait()
            if attempt >= len(WAIT_BACKOFF):
                return self._local_compute(key, factory)
            time.sleep(WAIT_BACKOFF[attempt])
        raise AssertionError("unreachable")

    def _lead(
        self,
        key: Key,
        factory: Callable[[], Any],
        ttl: int,
        cas: int,
        release: bool,
    ) -> Any:
        """Run the factory as the elected winner and publish the result.

        The factory's own exception propagates to every merged caller; only
        write-back failures are diverted to the failure hook. When nothing
        gets written back on the miss path, the placeholder that granted
        the lease is released so the next reader re-elects immediately.
        """
        merge_key = self._merge_key(key)
        flight, leader = self._claim_flight(merge_key)
        if not leader:
            return flight.wait()
        try:
            value = factory()
        except BaseException as exc:
            self._finish_flight(merge_key, flight, error=exc)
            if release:
                self._release_lease(key, cas)
            raise
        self._write_back(key, value, ttl, cas, release)
        self._finish_flight(merge_key, flight, value=value)
        return value

    def _local_compute(self, key: Key, factory: Callable[[], Any]) -> Any:
        """Run the factory without write-back, merged per process."""
        merge_key = self._merge_key(key)
        flight, leader = self._claim_flight(merge_key)
        if not leader:
            return flight.wait()
        try:
            value = factory()
        except BaseException as exc:
            self._finish_flight(merge_key, flight, error=exc)
            raise
        self._finish_flight(merge_key, flight, value=value)
        return value

    def _write_back(
        self, key: Key, value: Any, ttl: int, cas: int, release: bool
    ) -> None:
        """Store a recomputed value conditioned on the election's version.

        The gap between "factory started" and "write lands" may contain a
        delete, a set, or another invalidation; an unconditional write
        would resurrect dead data, so a rejected write is abandoned. No
        write-back failure changes what get() returns; they are
        observability events.
        """
        try:
            raw, client_flags = dump_value(self._serializer, key, value)
        except Exception as exc:
            self._report(exc)
            if release:
                self._release_lease(key, cas)
            return
        op, stored = plan_store(
            self._wire_key(key), key, raw, client_flags, ttl, compare_cas=cas
        )
        try:
            applied = stored(self._run_one(op))
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

    def _release_lease(self, key: Key, cas: int) -> None:
        """Delete the zero-byte placeholder whose lease went unpaid.

        The version guard means a concurrent rewrite wins and the delete
        quietly stands down; only transport failures are worth reporting.
        """
        op, erased = plan_erase(self._wire_key(key), key, compare_cas=cas)
        try:
            erased(self._run_one(op))
        except Exception as exc:
            self._report(exc)

    # ------------------------------------------------------------------
    # maintenance

    def flush_all(self, delay: int = 0) -> None:
        """Expire every item on every server; an operations tool, not a verb."""
        self._check_open()
        positive("delay", delay)
        failure: BaseException | None = None
        for server in self._servers:
            try:
                server.flush(delay, self._timeout)
            except Exception as exc:
                if failure is None:
                    failure = exc
        if failure is not None:
            raise failure


__all__ = [
    "Deferred",
    "FOREVER",
    "ItemInfo",
    "Memcache",
    "MetaNamespace",
    "Pipeline",
]
