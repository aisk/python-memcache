from __future__ import annotations

import threading
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from time import monotonic
from typing import (
    Any,
    cast,
)
from collections.abc import Iterator, Sequence

import hashring

from ..connection import Addr, Connection, chunk_pipeline
from ..errors import PipelineError, ProtocolError
from ..meta_command import MetaCommand, MetaResult
from ..serialize import Serializer, StrictSerializer
from ._meta_core import (
    MetaProtocol,
    fill_meta_responses,
    finalize_batch,
    finalize_meta_batch,
    group_meta_commands,
    group_prepared,
    normalize_addresses,
    raise_on_failure,
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
    Field,
    GetResult,
    Key,
    LeaseResult,
    MutationResult,
    Result,
)


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
class _Outcome:
    """What one server's pipeline produced, successful or not."""

    responses: list[MetaResult]
    written: int
    barrier: bool
    error: BaseException | None
    confirmed: int = 0


class MetaNamespace:
    """Typed, flag-for-flag surface over the meta protocol commands.

    Methods map 1:1 to the wire commands (``mg``/``ms``/``md``/``ma``/``me``)
    with one keyword argument per protocol flag. Values are raw bytes and
    responses are lightly parsed :class:`MetaCommandResult` objects; no
    serialization or semantic interpretation happens here. The bytes-level
    ``execute``/``batch`` escape hatch remains for anything the typed
    surface does not cover.
    """

    def __init__(self, client: MetaClient) -> None:
        self._client = client

    def _run(self, command: MetaCommand, timeout: float | None) -> MetaCommandResult:
        return parse_meta_result(
            self._client.execute_meta_command(command, timeout=timeout)
        )

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
            key,
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
        return self._run(command, timeout)

    def debug(self, key: Key, *, timeout: float | None = None) -> dict[str, str] | None:
        return parse_debug_result(
            self._client.execute_meta_command(build_debug(key), timeout=timeout)
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
            key=key_bytes(key),
            datalen=len(value) if value is not None else None,
            flags=list(flags),
            value=value,
        )
        return self._client.execute_meta_command(meta, timeout=timeout)

    def batch(
        self, commands: Sequence[MetaCommand], *, timeout: float | None = None
    ) -> list[MetaResult]:
        if self._client._closed:
            raise RuntimeError("client is closed")
        grouped = group_meta_commands(commands, self._client._server_for)
        output: list[MetaResult | None] = [None] * len(commands)
        outcomes = self._client._run_pipelines(
            {
                server: [command for _, command in group]
                for server, group in grouped.items()
            },
            self._client._timeout(timeout),
        )
        for server, group in grouped.items():
            outcome = outcomes[server]
            if outcome.error is not None:
                raise outcome.error
            fill_meta_responses(output, group, outcome.responses)
        return finalize_meta_batch(output)


class MetaClient(MetaProtocol):
    """Native synchronous meta protocol client."""

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
        self.meta = MetaNamespace(self)
        self._closed = False

    def __enter__(self) -> MetaClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _timeout(self, timeout: float | None) -> float | None:
        return self.default_timeout if timeout is None else timeout

    def _server_for(self, key: Key) -> _Server:
        return cast(_Server, self._ring.get_node(routing_key(key)))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for server in self._servers:
            server.close()

    def execute_meta_command(
        self, command: MetaCommand, *, timeout: float | None = None
    ) -> MetaResult:
        if self._closed:
            raise RuntimeError("client is closed")
        return self._server_for(command.key).execute(command, self._timeout(timeout))

    def _lease_fulfill(self, key: Key, cas: int | None) -> Any:
        def fulfill(value: Any, **options: Any) -> MutationResult:
            if cas is None:
                raise ProtocolError("lease response did not include CAS")
            return self.set(key, value, compare_cas=cas, **options)

        return fulfill

    @staticmethod
    def _failed_outcome(error: BaseException) -> _Outcome:
        if isinstance(error, PipelineError):
            return _Outcome(
                error.responses, error.written, False, error, error.confirmed
            )
        return _Outcome([], 0, False, error)

    def _run_pipelines(
        self,
        grouped: dict[_Server, list[MetaCommand]],
        timeout: float | None,
    ) -> dict[_Server, _Outcome]:
        """Write every server's pipeline, then read them back one by one.

        The servers process their pipelines concurrently while this thread
        drains responses sequentially, so the fan-out needs no worker
        threads and its latency still tracks the slowest server. One server
        failing never disturbs another; ``timeout`` bounds the whole batch
        as a single deadline rather than applying per socket operation.
        """
        deadline = _deadline(timeout)
        outcomes: dict[_Server, _Outcome] = {}
        pending: list[tuple[_Server, _InFlight]] = []
        for server, commands in grouped.items():
            try:
                pending.append((server, server.start_pipeline(commands, deadline)))
            except Exception as exc:
                outcomes[server] = self._failed_outcome(exc)
        for server, flight in pending:
            try:
                responses = flight.finish(deadline)
            except Exception as exc:
                outcomes[server] = self._failed_outcome(exc)
            else:
                outcomes[server] = _Outcome(responses, flight.count, True, None)
        return outcomes

    def batch(
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
        outcomes = self._run_pipelines(
            {
                server: [self._pipeline_command(item) for item in group]
                for server, group in grouped.items()
            },
            self._timeout(timeout),
        )
        for server, group in grouped.items():
            outcome = outcomes[server]
            error = outcome.error
            cause = error.cause if isinstance(error, PipelineError) else error
            self._resolve_group(
                group,
                output,
                outcome.responses,
                outcome.written,
                outcome.barrier,
                cause,
                outcome.confirmed,
            )
        return finalize_batch(output)

    def _one(self, operation: Operation, timeout: float | None) -> Result:
        return raise_on_failure(
            cast(Result, self.batch([operation], timeout=timeout)[0])
        )

    def get(
        self,
        key: Key,
        *,
        value: bool = True,
        fields: Field = Field.NONE,
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
        return self._one(  # type: ignore[return-value]
            Get(
                key,
                fields=fields,
                touch=touch,
                no_lru_bump=no_lru_bump,
                unless_cas=unless_cas,
                value=value,
                lease_ttl=lease_ttl,
                refresh_before=refresh_before,
            ),
            timeout,
        )

    def inspect(
        self,
        key: Key,
        *,
        fields: Field = Field.CAS | Field.TTL | Field.SIZE,
        no_lru_bump: bool = True,
        timeout: float | None = None,
    ) -> GetResult[Any]:
        return self._one(  # type: ignore[return-value]
            Get(key, fields=fields, no_lru_bump=no_lru_bump, value=False), timeout
        )

    def get_with_lease(
        self,
        key: Key,
        *,
        lease_ttl: int,
        refresh_before: int | None = None,
        fields: Field = Field.NONE,
        timeout: float | None = None,
    ) -> LeaseResult[Any]:
        return self._one(  # type: ignore[return-value]
            Get(
                key,
                fields=fields,
                lease_ttl=lease_ttl,
                refresh_before=refresh_before,
            ),
            timeout,
        )

    def set(
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
        return self._one(  # type: ignore[return-value]
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

    def add(
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
        return self.set(
            key,
            value,
            ttl=ttl,
            mode="add",
            version=version,
            return_cas=return_cas,
            timeout=timeout,
        )

    def cas(
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
        return self.set(
            key,
            value,
            ttl=ttl,
            compare_cas=cas_token,
            version=version,
            return_cas=return_cas,
            timeout=timeout,
        )

    def append(
        self,
        key: Key,
        value: bytes,
        *,
        vivify_ttl: int | None = None,
        timeout: float | None = None,
    ) -> MutationResult:
        """Append bytes to an existing value; serialization is skipped."""
        return self.set(
            key, value, mode="append", vivify_ttl=vivify_ttl, timeout=timeout
        )

    def prepend(
        self,
        key: Key,
        value: bytes,
        *,
        vivify_ttl: int | None = None,
        timeout: float | None = None,
    ) -> MutationResult:
        """Prepend bytes to an existing value; serialization is skipped."""
        return self.set(
            key, value, mode="prepend", vivify_ttl=vivify_ttl, timeout=timeout
        )

    def delete(
        self,
        key: Key,
        *,
        compare_cas: int | None = None,
        timeout: float | None = None,
    ) -> MutationResult:
        return self._one(  # type: ignore[return-value]
            Delete(key, compare_cas=compare_cas), timeout
        )

    def invalidate(
        self,
        key: Key,
        *,
        stale_for: int | None = None,
        compare_cas: int | None = None,
        timeout: float | None = None,
    ) -> MutationResult:
        return self._one(  # type: ignore[return-value]
            Delete(key, compare_cas=compare_cas, invalidate=True, stale_for=stale_for),
            timeout,
        )

    def increment(
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
        return self._one(  # type: ignore[return-value]
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

    def decrement(
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
        return self._one(  # type: ignore[return-value]
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

    def touch(
        self, key: Key, ttl: int, *, timeout: float | None = None
    ) -> MutationResult:
        return touch_result(key, self._one(Get(key, touch=ttl, value=False), timeout))

    def flush_all(self, delay: int = 0) -> None:
        if self._closed:
            raise RuntimeError("client is closed")
        positive("delay", delay)
        failure: BaseException | None = None
        for server in self._servers:
            try:
                server.flush(delay, self.default_timeout)
            except Exception as exc:
                if failure is None:
                    failure = exc
        if failure is not None:
            raise failure


__all__ = [
    "Arithmetic",
    "Delete",
    "Get",
    "MetaClient",
    "Set",
]
