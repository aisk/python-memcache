from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import (
    Any,
    TypeVar,
    cast,
)
from collections.abc import Callable, Iterator, Sequence

import hashring

from ..connection import Addr, Connection, Pool
from ..errors import AmbiguousWriteError, PipelineError, ProtocolError
from ..meta_command import MetaCommand, MetaResult
from ..serialize import Serializer, StrictSerializer
from ._meta_core import MetaProtocol, Prepared
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
    GetStatus,
    Key,
    LeaseResult,
    Meta,
    MutationResult,
    MutationStatus,
    Result,
)


ServerT = TypeVar("ServerT")
GroupT = TypeVar("GroupT")


class _Server:
    def __init__(
        self,
        addr: Addr,
        username: str | None,
        password: str | None,
    ) -> None:
        self.addr = addr
        self._username = username
        self._password = password
        self._connection: Connection | None = None
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

    def pipeline(
        self, commands: list[MetaCommand], timeout: float | None
    ) -> list[MetaResult]:
        with self._lock:
            if self._closed:
                raise RuntimeError("client is closed")
            if self._connection is None:
                self._connection = self._new_connection(timeout)
            try:
                return self._connection.execute_pipeline(commands, timeout)
            except BaseException:
                connection, self._connection = self._connection, None
                try:
                    connection.close()
                except BaseException:
                    pass
                raise

    def execute(self, command: MetaCommand, timeout: float | None) -> MetaResult:
        with self._lock:
            if self._closed:
                raise RuntimeError("client is closed")
            if self._connection is None:
                self._connection = self._new_connection(timeout)
            try:
                return self._connection.execute_meta_command(command, timeout)
            except BaseException:
                connection, self._connection = self._connection, None
                try:
                    connection.close()
                except BaseException:
                    pass
                raise

    def flush(self, delay: int, timeout: float | None) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("client is closed")
            if self._connection is None:
                self._connection = self._new_connection(timeout)
            try:
                self._connection.flush_all(delay, timeout)
            except BaseException:
                connection, self._connection = self._connection, None
                try:
                    connection.close()
                except BaseException:
                    pass
                raise

    def close(self) -> None:
        with self._lock:
            self._closed = True
            connection, self._connection = self._connection, None
            if connection is not None:
                connection.close()


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
        grouped: dict[_Server, list[tuple[int, MetaCommand]]] = {}
        for index, command in enumerate(commands):
            if b"q" in command.flags:
                raise ValueError("meta batch does not accept quiet commands")
            server = self._client._server_for(command.key)
            grouped.setdefault(server, []).append((index, command))
        output: list[MetaResult | None] = [None] * len(commands)

        def run_group(server: _Server, group: list[tuple[int, MetaCommand]]) -> None:
            responses = server.pipeline(
                [command for _, command in group],
                self._client._timeout(timeout),
            )
            if len(responses) != len(group):
                raise ProtocolError("meta batch received an unexpected response count")
            for (index, _), response in zip(group, responses):
                output[index] = response

        self._client._run_parallel(grouped, run_group)
        if any(result is None for result in output):
            raise ProtocolError("meta batch left an operation unresolved")
        return cast(list[MetaResult], output)


class MetaClient(MetaProtocol):
    """Native synchronous meta protocol client."""

    def __init__(
        self,
        addr: Addr | list[Addr] | None = None,
        *,
        pool_size: int | None = 23,
        pool_timeout: int | None = 1,
        timeout: float | None = 1.0,
        serializer: Serializer | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        super().__init__(serializer if serializer is not None else StrictSerializer())
        self.default_timeout = timeout
        addresses: list[Addr]
        if addr is None:
            addresses = [("localhost", 11211)]
        elif isinstance(addr, tuple) and len(addr) == 2:
            addresses = [addr]
        elif isinstance(addr, list) and addr:
            addresses = addr
        else:
            raise TypeError("addr must be a server tuple or a non-empty list")
        self._servers = [
            _Server(server, username=username, password=password)
            for server in addresses
        ]
        self._ring = hashring.HashRing(self._servers)
        compat_pools = []
        for server in addresses:

            def make(server: Addr = server) -> Connection:
                return Connection(server, username=username, password=password)

            compat_pools.append(Pool(make, max_size=pool_size, timeout=pool_timeout))
        self._compat_ring = hashring.HashRing(compat_pools)
        self.meta = MetaNamespace(self)
        self._closed = False

    def __enter__(self) -> MetaClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _timeout(self, timeout: float | None) -> float | None:
        return self.default_timeout if timeout is None else timeout

    def _server_for(self, key: Key) -> _Server:
        routing_key = key if isinstance(key, str) else key.decode("latin-1")
        return cast(_Server, self._ring.get_node(routing_key))

    @contextmanager
    def _get_connection(self, key: Key) -> Iterator[Connection]:
        routing_key = key if isinstance(key, str) else key.decode("latin-1")
        pool = self._compat_ring.get_node(routing_key)
        with pool.get() as connection:
            yield connection

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for server in self._servers:
            server.close()
        for pool in self._compat_ring.nodes:
            pool.close()

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
    def _run_parallel(
        groups: dict[ServerT, GroupT],
        function: Callable[[ServerT, GroupT], None],
    ) -> None:
        items = list(groups.items())
        if len(items) <= 1:
            for server, group in items:
                function(server, group)
            return
        with ThreadPoolExecutor(max_workers=len(items)) as executor:
            futures = [
                executor.submit(function, server, group) for server, group in items
            ]
            for future in futures:
                future.result()

    def _run_group(
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
            responses = server.pipeline(commands, timeout)
            written = len(prepared)
            barrier = True
        except PipelineError as exc:
            responses = exc.responses
            written = exc.written
            failure = exc.cause
        except BaseException as exc:
            failure = exc
        self._resolve_group(prepared, output, responses, written, barrier, failure)

    def batch(
        self,
        operations: Sequence[Operation],
        *,
        timeout: float | None = None,
    ) -> BatchResult:
        if self._closed:
            raise RuntimeError("client is closed")
        prepared = [self._prepare(index, op) for index, op in enumerate(operations)]
        grouped: dict[_Server, list[Prepared]] = {}
        for item in prepared:
            grouped.setdefault(self._server_for(item.operation.key), []).append(item)
        output: list[Result | None] = [None] * len(prepared)

        def run(server: _Server, group: list[Prepared]) -> None:
            self._run_group(server, group, output, self._timeout(timeout))

        self._run_parallel(grouped, run)
        if any(item is None for item in output):
            raise AssertionError("batch executor left an operation unresolved")
        return BatchResult(output)  # type: ignore[arg-type]

    def _one(self, operation: Operation, timeout: float | None) -> Result:
        result = cast(Result, self.batch([operation], timeout=timeout)[0])
        if result.status in (GetStatus.AMBIGUOUS, MutationStatus.AMBIGUOUS):
            raise AmbiguousWriteError(result)
        return result

    def get(
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
        return self._one(  # type: ignore[return-value]
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

    def inspect(
        self,
        key: Key,
        *,
        meta: Meta = Meta.CAS | Meta.TTL | Meta.SIZE,
        no_lru_bump: bool = True,
        timeout: float | None = None,
    ) -> GetResult[Any]:
        return self._one(  # type: ignore[return-value]
            Get(key, meta=meta, no_lru_bump=no_lru_bump, value=False), timeout
        )

    def get_with_lease(
        self,
        key: Key,
        *,
        lease_ttl: int,
        refresh_before: int | None = None,
        meta: Meta = Meta.NONE,
        timeout: float | None = None,
    ) -> LeaseResult[Any]:
        return self._one(  # type: ignore[return-value]
            Get(
                key,
                meta=meta,
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
        result = self._one(Get(key, touch=ttl, value=False), timeout)
        if isinstance(result, GetResult):
            status = (
                MutationStatus.STORED
                if result.status is GetStatus.HIT
                else (
                    MutationStatus.NOT_FOUND
                    if result.status is GetStatus.MISS
                    else MutationStatus.FAILED
                )
            )
            return MutationResult(key, status, error=result.error)
        raise AssertionError("unexpected touch result")

    def flush_all(self, delay: int = 0) -> None:
        if self._closed:
            raise RuntimeError("client is closed")
        positive("delay", delay)
        groups = {server: None for server in self._servers}

        def flush(server: _Server, unused: None) -> None:
            server.flush(delay, self.default_timeout)

        self._run_parallel(groups, flush)


__all__ = [
    "Arithmetic",
    "Delete",
    "Get",
    "MetaClient",
    "Set",
]
