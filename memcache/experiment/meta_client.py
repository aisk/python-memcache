from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union, cast

import hashring

from ..connection import Addr, Connection, Pool
from ..errors import AmbiguousWriteError, PipelineError, ProtocolError
from ..meta_command import MetaCommand, MetaResult
from ..serialize import DumpFunc, LoadFunc, dump, load
from ._meta_core import MetaProtocol, Prepared, key_bytes, positive
from .operation import ABSENT, PRESENT, Delete, Get, IfCas, Increment, Operation, Set
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


class _Server:
    def __init__(
        self,
        addr: Addr,
        username: Optional[str],
        password: Optional[str],
    ) -> None:
        self.addr = addr
        self._username = username
        self._password = password
        self._connection: Optional[Connection] = None
        self._lock = threading.Lock()
        self._closed = False

    def __repr__(self) -> str:
        return "%s:%d" % self.addr

    def _new_connection(self, timeout: Optional[float]) -> Connection:
        return Connection(
            self.addr,
            username=self._username,
            password=self._password,
            timeout=timeout,
        )

    def pipeline(
        self, commands: List[MetaCommand], timeout: Optional[float]
    ) -> List[MetaResult]:
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

    def execute(self, command: MetaCommand, timeout: Optional[float]) -> MetaResult:
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

    def flush(self, delay: int, timeout: Optional[float]) -> None:
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


class RawClient:
    def __init__(self, client: "MetaClient") -> None:
        self._client = client

    def execute(
        self,
        *,
        command: Union[str, bytes],
        key: Key,
        flags: Sequence[bytes] = (),
        value: Optional[bytes] = None,
        timeout: Optional[float] = None,
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
        self, commands: Sequence[MetaCommand], *, timeout: Optional[float] = None
    ) -> List[MetaResult]:
        if self._client._closed:
            raise RuntimeError("client is closed")
        grouped: Dict[_Server, List[Tuple[int, MetaCommand]]] = {}
        for index, command in enumerate(commands):
            if b"q" in command.flags:
                raise ValueError("raw batch does not accept quiet commands")
            server = self._client._server_for(command.key)
            grouped.setdefault(server, []).append((index, command))
        output: List[Optional[MetaResult]] = [None] * len(commands)

        def run_group(server: _Server, group: List[Tuple[int, MetaCommand]]) -> None:
            responses = server.pipeline(
                [command for _, command in group],
                self._client._timeout(timeout),
            )
            if len(responses) != len(group):
                raise ProtocolError("raw batch received an unexpected response count")
            for (index, _), response in zip(group, responses):
                output[index] = response

        self._client._run_parallel(grouped, run_group)
        if any(result is None for result in output):
            raise ProtocolError("raw batch left an operation unresolved")
        return cast(List[MetaResult], output)


class MetaClient(MetaProtocol):
    """Native synchronous meta protocol client."""

    def __init__(
        self,
        addr: Union[Addr, List[Addr], None] = None,
        *,
        pool_size: Optional[int] = 23,
        pool_timeout: Optional[int] = 1,
        timeout: Optional[float] = 1.0,
        load_func: LoadFunc = load,
        dump_func: DumpFunc = dump,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        super().__init__(load_func, dump_func)
        self.default_timeout = timeout
        addresses: List[Addr]
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
        self.raw = RawClient(self)
        self._closed = False

    def __enter__(self) -> "MetaClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _timeout(self, timeout: Optional[float]) -> Optional[float]:
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
        self, command: MetaCommand, *, timeout: Optional[float] = None
    ) -> MetaResult:
        if self._closed:
            raise RuntimeError("client is closed")
        return self._server_for(command.key).execute(command, self._timeout(timeout))

    def _lease_fulfill(self, key: Key, cas: Optional[int]) -> Any:
        def fulfill(value: Any, **options: Any) -> MutationResult:
            if cas is None:
                raise ProtocolError("lease response did not include CAS")
            return self.set(key, value, condition=IfCas(cas), **options)

        return fulfill

    @staticmethod
    def _run_parallel(groups: Dict[Any, Any], function: Any) -> None:
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
        prepared: List[Prepared],
        output: List[Optional[Result]],
        timeout: Optional[float],
    ) -> None:
        commands = [self._pipeline_command(item) for item in prepared]
        responses: List[MetaResult] = []
        written = 0
        failure: Optional[BaseException] = None
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
        timeout: Optional[float] = None,
    ) -> BatchResult:
        if self._closed:
            raise RuntimeError("client is closed")
        prepared = [self._prepare(index, op) for index, op in enumerate(operations)]
        grouped: Dict[_Server, List[Prepared]] = {}
        for item in prepared:
            grouped.setdefault(self._server_for(item.operation.key), []).append(item)
        output: List[Optional[Result]] = [None] * len(prepared)

        def run(server: _Server, group: List[Prepared]) -> None:
            self._run_group(server, group, output, self._timeout(timeout))

        self._run_parallel(grouped, run)
        if any(item is None for item in output):
            raise AssertionError("batch executor left an operation unresolved")
        return BatchResult(output)  # type: ignore[arg-type]

    def _one(self, operation: Operation, timeout: Optional[float]) -> Result:
        result = cast(Result, self.batch([operation], timeout=timeout)[0])
        if result.status in (GetStatus.AMBIGUOUS, MutationStatus.AMBIGUOUS):
            raise AmbiguousWriteError(result)
        return result

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
        return self._one(  # type: ignore[return-value]
            Get(key, meta, touch, no_lru_bump, unless_cas), timeout
        )

    def inspect(
        self,
        key: Key,
        *,
        meta: Meta = Meta.CAS | Meta.TTL | Meta.SIZE,
        no_lru_bump: bool = True,
        timeout: Optional[float] = None,
    ) -> GetResult[Any]:
        return self._one(  # type: ignore[return-value]
            Get(key, meta=meta, no_lru_bump=no_lru_bump, value=False), timeout
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
        return self._one(  # type: ignore[return-value]
            Get(
                key,
                meta=meta,
                lease_ttl=lease_ttl,
                refresh_before=refresh_before,
            ),
            timeout,
        )

    def get_many(
        self,
        keys: Sequence[Key],
        *,
        meta: Meta = Meta.NONE,
        timeout: Optional[float] = None,
    ) -> BatchResult:
        return self.batch([Get(key, meta=meta) for key in keys], timeout=timeout)

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
        return self._one(  # type: ignore[return-value]
            Set(key, value, ttl, condition, version, return_cas), timeout
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

    def append_bytes(
        self,
        key: Key,
        value: bytes,
        *,
        vivify_ttl: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        if not isinstance(value, bytes):
            raise TypeError("append_bytes requires bytes")
        return self._one(  # type: ignore[return-value]
            Set(key, value, mode="append", vivify_ttl=vivify_ttl), timeout
        )

    def prepend_bytes(
        self,
        key: Key,
        value: bytes,
        *,
        vivify_ttl: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        if not isinstance(value, bytes):
            raise TypeError("prepend_bytes requires bytes")
        return self._one(  # type: ignore[return-value]
            Set(key, value, mode="prepend", vivify_ttl=vivify_ttl), timeout
        )

    def delete(
        self,
        key: Key,
        *,
        condition: Optional[IfCas] = None,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        return self._one(Delete(key, condition), timeout)  # type: ignore[return-value]

    def invalidate(
        self,
        key: Key,
        *,
        stale_for: Optional[int] = None,
        condition: Optional[IfCas] = None,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        return self._one(  # type: ignore[return-value]
            Delete(key, condition, invalidate=True, stale_for=stale_for), timeout
        )

    def increment(
        self,
        key: Key,
        delta: int = 1,
        *,
        initial: Optional[int] = None,
        initial_ttl: Optional[int] = None,
        ttl: Optional[int] = None,
        condition: Optional[IfCas] = None,
        version: Optional[int] = None,
        return_cas: bool = False,
        timeout: Optional[float] = None,
    ) -> ArithmeticResult:
        return self._one(  # type: ignore[return-value]
            Increment(
                key,
                delta,
                initial,
                initial_ttl,
                ttl,
                False,
                condition,
                version,
                return_cas,
            ),
            timeout,
        )

    def decrement(self, key: Key, delta: int = 1, **options: Any) -> ArithmeticResult:
        timeout = options.pop("timeout", None) if "timeout" in options else None
        operation = Increment(key, delta=delta, decrement=True, **options)
        return self._one(operation, timeout)  # type: ignore[return-value]

    def touch(
        self, key: Key, ttl: int, *, timeout: Optional[float] = None
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
    "ABSENT",
    "PRESENT",
    "Delete",
    "Get",
    "IfCas",
    "Increment",
    "MetaClient",
    "Set",
]
