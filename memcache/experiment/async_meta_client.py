from __future__ import annotations

from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import anyio
import hashring

from ..async_connection import AsyncConnection, AsyncPool, PipelineError
from ..connection import Addr
from ..errors import AmbiguousWriteError, MemcacheError, ProtocolError
from ..meta_command import MetaCommand, MetaResult
from ..serialize import DumpFunc, LoadFunc, dump, load
from .operation import (
    ABSENT,
    PRESENT,
    Delete,
    Get,
    IfCas,
    Increment,
    Operation,
    Set,
)
from .result import (
    ArithmeticResult,
    BatchResult,
    GetResult,
    GetStatus,
    ItemMeta,
    Key,
    LeaseResult,
    LeaseState,
    Meta,
    MutationResult,
    MutationStatus,
    Result,
    ValueState,
)


def _key_bytes(key: Key) -> bytes:
    if isinstance(key, str):
        return key.encode("utf-8")
    if isinstance(key, bytes):
        return key
    raise TypeError("key must be str or bytes")


def _positive(name: str, value: Optional[int], *, allow_zero: bool = True) -> None:
    if value is None:
        return
    minimum = 0 if allow_zero else 1
    if not isinstance(value, int) or value < minimum:
        raise ValueError("%s must be an integer >= %d" % (name, minimum))


def _response_flags(flags: Iterable[bytes]) -> Dict[str, Any]:
    parsed: Dict[str, Any] = {}
    for flag in flags:
        if not flag:
            continue
        code = chr(flag[0])
        token = flag[1:]
        if code in ("f", "c", "t", "l", "s"):
            names = {
                "f": "client_flags",
                "c": "cas",
                "t": "ttl",
                "l": "last_access",
                "s": "size",
            }
            parsed[names[code]] = int(token)
        elif code == "h":
            parsed["hit_before"] = token != b"0"
        elif code == "O":
            parsed["opaque"] = token
        elif code == "W":
            parsed["won"] = True
        elif code == "Z":
            parsed["busy"] = True
        elif code == "X":
            parsed["stale"] = True
    return parsed


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
        self._connection: Optional[AsyncConnection] = None
        self._lock = anyio.Lock()

    def __repr__(self) -> str:
        return "%s:%d" % self.addr

    async def pipeline(
        self, commands: List[MetaCommand], timeout: Optional[float]
    ) -> List[MetaResult]:
        async with self._lock:
            if self._connection is None:
                self._connection = AsyncConnection(
                    self.addr, username=self._username, password=self._password
                )
            try:
                if timeout is None:
                    return await self._connection.execute_pipeline(commands)
                with anyio.fail_after(timeout):
                    return await self._connection.execute_pipeline(commands)
            except BaseException:
                connection, self._connection = self._connection, None
                if connection is not None:
                    try:
                        await connection.close()
                    except BaseException:
                        pass
                raise

    async def execute(
        self, command: MetaCommand, timeout: Optional[float]
    ) -> MetaResult:
        async with self._lock:
            if self._connection is None:
                self._connection = AsyncConnection(
                    self.addr, username=self._username, password=self._password
                )
            try:
                if timeout is None:
                    return await self._connection.execute_meta_command(command)
                with anyio.fail_after(timeout):
                    return await self._connection.execute_meta_command(command)
            except BaseException:
                connection, self._connection = self._connection, None
                if connection is not None:
                    try:
                        await connection.close()
                    except BaseException:
                        pass
                raise

    async def flush(self, delay: int) -> None:
        async with self._lock:
            if self._connection is None:
                self._connection = AsyncConnection(
                    self.addr, username=self._username, password=self._password
                )
            try:
                await self._connection.flush_all(delay)
            except BaseException:
                connection, self._connection = self._connection, None
                if connection is not None:
                    try:
                        await connection.close()
                    except BaseException:
                        pass
                raise

    async def close(self) -> None:
        async with self._lock:
            connection, self._connection = self._connection, None
            if connection is not None:
                await connection.close()


@dataclass
class _Prepared:
    index: int
    operation: Operation
    command: MetaCommand
    side_effect: bool


class AsyncRawClient:
    def __init__(self, client: "AsyncMetaClient") -> None:
        self._client = client

    async def execute(
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
            key=_key_bytes(key),
            datalen=len(value) if value is not None else None,
            flags=list(flags),
            value=value,
        )
        return await self._client.execute_meta_command(meta, timeout=timeout)

    async def batch(
        self,
        commands: Sequence[MetaCommand],
        *,
        timeout: Optional[float] = None,
    ) -> List[MetaResult]:
        # Raw batch does not infer quiet outcomes; require one response per
        # command so output can retain input order across server shards.
        grouped: Dict[_Server, List[Tuple[int, MetaCommand]]] = {}
        for index, command in enumerate(commands):
            if b"q" in command.flags:
                raise ValueError("raw batch does not accept quiet commands")
            server = self._client._server_for(command.key)
            grouped.setdefault(server, []).append((index, command))
        output: List[Optional[MetaResult]] = [None] * len(commands)
        async with anyio.create_task_group() as tasks:
            for server, group in grouped.items():
                tasks.start_soon(
                    self._run_group,
                    server,
                    group,
                    output,
                    self._client._timeout(timeout),
                )
        if any(result is None for result in output):
            raise ProtocolError("raw batch left an operation unresolved")
        return cast(List[MetaResult], output)

    @staticmethod
    async def _run_group(
        server: _Server,
        group: List[Tuple[int, MetaCommand]],
        output: List[Optional[MetaResult]],
        timeout: Optional[float],
    ) -> None:
        responses = await server.pipeline([command for _, command in group], timeout)
        if len(responses) != len(group):
            raise ProtocolError("raw batch received an unexpected response count")
        for (index, _), response in zip(group, responses):
            output[index] = response


class AsyncMetaClient:
    """Intent-oriented meta protocol client with a batch-first executor."""

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
        self._load = load_func
        self._dump = dump_func
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

            def make(server: Addr = server) -> AsyncConnection:
                return AsyncConnection(server, username=username, password=password)

            compat_pools.append(
                AsyncPool(make, max_size=pool_size, timeout=pool_timeout)
            )
        self._compat_ring = hashring.HashRing(compat_pools)
        self.raw = AsyncRawClient(self)
        self._closed = False

    async def __aenter__(self) -> "AsyncMetaClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def _timeout(self, timeout: Optional[float]) -> Optional[float]:
        return self.default_timeout if timeout is None else timeout

    def _server_for(self, key: Key) -> _Server:
        routing_key = key if isinstance(key, str) else key.decode("latin-1")
        return cast(_Server, self._ring.get_node(routing_key))

    @asynccontextmanager
    async def _get_connection(self, key: Key) -> AsyncIterator[AsyncConnection]:
        """Backward-compatible private pool hook used by AsyncMemcache."""
        routing_key = key if isinstance(key, str) else key.decode("latin-1")
        pool = self._compat_ring.get_node(routing_key)
        async with pool.get() as connection:
            yield connection

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for server in self._servers:
            await server.close()
        for pool in self._compat_ring.nodes:
            await pool.close()

    async def execute_meta_command(
        self, command: MetaCommand, *, timeout: Optional[float] = None
    ) -> MetaResult:
        if self._closed:
            raise RuntimeError("client is closed")
        return await self._server_for(command.key).execute(
            command, self._timeout(timeout)
        )

    def _prepare(self, index: int, operation: Operation) -> _Prepared:
        key = _key_bytes(operation.key)
        if isinstance(operation, Get):
            return self._prepare_get(index, operation, key)
        if isinstance(operation, Set):
            return self._prepare_set(index, operation, key)
        if isinstance(operation, Delete):
            return self._prepare_delete(index, operation, key)
        if isinstance(operation, Increment):
            return self._prepare_increment(index, operation, key)
        raise TypeError("unsupported batch operation")

    def _prepare_get(self, index: int, operation: Get, key: bytes) -> _Prepared:
        _positive("touch", operation.touch)
        _positive("lease_ttl", operation.lease_ttl, allow_zero=False)
        _positive("refresh_before", operation.refresh_before, allow_zero=False)
        if operation.refresh_before is not None and operation.lease_ttl is None:
            raise ValueError("refresh_before requires lease_ttl")
        if operation.unless_cas is not None and not operation.value:
            raise ValueError("unless_cas requires a value read")
        _positive("unless_cas", operation.unless_cas)
        flags = [b"f"]
        if operation.value:
            flags.append(b"v")
        requested_meta = operation.meta
        if operation.lease_ttl is not None or operation.unless_cas is not None:
            requested_meta |= Meta.CAS
        mapping = (
            (Meta.CAS, b"c"),
            (Meta.TTL, b"t"),
            (Meta.SIZE, b"s"),
            (Meta.LAST_ACCESS, b"l"),
            (Meta.HIT_BEFORE, b"h"),
        )
        flags.extend(wire for bit, wire in mapping if requested_meta & bit)
        optional_flags = (
            (operation.touch, b"T"),
            (operation.unless_cas, b"C"),
            (operation.lease_ttl, b"N"),
            (operation.refresh_before, b"R"),
        )
        for value, prefix in optional_flags:
            if value is not None:
                flags.append(prefix + str(value).encode("ascii"))
        if operation.no_lru_bump:
            flags.append(b"u")
        command = MetaCommand(b"mg", key, flags=flags)
        side_effect = any(
            value is not None
            for value in (
                operation.touch,
                operation.lease_ttl,
                operation.refresh_before,
            )
        )
        return _Prepared(index, operation, command, side_effect)

    def _prepare_set(self, index: int, operation: Set, key: bytes) -> _Prepared:
        _positive("ttl", operation.ttl)
        _positive("version", operation.version)
        raw, client_flags = self._dump(key, operation.value)
        flags = [b"F%d" % client_flags]
        if operation.ttl is not None:
            flags.append(b"T%d" % operation.ttl)
        if operation.version is not None:
            flags.append(b"E%d" % operation.version)
        if operation.return_cas:
            flags.append(b"c")
        flags.extend(self._condition_flags(operation))
        flags.extend(self._store_mode_flags(operation))
        if operation.vivify_ttl is not None:
            if operation.mode not in ("append", "prepend"):
                raise ValueError("vivify_ttl is only valid for byte concatenation")
            _positive("vivify_ttl", operation.vivify_ttl, allow_zero=False)
            flags.append(b"N%d" % operation.vivify_ttl)
        command = MetaCommand(b"ms", key, len(raw), flags, raw)
        return _Prepared(index, operation, command, True)

    @staticmethod
    def _condition_flags(operation: Set) -> List[bytes]:
        condition = operation.condition
        if condition is ABSENT:
            return [b"ME"]
        if condition is PRESENT:
            return [b"MR"]
        if isinstance(condition, IfCas):
            return [b"C%d" % condition.token]
        if condition is not None:
            raise TypeError("invalid store condition")
        return []

    @staticmethod
    def _store_mode_flags(operation: Set) -> List[bytes]:
        modes = {"set": None, "append": b"MA", "prepend": b"MP"}
        if operation.mode not in modes:
            raise ValueError("invalid store mode")
        mode_flag = modes[operation.mode]
        return [mode_flag] if mode_flag is not None else []

    def _prepare_delete(self, index: int, operation: Delete, key: bytes) -> _Prepared:
        _positive("stale_for", operation.stale_for)
        if operation.stale_for is not None and not operation.invalidate:
            raise ValueError("stale_for is only valid for invalidate")
        flags = []
        if operation.condition is not None:
            flags.append(b"C%d" % operation.condition.token)
        if operation.invalidate:
            flags.append(b"I")
            if operation.stale_for is not None:
                flags.append(b"T%d" % operation.stale_for)
        return _Prepared(index, operation, MetaCommand(b"md", key, flags=flags), True)

    def _prepare_increment(
        self, index: int, operation: Increment, key: bytes
    ) -> _Prepared:
        _positive("delta", operation.delta)
        _positive("initial", operation.initial)
        _positive("initial_ttl", operation.initial_ttl, allow_zero=False)
        _positive("ttl", operation.ttl)
        _positive("version", operation.version)
        if operation.initial is not None and operation.initial_ttl is None:
            raise ValueError("initial requires initial_ttl")
        if operation.initial_ttl is not None and operation.initial is None:
            raise ValueError("initial_ttl requires initial")
        flags = [b"D%d" % operation.delta, b"v"]
        if operation.decrement:
            flags.append(b"MD")
        if operation.initial is not None:
            assert operation.initial_ttl is not None
            flags.extend([b"J%d" % operation.initial, b"N%d" % operation.initial_ttl])
        if operation.ttl is not None:
            flags.append(b"T%d" % operation.ttl)
        if operation.condition is not None:
            flags.append(b"C%d" % operation.condition.token)
        if operation.version is not None:
            flags.append(b"E%d" % operation.version)
        if operation.return_cas:
            flags.append(b"c")
        return _Prepared(index, operation, MetaCommand(b"ma", key, flags=flags), True)

    def _failure(
        self, prepared: _Prepared, ambiguous: bool, error: BaseException
    ) -> Result:
        mutation_status = (
            MutationStatus.AMBIGUOUS if ambiguous else MutationStatus.FAILED
        )
        operation = prepared.operation
        if isinstance(operation, Get):
            status = GetStatus.AMBIGUOUS if ambiguous else GetStatus.FAILED
            return GetResult(key=operation.key, status=status, error=error)
        if isinstance(operation, Increment):
            return ArithmeticResult(operation.key, mutation_status, error=error)
        return MutationResult(operation.key, mutation_status, error=error)

    def _parse(self, prepared: _Prepared, response: Optional[MetaResult]) -> Result:
        operation = prepared.operation
        if response is None:
            if isinstance(operation, Get):
                return GetResult(key=operation.key, status=GetStatus.MISS)
            if isinstance(operation, Increment):
                return ArithmeticResult(
                    operation.key,
                    MutationStatus.FAILED,
                    error=ProtocolError("arithmetic response was suppressed"),
                )
            return MutationResult(operation.key, MutationStatus.STORED)
        parsed = _response_flags(response.flags)
        if isinstance(operation, Get):
            return self._parse_get(operation, response, parsed)
        if isinstance(operation, Increment):
            arithmetic_status = self._mutation_status(operation, response.rc)
            value = (
                int(response.value) if response.rc == b"VA" and response.value else None
            )
            return ArithmeticResult(
                operation.key,
                arithmetic_status,
                value=value,
                item=ItemMeta(cas=parsed.get("cas"), ttl=parsed.get("ttl")),
            )
        return MutationResult(
            operation.key,
            self._mutation_status(operation, response.rc),
            cas=parsed.get("cas"),
        )

    def _parse_get(
        self, operation: Get, response: MetaResult, parsed: Dict[str, Any]
    ) -> GetResult[Any]:
        if response.rc == b"EN":
            return GetResult(key=operation.key, status=GetStatus.MISS)
        if response.rc not in (b"VA", b"HD"):
            return GetResult(
                key=operation.key,
                status=GetStatus.FAILED,
                error=ProtocolError("unexpected get response %r" % response.rc),
            )
        item = ItemMeta(
            cas=parsed.get("cas"),
            ttl=parsed.get("ttl"),
            size=parsed.get("size"),
            last_access=parsed.get("last_access"),
            hit_before=parsed.get("hit_before"),
        )
        lease_state = (
            LeaseState.GRANTED
            if parsed.get("won")
            else LeaseState.BUSY if parsed.get("busy") else LeaseState.NONE
        )
        stale = bool(parsed.get("stale"))
        placeholder = (
            not stale and response.datalen == 0 and lease_state is not LeaseState.NONE
        )
        value_state = (
            ValueState.STALE
            if stale
            else ValueState.MISSING if placeholder else ValueState.FRESH
        )
        has_value = False
        value: Any = None
        if placeholder:
            status = (
                GetStatus.MISS if operation.lease_ttl is not None else GetStatus.PENDING
            )
        elif response.rc == b"HD" and operation.unless_cas is not None:
            status = GetStatus.UNCHANGED
        else:
            status = GetStatus.HIT
            has_value = response.rc == b"VA" and response.value is not None
            if has_value:
                value = self._load(
                    _key_bytes(operation.key),
                    response.value or b"",
                    parsed.get("client_flags", 0),
                )
        kwargs: Dict[str, Any] = dict(
            key=operation.key,
            status=status,
            item=item,
            value_state=value_state,
            lease_state=lease_state,
        )
        if has_value:
            kwargs["value"] = value
        if operation.lease_ttl is None:
            return GetResult(**kwargs)
        cas = item.cas

        async def fulfill(value: Any, **options: Any) -> MutationResult:
            if cas is None:
                raise ProtocolError("lease response did not include CAS")
            return await self.set(
                operation.key,
                value,
                condition=IfCas(cas),
                **options,
            )

        return LeaseResult(fulfill=fulfill, **kwargs)

    @staticmethod
    def _mutation_status(operation: Operation, rc: bytes) -> MutationStatus:
        if rc in (b"HD", b"VA"):
            return MutationStatus.STORED
        if rc == b"EX":
            return MutationStatus.CAS_MISMATCH
        if rc == b"NF":
            return MutationStatus.NOT_FOUND
        if rc == b"NS":
            if isinstance(operation, Set) and operation.condition is ABSENT:
                return MutationStatus.ALREADY_EXISTS
            return MutationStatus.NOT_FOUND
        return MutationStatus.FAILED

    @staticmethod
    def _pipeline_command(item: _Prepared) -> MetaCommand:
        flags = item.command.flags + [b"O%d" % item.index]
        # q suppresses the entire success line, including requested result
        # data. Arithmetic values and returned store CAS tokens need that line.
        needs_success_response = isinstance(item.operation, Increment) or (
            isinstance(item.operation, Set) and item.operation.return_cas
        )
        if not needs_success_response:
            flags.append(b"q")
        return MetaCommand(
            item.command.cm,
            item.command.key,
            item.command.datalen,
            flags,
            item.command.value,
        )

    @staticmethod
    def _index_responses(
        responses: Sequence[MetaResult],
    ) -> Tuple[Dict[int, MetaResult], Optional[BaseException]]:
        by_index: Dict[int, MetaResult] = {}
        failure: Optional[BaseException] = None
        for response in responses:
            opaque = _response_flags(response.flags).get("opaque")
            if opaque is None:
                failure = ProtocolError("pipeline response omitted opaque token")
                continue
            try:
                by_index[int(opaque)] = response
            except ValueError:
                failure = ProtocolError("invalid opaque token")
        return by_index, failure

    def _record_parsed(
        self,
        output: List[Optional[Result]],
        item: _Prepared,
        response: Optional[MetaResult],
    ) -> None:
        try:
            output[item.index] = self._parse(item, response)
        except BaseException as exc:
            output[item.index] = self._failure(item, False, exc)

    async def _run_group(
        self,
        server: _Server,
        prepared: List[_Prepared],
        output: List[Optional[Result]],
        timeout: Optional[float],
    ) -> None:
        commands = [self._pipeline_command(item) for item in prepared]
        responses: List[MetaResult] = []
        written = 0
        failure: Optional[BaseException] = None
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
        by_index, index_failure = self._index_responses(responses)
        if index_failure is not None:
            failure = index_failure
        for position, item in enumerate(prepared):
            candidate = by_index.get(item.index)
            if candidate is not None:
                self._record_parsed(output, item, candidate)
            elif barrier:
                self._record_parsed(output, item, None)
            else:
                error = failure or MemcacheError("pipeline did not reach barrier")
                output[item.index] = self._failure(
                    item,
                    ambiguous=position < written and item.side_effect,
                    error=error,
                )

    async def batch(
        self,
        operations: Sequence[Operation],
        *,
        timeout: Optional[float] = None,
    ) -> BatchResult:
        if self._closed:
            raise RuntimeError("client is closed")
        prepared = [self._prepare(index, op) for index, op in enumerate(operations)]
        grouped: Dict[_Server, List[_Prepared]] = {}
        for item in prepared:
            grouped.setdefault(self._server_for(item.operation.key), []).append(item)
        output: List[Optional[Result]] = [None] * len(prepared)
        async with anyio.create_task_group() as tasks:
            for server, group in grouped.items():
                tasks.start_soon(
                    self._run_group,
                    server,
                    group,
                    output,
                    self._timeout(timeout),
                )
        if any(item is None for item in output):
            raise AssertionError("batch executor left an operation unresolved")
        return BatchResult(output)  # type: ignore[arg-type]

    async def _one(self, operation: Operation, timeout: Optional[float]) -> Result:
        result = cast(Result, (await self.batch([operation], timeout=timeout))[0])
        status = result.status
        if status in (GetStatus.AMBIGUOUS, MutationStatus.AMBIGUOUS):
            raise AmbiguousWriteError(result)
        return result

    async def get(
        self,
        key: Key,
        *,
        meta: Meta = Meta.NONE,
        touch: Optional[int] = None,
        no_lru_bump: bool = False,
        unless_cas: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> GetResult[Any]:
        return await self._one(  # type: ignore[return-value]
            Get(key, meta, touch, no_lru_bump, unless_cas), timeout
        )

    async def inspect(
        self,
        key: Key,
        *,
        meta: Meta = Meta.CAS | Meta.TTL | Meta.SIZE,
        no_lru_bump: bool = True,
        timeout: Optional[float] = None,
    ) -> GetResult[Any]:
        return await self._one(  # type: ignore[return-value]
            Get(key, meta=meta, no_lru_bump=no_lru_bump, value=False), timeout
        )

    async def get_with_lease(
        self,
        key: Key,
        *,
        lease_ttl: int,
        refresh_before: Optional[int] = None,
        meta: Meta = Meta.NONE,
        timeout: Optional[float] = None,
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

    async def get_many(
        self,
        keys: Sequence[Key],
        *,
        meta: Meta = Meta.NONE,
        timeout: Optional[float] = None,
    ) -> BatchResult:
        return await self.batch([Get(key, meta=meta) for key in keys], timeout=timeout)

    async def set(
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
        return await self._one(  # type: ignore[return-value]
            Set(key, value, ttl, condition, version, return_cas), timeout
        )

    async def add(self, key: Key, value: Any, **options: Any) -> MutationResult:
        options["condition"] = ABSENT
        return await self.set(key, value, **options)

    async def replace(self, key: Key, value: Any, **options: Any) -> MutationResult:
        options["condition"] = PRESENT
        return await self.set(key, value, **options)

    async def cas(
        self, key: Key, value: Any, cas_token: int, **options: Any
    ) -> MutationResult:
        options["condition"] = IfCas(cas_token)
        return await self.set(key, value, **options)

    async def append_bytes(
        self,
        key: Key,
        value: bytes,
        *,
        vivify_ttl: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        if not isinstance(value, bytes):
            raise TypeError("append_bytes requires bytes")
        return await self._one(  # type: ignore[return-value]
            Set(key, value, mode="append", vivify_ttl=vivify_ttl), timeout
        )

    async def prepend_bytes(
        self,
        key: Key,
        value: bytes,
        *,
        vivify_ttl: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        if not isinstance(value, bytes):
            raise TypeError("prepend_bytes requires bytes")
        return await self._one(  # type: ignore[return-value]
            Set(key, value, mode="prepend", vivify_ttl=vivify_ttl), timeout
        )

    async def delete(
        self,
        key: Key,
        *,
        condition: Optional[IfCas] = None,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        return await self._one(  # type: ignore[return-value]
            Delete(key, condition), timeout
        )

    async def invalidate(
        self,
        key: Key,
        *,
        stale_for: Optional[int] = None,
        condition: Optional[IfCas] = None,
        timeout: Optional[float] = None,
    ) -> MutationResult:
        return await self._one(  # type: ignore[return-value]
            Delete(key, condition, invalidate=True, stale_for=stale_for), timeout
        )

    async def increment(
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
        return await self._one(  # type: ignore[return-value]
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

    async def decrement(
        self, key: Key, delta: int = 1, **options: Any
    ) -> ArithmeticResult:
        timeout = options.pop("timeout", None) if "timeout" in options else None
        operation = Increment(key, delta=delta, decrement=True, **options)
        return await self._one(operation, timeout)  # type: ignore[return-value]

    async def touch(
        self, key: Key, ttl: int, *, timeout: Optional[float] = None
    ) -> MutationResult:
        result = await self._one(Get(key, touch=ttl, value=False), timeout)
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

    async def flush_all(self, delay: int = 0) -> None:
        _positive("delay", delay)
        async with anyio.create_task_group() as tasks:
            for server in self._servers:
                tasks.start_soon(server.flush, delay)
