from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeVar, cast
from collections.abc import Callable, Sequence

from ..connection import Addr
from ..errors import MemcacheError, ProtocolError
from ..meta_command import MetaCommand, MetaResult
from ..serialize import Serializer
from .meta_api import (
    MetaCommandResult,
    build_arithmetic,
    build_delete,
    build_get,
    build_set,
    key_bytes,
    parse_meta_result,
    positive,
    response_flags,
)
from .operation import Arithmetic, Delete, Get, Operation, Set
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
    failure_error,
)

ServerT = TypeVar("ServerT")


@dataclass
class Prepared:
    index: int
    operation: Operation
    command: MetaCommand
    side_effect: bool


def normalize_addresses(addr: Addr | list[Addr] | None) -> list[Addr]:
    """Turn the client constructor's ``addr`` argument into a server list."""
    if addr is None:
        return [("localhost", 11211)]
    if isinstance(addr, tuple) and len(addr) == 2:
        return [addr]
    if isinstance(addr, list) and addr:
        return list(addr)
    raise TypeError("addr must be a server tuple or a non-empty list")


def routing_key(key: Key) -> str:
    """The string form of ``key`` fed to the hash ring for server routing."""
    return key if isinstance(key, str) else key.decode("latin-1")


def raise_on_failure(result: Result) -> Result:
    """Single-operation policy: no answer raises, any answer returns.

    A lone operation has nowhere to put "I do not know", so infrastructure
    trouble leaves through the exception channel. Semantic outcomes such as a
    miss or a CAS mismatch are answers and stay in the returned status.
    """
    error = failure_error(result)
    if error is not None:
        raise error from result.error
    return result


def touch_result(key: Key, result: Result) -> MutationResult:
    """Recast a value-less touch read as the mutation result callers expect."""
    if not isinstance(result, GetResult):
        raise AssertionError("unexpected touch result")
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


def group_prepared(
    prepared: Sequence[Prepared],
    server_for: Callable[[Key], ServerT],
) -> dict[ServerT, list[Prepared]]:
    grouped: dict[ServerT, list[Prepared]] = {}
    for item in prepared:
        grouped.setdefault(server_for(item.operation.key), []).append(item)
    return grouped


def finalize_batch(output: list[Result | None]) -> BatchResult:
    if any(item is None for item in output):
        raise AssertionError("batch executor left an operation unresolved")
    return BatchResult(output)  # type: ignore[arg-type]


def group_meta_commands(
    commands: Sequence[MetaCommand],
    server_for: Callable[[Key], ServerT],
) -> dict[ServerT, list[tuple[int, MetaCommand]]]:
    grouped: dict[ServerT, list[tuple[int, MetaCommand]]] = {}
    for index, command in enumerate(commands):
        if b"q" in command.flags:
            raise ValueError("meta batch does not accept quiet commands")
        grouped.setdefault(server_for(command.key), []).append((index, command))
    return grouped


def fill_meta_responses(
    output: list[MetaResult | None],
    group: list[tuple[int, MetaCommand]],
    responses: Sequence[MetaResult],
) -> None:
    if len(responses) != len(group):
        raise ProtocolError("meta batch received an unexpected response count")
    for (index, _), response in zip(group, responses):
        output[index] = response


def finalize_meta_batch(output: list[MetaResult | None]) -> list[MetaResult]:
    if any(result is None for result in output):
        raise ProtocolError("meta batch left an operation unresolved")
    return cast(list[MetaResult], output)


class MetaProtocol:
    """Transport-independent meta command construction and result parsing."""

    def __init__(self, serializer: Serializer) -> None:
        self._serializer = serializer

    def _lease_fulfill(self, key: Key, cas: int | None) -> Callable[..., Any]:
        raise NotImplementedError

    def _prepare(self, index: int, operation: Operation) -> Prepared:
        key = key_bytes(operation.key)
        if isinstance(operation, Get):
            return self._prepare_get(index, operation, key)
        if isinstance(operation, Set):
            return self._prepare_set(index, operation, key)
        if isinstance(operation, Delete):
            return self._prepare_delete(index, operation, key)
        if isinstance(operation, Arithmetic):
            return self._prepare_arithmetic(index, operation, key)
        raise TypeError("unsupported batch operation")

    def _prepare_get(self, index: int, operation: Get, key: bytes) -> Prepared:
        positive("lease_ttl", operation.lease_ttl, allow_zero=False)
        positive("refresh_before", operation.refresh_before, allow_zero=False)
        if operation.refresh_before is not None and operation.lease_ttl is None:
            raise ValueError("refresh_before requires lease_ttl")
        if operation.unless_cas is not None and not operation.value:
            raise ValueError("unless_cas requires a value read")
        requested_meta = operation.meta
        if operation.lease_ttl is not None or operation.unless_cas is not None:
            requested_meta |= Meta.CAS
        command = build_get(
            operation.key,
            value=operation.value,
            return_client_flags=True,
            return_cas=bool(requested_meta & Meta.CAS),
            return_ttl=bool(requested_meta & Meta.TTL),
            return_size=bool(requested_meta & Meta.SIZE),
            return_last_access=bool(requested_meta & Meta.LAST_ACCESS),
            return_hit_before=bool(requested_meta & Meta.HIT_BEFORE),
            touch=operation.touch,
            vivify_ttl=operation.lease_ttl,
            recache_ttl=operation.refresh_before,
            unless_cas=operation.unless_cas,
            no_lru_bump=operation.no_lru_bump,
        )
        side_effect = any(
            value is not None
            for value in (
                operation.touch,
                operation.lease_ttl,
                operation.refresh_before,
            )
        )
        return Prepared(index, operation, command, side_effect)

    def _prepare_set(self, index: int, operation: Set, key: bytes) -> Prepared:
        positive("version", operation.version)
        positive("vivify_ttl", operation.vivify_ttl, allow_zero=False)
        client_flags: int | None
        if operation.mode in ("append", "prepend"):
            # The server concatenates raw bytes; a serialized payload would
            # corrupt the stored value, so concatenation takes bytes only.
            if not isinstance(operation.value, bytes):
                raise TypeError("%s requires a bytes value" % operation.mode)
            raw, client_flags = operation.value, None
        else:
            raw, client_flags = self._serializer.dump(key, operation.value)
        command = build_set(
            operation.key,
            raw,
            client_flags=client_flags,
            ttl=operation.ttl,
            mode=operation.mode,
            compare_cas=operation.compare_cas,
            new_cas=operation.version,
            vivify_ttl=operation.vivify_ttl,
            return_cas=operation.return_cas,
        )
        return Prepared(index, operation, command, True)

    def _prepare_delete(self, index: int, operation: Delete, key: bytes) -> Prepared:
        positive("stale_for", operation.stale_for)
        if operation.stale_for is not None and not operation.invalidate:
            raise ValueError("stale_for is only valid for invalidate")
        command = build_delete(
            operation.key,
            compare_cas=operation.compare_cas,
            invalidate=operation.invalidate,
            ttl=operation.stale_for,
        )
        return Prepared(index, operation, command, True)

    def _prepare_arithmetic(
        self, index: int, operation: Arithmetic, key: bytes
    ) -> Prepared:
        positive("initial_ttl", operation.initial_ttl, allow_zero=False)
        positive("version", operation.version)
        if operation.initial is not None and operation.initial_ttl is None:
            raise ValueError("initial requires initial_ttl")
        if operation.initial_ttl is not None and operation.initial is None:
            raise ValueError("initial_ttl requires initial")
        command = build_arithmetic(
            operation.key,
            delta=operation.delta,
            decrement=operation.decrement,
            initial=operation.initial,
            initial_ttl=operation.initial_ttl,
            ttl=operation.ttl,
            compare_cas=operation.compare_cas,
            new_cas=operation.version,
            return_ttl=operation.return_ttl,
            return_cas=operation.return_cas,
        )
        return Prepared(index, operation, command, True)

    def _failure(
        self, prepared: Prepared, ambiguous: bool, error: BaseException
    ) -> Result:
        mutation_status = (
            MutationStatus.AMBIGUOUS if ambiguous else MutationStatus.FAILED
        )
        operation = prepared.operation
        if isinstance(operation, Get):
            status = GetStatus.AMBIGUOUS if ambiguous else GetStatus.FAILED
            return GetResult(key=operation.key, status=status, error=error)
        if isinstance(operation, Arithmetic):
            return ArithmeticResult(operation.key, mutation_status, error=error)
        return MutationResult(operation.key, mutation_status, error=error)

    def _parse(self, prepared: Prepared, response: MetaResult | None) -> Result:
        operation = prepared.operation
        if response is None:
            if isinstance(operation, Get):
                return GetResult(key=operation.key, status=GetStatus.MISS)
            if isinstance(operation, Arithmetic):
                return ArithmeticResult(
                    operation.key,
                    MutationStatus.FAILED,
                    error=ProtocolError("arithmetic response was suppressed"),
                )
            return MutationResult(operation.key, MutationStatus.STORED)
        wire = parse_meta_result(response)
        if isinstance(operation, Get):
            return self._parse_get(operation, wire)
        if isinstance(operation, Arithmetic):
            arithmetic_status = self._mutation_status(operation, wire.rc)
            value = int(wire.value) if wire.rc == b"VA" and wire.value else None
            return ArithmeticResult(
                operation.key,
                arithmetic_status,
                value=value,
                item=ItemMeta(cas=wire.cas, ttl=wire.ttl),
            )
        return MutationResult(
            operation.key,
            self._mutation_status(operation, wire.rc),
            cas=wire.cas,
        )

    def _parse_get(self, operation: Get, wire: MetaCommandResult) -> GetResult[Any]:
        if wire.rc == b"EN":
            return GetResult(key=operation.key, status=GetStatus.MISS)
        if wire.rc not in (b"VA", b"HD"):
            return GetResult(
                key=operation.key,
                status=GetStatus.FAILED,
                error=ProtocolError("unexpected get response %r" % wire.rc),
            )
        item = ItemMeta(
            cas=wire.cas,
            ttl=wire.ttl,
            size=wire.size,
            last_access=wire.last_access,
            hit_before=wire.hit_before,
        )
        lease_state = (
            LeaseState.GRANTED
            if wire.won
            else LeaseState.BUSY if wire.busy else LeaseState.NONE
        )
        placeholder = (
            not wire.stale and wire.value == b"" and lease_state is not LeaseState.NONE
        )
        value_state = (
            ValueState.STALE
            if wire.stale
            else ValueState.MISSING if placeholder else ValueState.FRESH
        )
        has_value = False
        value: Any = None
        if placeholder:
            status = (
                GetStatus.MISS if operation.lease_ttl is not None else GetStatus.PENDING
            )
        elif wire.rc == b"HD" and operation.unless_cas is not None:
            status = GetStatus.UNCHANGED
        else:
            status = GetStatus.HIT
            has_value = wire.rc == b"VA" and wire.value is not None
            if has_value:
                value = self._serializer.load(
                    key_bytes(operation.key),
                    wire.value or b"",
                    wire.client_flags or 0,
                )
        kwargs: dict[str, Any] = dict(
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
        return LeaseResult(
            fulfill=self._lease_fulfill(operation.key, item.cas), **kwargs
        )

    @staticmethod
    def _mutation_status(operation: Operation, rc: bytes) -> MutationStatus:
        if rc in (b"HD", b"VA"):
            return MutationStatus.STORED
        if rc == b"EX":
            return MutationStatus.CAS_MISMATCH
        if rc == b"NF":
            return MutationStatus.NOT_FOUND
        if rc == b"NS":
            if isinstance(operation, Set) and operation.mode == "add":
                return MutationStatus.ALREADY_EXISTS
            return MutationStatus.NOT_FOUND
        return MutationStatus.FAILED

    @staticmethod
    def _pipeline_command(item: Prepared) -> MetaCommand:
        flags = item.command.flags + [b"O%d" % item.index]
        needs_success_response = isinstance(item.operation, Arithmetic) or (
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
    ) -> tuple[dict[int, MetaResult], BaseException | None]:
        by_index: dict[int, MetaResult] = {}
        failure: BaseException | None = None
        for response in responses:
            opaque = response_flags(response.flags).get("opaque")
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
        output: list[Result | None],
        item: Prepared,
        response: MetaResult | None,
    ) -> None:
        try:
            output[item.index] = self._parse(item, response)
        except BaseException as exc:
            output[item.index] = self._failure(item, False, exc)

    def _resolve_group(
        self,
        prepared: list[Prepared],
        output: list[Result | None],
        responses: Sequence[MetaResult],
        written: int,
        barrier: bool,
        failure: BaseException | None,
    ) -> None:
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
