from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from ..errors import MemcacheError, ProtocolError
from ..meta_command import MetaCommand, MetaResult
from ..serialize import DumpFunc, LoadFunc
from .operation import ABSENT, PRESENT, Delete, Get, IfCas, Increment, Operation, Set
from .result import (
    ArithmeticResult,
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


def key_bytes(key: Key) -> bytes:
    if isinstance(key, str):
        return key.encode("utf-8")
    if isinstance(key, bytes):
        return key
    raise TypeError("key must be str or bytes")


def positive(name: str, value: Optional[int], *, allow_zero: bool = True) -> None:
    if value is None:
        return
    minimum = 0 if allow_zero else 1
    if not isinstance(value, int) or value < minimum:
        raise ValueError("%s must be an integer >= %d" % (name, minimum))


def response_flags(flags: Iterable[bytes]) -> Dict[str, Any]:
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


@dataclass
class Prepared:
    index: int
    operation: Operation
    command: MetaCommand
    side_effect: bool


class MetaProtocol:
    """Transport-independent meta command construction and result parsing."""

    def __init__(self, load_func: LoadFunc, dump_func: DumpFunc) -> None:
        self._load = load_func
        self._dump = dump_func

    def _lease_fulfill(self, key: Key, cas: Optional[int]) -> Callable[..., Any]:
        raise NotImplementedError

    def _prepare(self, index: int, operation: Operation) -> Prepared:
        key = key_bytes(operation.key)
        if isinstance(operation, Get):
            return self._prepare_get(index, operation, key)
        if isinstance(operation, Set):
            return self._prepare_set(index, operation, key)
        if isinstance(operation, Delete):
            return self._prepare_delete(index, operation, key)
        if isinstance(operation, Increment):
            return self._prepare_increment(index, operation, key)
        raise TypeError("unsupported batch operation")

    def _prepare_get(self, index: int, operation: Get, key: bytes) -> Prepared:
        positive("touch", operation.touch)
        positive("lease_ttl", operation.lease_ttl, allow_zero=False)
        positive("refresh_before", operation.refresh_before, allow_zero=False)
        if operation.refresh_before is not None and operation.lease_ttl is None:
            raise ValueError("refresh_before requires lease_ttl")
        if operation.unless_cas is not None and not operation.value:
            raise ValueError("unless_cas requires a value read")
        positive("unless_cas", operation.unless_cas)
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
        return Prepared(index, operation, command, side_effect)

    def _prepare_set(self, index: int, operation: Set, key: bytes) -> Prepared:
        positive("ttl", operation.ttl)
        positive("version", operation.version)
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
            positive("vivify_ttl", operation.vivify_ttl, allow_zero=False)
            flags.append(b"N%d" % operation.vivify_ttl)
        command = MetaCommand(b"ms", key, len(raw), flags, raw)
        return Prepared(index, operation, command, True)

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

    def _prepare_delete(self, index: int, operation: Delete, key: bytes) -> Prepared:
        positive("stale_for", operation.stale_for)
        if operation.stale_for is not None and not operation.invalidate:
            raise ValueError("stale_for is only valid for invalidate")
        flags = []
        if operation.condition is not None:
            flags.append(b"C%d" % operation.condition.token)
        if operation.invalidate:
            flags.append(b"I")
            if operation.stale_for is not None:
                flags.append(b"T%d" % operation.stale_for)
        return Prepared(index, operation, MetaCommand(b"md", key, flags=flags), True)

    def _prepare_increment(
        self, index: int, operation: Increment, key: bytes
    ) -> Prepared:
        positive("delta", operation.delta)
        positive("initial", operation.initial)
        positive("initial_ttl", operation.initial_ttl, allow_zero=False)
        positive("ttl", operation.ttl)
        positive("version", operation.version)
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
        return Prepared(index, operation, MetaCommand(b"ma", key, flags=flags), True)

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
        if isinstance(operation, Increment):
            return ArithmeticResult(operation.key, mutation_status, error=error)
        return MutationResult(operation.key, mutation_status, error=error)

    def _parse(self, prepared: Prepared, response: Optional[MetaResult]) -> Result:
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
        parsed = response_flags(response.flags)
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
                    key_bytes(operation.key),
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
            if isinstance(operation, Set) and operation.condition is ABSENT:
                return MutationStatus.ALREADY_EXISTS
            return MutationStatus.NOT_FOUND
        return MutationStatus.FAILED

    @staticmethod
    def _pipeline_command(item: Prepared) -> MetaCommand:
        flags = item.command.flags + [b"O%d" % item.index]
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
        output: List[Optional[Result]],
        item: Prepared,
        response: Optional[MetaResult],
    ) -> None:
        try:
            output[item.index] = self._parse(item, response)
        except BaseException as exc:
            output[item.index] = self._failure(item, False, exc)

    def _resolve_group(
        self,
        prepared: List[Prepared],
        output: List[Optional[Result]],
        responses: Sequence[MetaResult],
        written: int,
        barrier: bool,
        failure: Optional[BaseException],
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
