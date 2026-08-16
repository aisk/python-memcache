"""Transport-independent core of the scenario-level clients.

This module holds everything :class:`~memcache.experiment.Memcache` and
:class:`~memcache.experiment.AsyncMemcache` share: the wire-op planning and
attribution that turns scenario verbs into quiet meta-protocol pipelines, and
the interpretation that turns wire responses back into business values. The
sync and async clients only add transport (connection pools, task scheduling).

The vocabulary boundary is enforced here: protocol concepts (cas tokens,
lease flags, stale markers) are consumed inside the ``finish`` closures and
never appear in a scenario-level signature or return value.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from collections.abc import Callable, Sequence

import logging

from ..connection import Addr
from ..errors import (
    AmbiguousWriteError,
    MemcacheError,
    OperationFailedError,
    ProtocolError,
    SerializeError,
)
from ..meta_command import MetaCommand, MetaResult
from ..serialize import Serializer, StrictSerializer
from .meta_api import (
    Key,
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

FOREVER = 0
"""The ttl for entries that must never expire, matching the protocol's 0."""

LEASE_TTL = 30
"""How long a miss-path lease placeholder lives, in seconds.

A crashed winner's exclusive right to recompute expires on its own after
this long, so a later request re-elects instead of waiting forever.
"""

WAIT_BACKOFF = (0.025, 0.05, 0.1, 0.2, 0.4)
"""A loser's cross-process polling schedule while another process's winner
recomputes. When the schedule is exhausted the caller computes locally
without writing back."""

UPDATE_ATTEMPTS = 8
"""Retry bound for the optimistic concurrency loops in update and pop."""

# memcached reads a TTL beyond 30 days as an absolute unix timestamp, so
# longer relative lifetimes must be converted before they hit the wire.
_UNIX_TTL_THRESHOLD = 30 * 24 * 60 * 60

_MISS = object()
"""Internal miss marker, distinguishable from any user default."""

RAISE = object()
"""Degrade marker for verbs whose answer feeds business decisions: their
infrastructure failures are never absorbed, even in degrade mode."""


def wire_ttl(ttl: int, name: str = "ttl") -> int:
    """Validate a caller ttl and convert it to what goes on the wire."""
    if isinstance(ttl, bool) or not isinstance(ttl, int):
        raise TypeError(
            "%s must be an int number of seconds; use FOREVER (0) for no expiry" % name
        )
    if ttl < 0:
        raise ValueError("%s must not be negative" % name)
    if ttl > _UNIX_TTL_THRESHOLD:
        return int(time.time()) + ttl
    return ttl


def check_refresh_ahead(ttl: int, refresh_ahead: int | None) -> int | None:
    """Validate refresh_ahead against the caller's ttl; None disables it."""
    if refresh_ahead is None:
        return None
    if isinstance(refresh_ahead, bool) or not isinstance(refresh_ahead, int):
        raise TypeError("refresh_ahead must be an int number of seconds")
    if refresh_ahead < 0:
        raise ValueError("refresh_ahead must not be negative")
    if refresh_ahead == 0:
        return None
    if ttl != FOREVER and refresh_ahead >= ttl:
        # A window at or beyond the ttl would put every write-back already
        # inside it, turning steady reads into a perpetual recompute loop.
        raise ValueError("refresh_ahead must be shorter than ttl")
    return refresh_ahead


def normalize_addresses(servers: Sequence[Addr]) -> list[Addr]:
    """Turn the client constructor's positional servers into a server list."""
    if not servers:
        return [("localhost", 11211)]
    addresses = []
    for server in servers:
        if (
            not isinstance(server, tuple)
            or len(server) != 2
            or not isinstance(server[0], str)
            or not isinstance(server[1], int)
        ):
            raise TypeError("each server must be a (host, port) tuple")
        addresses.append(server)
    return addresses


def routing_key(key: bytes) -> str:
    """The string form of ``key`` fed to the hash ring for server routing.

    Keys are normalized to the bytes that go on the wire before the ring
    sees them, so a str key and its utf-8 bytes twin route to the same
    server. latin-1 is the transport back to ``str`` because it is the one
    codec that maps every byte, and it leaves ascii keys where they already
    route.
    """
    return key.decode("latin-1")


@dataclass
class WireOp:
    """One wire command with the attribution facts the executor needs."""

    command: MetaCommand
    side_effect: bool
    """Whether the command, once written, may have changed server state.

    Decides ambiguity: a failed pipeline reports a written side-effect
    command as ambiguous rather than failed.
    """
    quiet: bool = True
    """Whether the success/miss response is suppressed; silence then means
    the settled quiet outcome for the verb (miss for reads, done for
    writes)."""


@dataclass
class WireOutcome:
    """What one wire command produced, successful or not."""

    response: MetaCommandResult | None = None
    """The parsed response; None means the quiet silence the op arranged
    for, which is a settled answer, not an error."""
    error: BaseException | None = None
    ambiguous: bool = False


Finish = Callable[[WireOutcome], tuple[Any, "ReadView | None"]]
"""Interprets one outcome into ``(business value, accidental stale win)``.

Raises for operations that never reached an answer. The second element is
the read view of a stale entry whose recache token this read accidentally
consumed; the client hands it back to the server (see plan_return_win).
"""


def pipeline_command(index: int, op: WireOp) -> MetaCommand:
    """The op's wire command tagged for pipelined execution."""
    flags = op.command.flags + [b"O%d" % index]
    if op.quiet:
        flags.append(b"q")
    return MetaCommand(
        op.command.cm,
        op.command.key,
        op.command.datalen,
        flags,
        op.command.value,
    )


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


def resolve_group(
    group: Sequence[tuple[int, WireOp]],
    output: list[WireOutcome | None],
    responses: Sequence[MetaResult],
    written: int,
    barrier: bool,
    failure: BaseException | None,
    confirmed: int = 0,
) -> None:
    """Attribute one server's responses back onto its operations.

    ``confirmed`` counts leading commands that cleared a barrier of their
    own in an earlier chunk. Their silence is the ordinary quiet-protocol
    kind and means a settled outcome, exactly as ``barrier`` does for the
    whole pipeline, so a later chunk's failure must not drag them into
    failed or ambiguous.
    """
    by_index, index_failure = _index_responses(responses)
    if index_failure is not None:
        failure = index_failure
    for position, (index, op) in enumerate(group):
        raw = by_index.get(index)
        if raw is not None:
            try:
                output[index] = WireOutcome(response=parse_meta_result(raw))
            except Exception as exc:
                output[index] = WireOutcome(error=exc)
        elif barrier or position < confirmed:
            output[index] = WireOutcome()
        else:
            error = failure or MemcacheError("pipeline did not reach barrier")
            output[index] = WireOutcome(
                error=error, ambiguous=position < written and op.side_effect
            )


def finalize_outcomes(output: list[WireOutcome | None]) -> list[WireOutcome]:
    if any(item is None for item in output):
        raise AssertionError("pipeline executor left an operation unresolved")
    return output  # type: ignore[return-value]


@dataclass(frozen=True)
class ReadView:
    """One read response reduced to the facts the scenario layer branches on."""

    hit: bool
    data: bytes | None
    client_flags: int
    cas: int | None = None
    ttl: int | None = None
    size: int | None = None
    last_access: int | None = None
    hit_before: bool | None = None
    won: bool = False
    busy: bool = False
    stale: bool = False


_NO_ITEM = ReadView(hit=False, data=None, client_flags=0)


def read_view(response: MetaCommandResult | None) -> ReadView:
    if response is None or response.rc == b"EN":
        return _NO_ITEM
    if response.rc not in (b"VA", b"HD"):
        raise ProtocolError("unexpected read response %r" % response.rc)
    return ReadView(
        hit=True,
        data=response.value,
        client_flags=response.client_flags or 0,
        cas=response.cas,
        ttl=response.ttl,
        size=response.size,
        last_access=response.last_access,
        hit_before=response.hit_before,
        won=response.won,
        busy=response.busy,
        stale=response.stale,
    )


def settle(key: Key, outcome: WireOutcome) -> MetaCommandResult | None:
    """Raise for an operation that never reached an answer.

    Returns the parsed response; ``None`` is the quiet silence the op's
    planner arranged for. Semantic outcomes such as a miss or a compare
    mismatch are answers and pass through in the response.
    """
    if outcome.ambiguous:
        raise AmbiguousWriteError() from outcome.error
    if outcome.error is not None:
        raise OperationFailedError(key) from outcome.error
    return outcome.response


@dataclass(frozen=True)
class ItemInfo:
    """Read-only item metadata returned by ``inspect``.

    ``ttl`` is the remaining lifetime in seconds, -1 for entries that never
    expire. ``last_access`` is seconds since the item was last read or
    written. ``hit_before`` reports whether the item was ever hit since it
    was stored.
    """

    ttl: int | None
    size: int | None
    last_access: int | None
    hit_before: bool | None


@dataclass
class Call:
    """One scenario verb planned down to a wire op and its interpretation."""

    key: Key
    """The caller's key, for exception messages and the failure hook."""
    op: WireOp
    finish: Finish
    absorb: Any
    """What the verb returns when degrade mode absorbs an infrastructure
    failure; :data:`RAISE` for verbs whose failures always surface."""


def dump_value(serializer: Serializer, key: Key, value: Any) -> tuple[bytes, int]:
    """Serialize a value, enforcing the zero-byte rule.

    Zero-byte entries are the coordination placeholders the factory
    machinery creates on the server, and every scenario-level read folds
    them to a miss; letting a user store one would make their value
    silently unreadable.
    """
    raw, client_flags = serializer.dump(key, value)
    if not raw:
        raise SerializeError(
            "key %r: the serialized value is empty; zero-byte values are "
            "reserved as coordination placeholders and cannot be stored" % (key,)
        )
    return raw, client_flags


def fragment_bytes(fragment: Any) -> bytes:
    """Validate an append/prepend fragment; the byte-stream model is bytes
    or str only and bypasses the serializer."""
    if isinstance(fragment, str):
        fragment = fragment.encode("utf-8")
    if not isinstance(fragment, bytes):
        raise TypeError(
            "append/prepend take a bytes or str fragment; the byte-stream "
            "value model bypasses the serializer"
        )
    if not fragment:
        raise ValueError("append/prepend fragment must not be empty")
    return fragment


def _stale_win(view: ReadView) -> ReadView | None:
    return view if view.stale and view.won else None


def _unexpected(key: Key, what: str, response: MetaCommandResult) -> Exception:
    error = OperationFailedError(key)
    error.__cause__ = ProtocolError("unexpected %s response %r" % (what, response.rc))
    return error


def plan_probe(
    wire_key: Key, key: Key
) -> tuple[WireOp, Callable[[WireOutcome], ReadView]]:
    """A value read that surfaces the raw view; update/pop's read step."""
    command = build_get(
        wire_key,
        value=True,
        return_client_flags=True,
        return_cas=True,
        return_ttl=True,
    )

    def finish(outcome: WireOutcome) -> ReadView:
        return read_view(settle(key, outcome))

    return WireOp(command, side_effect=False), finish


def plan_get(
    wire_key: Key,
    key: Key,
    serializer: Serializer,
    default: Any,
    extend_ttl: int | None = None,
) -> tuple[WireOp, Finish]:
    command = build_get(
        wire_key,
        value=True,
        return_client_flags=True,
        return_cas=True,
        return_ttl=True,
        touch=extend_ttl,
    )

    def finish(outcome: WireOutcome) -> tuple[Any, ReadView | None]:
        view = read_view(settle(key, outcome))
        win = _stale_win(view)
        if not view.hit or not view.data:
            # A miss, a coordination placeholder, or a genuinely empty
            # entry: the zero-byte rule folds them all to the default.
            return default, win
        return serializer.load(key, view.data, view.client_flags), win

    return WireOp(command, side_effect=extend_ttl is not None), finish


def plan_store(
    wire_key: Key,
    key: Key,
    raw: bytes,
    client_flags: int | None,
    ttl: int,
    mode: str = "set",
    compare_cas: int | None = None,
) -> tuple[WireOp, Callable[[WireOutcome], bool]]:
    """A store whose finish reports whether it was applied.

    NS/EX/NF are answers (the conditional write's condition failed), not
    errors; the caller decides whether False means retry or a result.
    """
    command = build_set(
        wire_key,
        raw,
        client_flags=client_flags,
        ttl=ttl,
        mode=mode,
        compare_cas=compare_cas,
    )

    def finish(outcome: WireOutcome) -> bool:
        response = settle(key, outcome)
        if response is None or response.rc == b"HD":
            return True
        if response.rc in (b"NS", b"EX", b"NF"):
            return False
        raise _unexpected(key, "store", response)

    return WireOp(command, side_effect=True), finish


def plan_erase(
    wire_key: Key,
    key: Key,
    grace: int = 0,
    compare_cas: int | None = None,
) -> tuple[WireOp, Callable[[WireOutcome], bool]]:
    """A delete (or soft invalidation when ``grace`` > 0) reporting whether
    it found something to erase."""
    command = build_delete(
        wire_key,
        compare_cas=compare_cas,
        invalidate=grace > 0,
        ttl=grace if grace > 0 else None,
    )

    def finish(outcome: WireOutcome) -> bool:
        response = settle(key, outcome)
        if response is None or response.rc == b"HD":
            return True
        if response.rc in (b"NF", b"EX"):
            return False
        raise _unexpected(key, "delete", response)

    return WireOp(command, side_effect=True), finish


def plan_touch(wire_key: Key, key: Key, ttl: int) -> tuple[WireOp, Finish]:
    command = build_get(
        wire_key,
        value=False,
        return_cas=True,
        return_ttl=True,
        touch=ttl,
    )

    def finish(outcome: WireOutcome) -> tuple[Any, ReadView | None]:
        view = read_view(settle(key, outcome))
        return view.hit, _stale_win(view)

    return WireOp(command, side_effect=True), finish


def plan_counter(
    wire_key: Key, key: Key, delta: int, decrement: bool, ttl: int
) -> tuple[WireOp, Finish]:
    if isinstance(delta, bool) or not isinstance(delta, int) or delta < 1:
        raise ValueError("delta must be a positive integer")
    # A miss counts from zero: seeding the vivified item with the delta (or
    # zero when decrementing toward the floor) makes the first operation
    # behave as zero plus/minus delta. The ttl rides the creation only.
    initial = 0 if decrement else delta
    command = build_arithmetic(
        wire_key,
        delta=delta,
        decrement=decrement,
        initial=initial,
        initial_ttl=ttl,
        return_value=True,
    )

    def finish(outcome: WireOutcome) -> tuple[Any, ReadView | None]:
        response = settle(key, outcome)
        if response is not None and response.rc == b"VA" and response.value:
            try:
                return int(response.value), None
            except ValueError:
                pass
        error = OperationFailedError(key)
        error.__cause__ = ProtocolError(
            "counter %r did not return an updated value" % (key,)
        )
        raise error

    return WireOp(command, side_effect=True, quiet=False), finish


def plan_concat(
    wire_key: Key, key: Key, fragment: bytes, ttl: int, mode: str
) -> tuple[WireOp, Finish]:
    command = build_set(wire_key, fragment, mode=mode, vivify_ttl=ttl)

    def finish(outcome: WireOutcome) -> tuple[Any, ReadView | None]:
        response = settle(key, outcome)
        if response is None or response.rc == b"HD":
            return None, None
        raise _unexpected(key, mode, response)

    return WireOp(command, side_effect=True), finish


def plan_inspect(wire_key: Key, key: Key) -> tuple[WireOp, Finish]:
    command = build_get(
        wire_key,
        value=False,
        return_cas=True,
        return_ttl=True,
        return_size=True,
        return_last_access=True,
        return_hit_before=True,
        no_lru_bump=True,
    )

    def finish(outcome: WireOutcome) -> tuple[Any, ReadView | None]:
        view = read_view(settle(key, outcome))
        win = _stale_win(view)
        if not view.hit:
            return None, win
        info = ItemInfo(
            ttl=view.ttl,
            size=view.size,
            last_access=view.last_access,
            hit_before=view.hit_before,
        )
        return info, win

    return WireOp(command, side_effect=False), finish


def plan_election(
    wire_key: Key, key: Key, refresh_ahead: int | None
) -> tuple[WireOp, Callable[[WireOutcome], ReadView]]:
    """The factory path's combined read and server-side election.

    On a miss the vivify flag creates a zero-byte placeholder and grants
    this reader the recompute lease; near expiry the recache window elects
    one reader ahead of time; during a soft-delete grace the stale marker
    elects one reader to refresh. All of that arrives as flags on one read.
    """
    command = build_get(
        wire_key,
        value=True,
        return_client_flags=True,
        return_cas=True,
        return_ttl=True,
        vivify_ttl=LEASE_TTL,
        recache_ttl=refresh_ahead,
    )

    def finish(outcome: WireOutcome) -> ReadView:
        return read_view(settle(key, outcome))

    return WireOp(command, side_effect=True), finish


def plan_return_win(wire_key: Key, view: ReadView) -> WireOp:
    """Hand an accidentally consumed stale-recache token back.

    memcached grants a stale entry's single recache token to the first
    reader regardless of what it asked for, and never re-grants it until
    the entry is written. A read without a factory cannot recompute
    anything, so keeping the token would silently disable the factory
    election for the rest of the grace period. Re-invalidating with the
    version just read resets the token without changing the value or
    shrinking the window.
    """
    positive("cas", view.cas)
    ttl = view.ttl if view.ttl is not None and view.ttl > 0 else None
    command = build_delete(wire_key, compare_cas=view.cas, invalidate=True, ttl=ttl)
    return WireOp(command, side_effect=True)


logger = logging.getLogger("memcache")

MISSING = object()
"""Marks an argument the caller did not pass (update's default)."""


class ScenarioBase:
    """Policy state and verb planning shared by the sync and async clients.

    Everything here is transport-free: subclasses add connections, task
    scheduling, and the flight bookkeeping that matches their concurrency
    model.
    """

    def __init__(
        self,
        serializer: Serializer | None,
        prefix: str,
        on_error: str,
        on_failure: Callable[[BaseException], None] | None,
        timeout: float | None,
    ) -> None:
        if on_error not in ("raise", "degrade"):
            raise ValueError("on_error must be 'raise' or 'degrade'")
        if not isinstance(prefix, str):
            raise TypeError("prefix must be a str")
        self._serializer = serializer if serializer is not None else StrictSerializer()
        self._prefix = prefix
        self._degrade = on_error == "degrade"
        self._on_failure = on_failure
        self._timeout = timeout
        self._closed = False

    # -- keys ----------------------------------------------------------

    def _wire_key(self, key: Key) -> Key:
        if isinstance(key, str):
            return self._prefix + key if self._prefix else key
        if isinstance(key, bytes):
            return self._prefix.encode("utf-8") + key if self._prefix else key
        raise TypeError("key must be str or bytes")

    def _merge_key(self, key: Key) -> bytes:
        return key_bytes(self._wire_key(key))

    # -- reporting and degrade policy ----------------------------------

    def _report(self, error: BaseException) -> None:
        """Deliver a failure that never reaches a caller to the hook."""
        hook = self._on_failure
        if hook is None:
            logger.warning("memcache failure: %s", error, exc_info=error)
            return
        try:
            hook(error)
        except Exception:
            logger.exception("memcache on_failure hook raised")

    def _absorb(self, error: Exception) -> bool:
        """Whether degrade mode swallows this failure.

        Ambiguous writes never degrade: degrading covers "the cache is
        down", not "the write may or may not have landed". Absorbed
        failures go to the failure hook so degrading business behavior
        never degrades observability.
        """
        if not self._degrade or isinstance(error, AmbiguousWriteError):
            return False
        self._report(error)
        return True

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("client is closed")

    # -- verb planning, shared by direct calls and pipelines -----------

    def _call_get(self, key: Key, default: Any, extend_ttl: int | None) -> Call:
        extend = None if extend_ttl is None else wire_ttl(extend_ttl, "extend_ttl")
        op, finish = plan_get(
            self._wire_key(key), key, self._serializer, default, extend
        )
        return Call(key, op, finish, absorb=default)

    def _call_set(self, key: Key, value: Any, ttl: int) -> Call:
        ttl = wire_ttl(ttl)
        raw, client_flags = dump_value(self._serializer, key, value)
        op, stored = plan_store(self._wire_key(key), key, raw, client_flags, ttl)

        def finish(outcome: WireOutcome) -> tuple[Any, ReadView | None]:
            if not stored(outcome):
                error = OperationFailedError(key)
                error.__cause__ = ProtocolError("unconditional set was not stored")
                raise error
            return None, None

        return Call(key, op, finish, absorb=None)

    def _call_store_if(self, key: Key, value: Any, ttl: int, mode: str) -> Call:
        ttl = wire_ttl(ttl)
        raw, client_flags = dump_value(self._serializer, key, value)
        op, stored = plan_store(
            self._wire_key(key), key, raw, client_flags, ttl, mode=mode
        )

        def finish(outcome: WireOutcome) -> tuple[Any, ReadView | None]:
            return stored(outcome), None

        # "Did the key exist" is the answer a conditional write produces;
        # inventing one during an outage misleads both ways, so no absorb.
        return Call(key, op, finish, absorb=RAISE)

    def _call_delete(self, key: Key, grace: int) -> Call:
        grace = wire_ttl(grace, "grace")
        op, erased = plan_erase(self._wire_key(key), key, grace=grace)

        def finish(outcome: WireOutcome) -> tuple[Any, ReadView | None]:
            return erased(outcome), None

        return Call(key, op, finish, absorb=False)

    def _call_counter(self, key: Key, delta: int, decrement: bool, ttl: int) -> Call:
        op, finish = plan_counter(
            self._wire_key(key), key, delta, decrement, wire_ttl(ttl)
        )
        return Call(key, op, finish, absorb=RAISE)

    def _call_touch(self, key: Key, ttl: int) -> Call:
        op, finish = plan_touch(self._wire_key(key), key, wire_ttl(ttl))
        return Call(key, op, finish, absorb=False)

    def _call_concat(self, key: Key, fragment: Any, ttl: int, mode: str) -> Call:
        op, finish = plan_concat(
            self._wire_key(key), key, fragment_bytes(fragment), wire_ttl(ttl), mode
        )
        return Call(key, op, finish, absorb=None)

    def _call_inspect(self, key: Key) -> Call:
        op, finish = plan_inspect(self._wire_key(key), key)
        return Call(key, op, finish, absorb=None)

    @staticmethod
    def _factory_args(
        factory: Callable[[], Any] | None,
        ttl: int | None,
        refresh_ahead: int | None,
        extend_ttl: int | None,
    ) -> tuple[int, int | None] | None:
        """Validate get's parameter cluster; None means the plain path.

        The constraints are loud runtime errors rather than silent
        ignores: ttl and refresh_ahead only mean something with a factory,
        and a factory requires a ttl because every write states its
        lifetime.
        """
        if factory is None:
            if ttl is not None:
                raise TypeError("ttl is only meaningful together with factory")
            if refresh_ahead is not None:
                raise TypeError(
                    "refresh_ahead is only meaningful together with factory"
                )
            return None
        if extend_ttl is not None:
            raise TypeError(
                "extend_ttl cannot be combined with factory: sliding-expiry "
                "keys and factory-managed keys are different families"
            )
        if ttl is None:
            raise TypeError(
                "factory requires ttl; every write states its lifetime, "
                "FOREVER for no expiry"
            )
        return wire_ttl(ttl), check_refresh_ahead(ttl, refresh_ahead)


__all__ = [
    "FOREVER",
    "ItemInfo",
    "Key",
    "key_bytes",
]
