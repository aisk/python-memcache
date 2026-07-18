"""Typed 1:1 mapping of the memcached meta protocol.

This module builds wire commands (mg/ms/md/ma/me) from named arguments and
parses wire responses into :class:`MetaCommandResult`. Every keyword argument
corresponds to exactly one protocol flag. This layer performs no
serialization and no semantic interpretation of results; both belong to the
high-level client. Validation is limited to argument types/ranges and
combinations the protocol silently ignores.

The ``q`` (noreply) and ``b`` (base64 key) flags are intentionally not
exposed: quiet semantics are an internal concern of the pipeline executor,
and binary keys are base64-encoded automatically by
:class:`~memcache.meta_command.MetaCommand`.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any
from collections.abc import Iterable

from ..errors import ProtocolError
from ..meta_command import MetaCommand, MetaResult, encode_key
from .result import Key


Token = bytes | str | int

_OPAQUE_MAX = 32

_STORE_MODES: dict[str, bytes | None] = {
    "set": None,
    "add": b"ME",
    "replace": b"MR",
    "append": b"MA",
    "prepend": b"MP",
}


def key_bytes(key: Key) -> bytes:
    if isinstance(key, str):
        return key.encode("utf-8")
    if isinstance(key, bytes):
        return key
    raise TypeError("key must be str or bytes")


def positive(name: str, value: int | None, *, allow_zero: bool = True) -> None:
    if value is None:
        return
    minimum = 0 if allow_zero else 1
    if not isinstance(value, int) or value < minimum:
        raise ValueError("%s must be an integer >= %d" % (name, minimum))


_INT_RESPONSE_FLAGS = {
    "f": "client_flags",
    "c": "cas",
    "t": "ttl",
    "l": "last_access",
    "s": "size",
}
_MARKER_RESPONSE_FLAGS = {"W": "won", "Z": "busy", "X": "stale", "b": "key_base64"}
_TOKEN_RESPONSE_FLAGS = {"O": "opaque", "k": "key"}


def response_flags(flags: Iterable[bytes]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for flag in flags:
        if not flag:
            continue
        code = chr(flag[0])
        token = flag[1:]
        if code in _INT_RESPONSE_FLAGS:
            parsed[_INT_RESPONSE_FLAGS[code]] = int(token)
        elif code in _MARKER_RESPONSE_FLAGS:
            parsed[_MARKER_RESPONSE_FLAGS[code]] = True
        elif code in _TOKEN_RESPONSE_FLAGS:
            parsed[_TOKEN_RESPONSE_FLAGS[code]] = token
        elif code == "h":
            parsed["hit_before"] = token != b"0"
    return parsed


@dataclass(frozen=True)
class MetaCommandResult:
    """A lightly parsed meta protocol response.

    ``rc`` is the wire return code (``HD``, ``VA``, ``EN``, ``NS``, ``EX``,
    ``NF``); ``value`` is the raw data block, if any. The remaining fields
    are response flags decoded to native types; a field is ``None`` (or
    ``False`` for markers) when the server did not send the flag.
    """

    rc: bytes
    value: bytes | None = None
    cas: int | None = None
    ttl: int | None = None
    client_flags: int | None = None
    size: int | None = None
    last_access: int | None = None
    hit_before: bool | None = None
    key: bytes | None = None
    opaque: bytes | None = None
    won: bool = False
    busy: bool = False
    stale: bool = False
    flags: tuple[bytes, ...] = ()

    @property
    def ok(self) -> bool:
        return self.rc in (b"HD", b"VA")

    def __bool__(self) -> bool:
        return self.ok


def parse_meta_result(result: MetaResult) -> MetaCommandResult:
    parsed = response_flags(result.flags)
    key = parsed.get("key")
    if key is not None and parsed.get("key_base64"):
        key = base64.b64decode(key)
    return MetaCommandResult(
        rc=result.rc,
        value=result.value,
        cas=parsed.get("cas"),
        ttl=parsed.get("ttl"),
        client_flags=parsed.get("client_flags"),
        size=parsed.get("size"),
        last_access=parsed.get("last_access"),
        hit_before=parsed.get("hit_before"),
        key=key,
        opaque=parsed.get("opaque"),
        won=bool(parsed.get("won")),
        busy=bool(parsed.get("busy")),
        stale=bool(parsed.get("stale")),
        flags=tuple(result.flags),
    )


def parse_debug_result(result: MetaResult) -> dict[str, str] | None:
    """Parse an ``me`` response into its ``name=value`` fields."""
    if result.rc == b"EN":
        return None
    if result.rc != b"ME":
        raise ProtocolError("unexpected debug response %r" % result.rc)
    fields: dict[str, str] = {}
    # The first token is the (possibly base64) key; the rest are name=value.
    for token in result.flags[1:]:
        name, _, value = token.partition(b"=")
        fields[name.decode("ascii")] = value.decode("ascii")
    return fields


def _assemble_flags(
    markers: tuple[tuple[bool, bytes], ...],
    tokens: tuple[tuple[bytes, int | None], ...],
    opaque: Token | None,
) -> list[bytes]:
    flags = [wire for enabled, wire in markers if enabled]
    flags.extend(
        prefix + b"%d" % value for prefix, value in tokens if value is not None
    )
    if opaque is not None:
        flags.append(_opaque_flag(opaque))
    return flags


def _opaque_flag(opaque: Token) -> bytes:
    if isinstance(opaque, int):
        positive("opaque", opaque)
        token = str(opaque).encode("ascii")
    elif isinstance(opaque, str):
        token = opaque.encode("utf-8")
    elif isinstance(opaque, bytes):
        token = opaque
    else:
        raise TypeError("opaque must be bytes, str or int")
    if not token or len(token) > _OPAQUE_MAX:
        raise ValueError("opaque must be 1-%d bytes" % _OPAQUE_MAX)
    if any(byte <= 0x20 or byte == 0x7F for byte in token):
        raise ValueError("opaque must not contain whitespace or control bytes")
    return b"O" + token


def build_get(
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
) -> MetaCommand:
    positive("touch", touch)
    positive("vivify_ttl", vivify_ttl)
    positive("recache_ttl", recache_ttl, allow_zero=False)
    positive("unless_cas", unless_cas)
    positive("new_cas", new_cas)
    markers = (
        (value, b"v"),
        (return_client_flags, b"f"),
        (return_cas, b"c"),
        (return_ttl, b"t"),
        (return_size, b"s"),
        (return_last_access, b"l"),
        (return_hit_before, b"h"),
        (return_key, b"k"),
        (no_lru_bump, b"u"),
    )
    tokens = (
        (b"T", touch),
        (b"N", vivify_ttl),
        (b"R", recache_ttl),
        (b"C", unless_cas),
        (b"E", new_cas),
    )
    flags = _assemble_flags(markers, tokens, opaque)
    return MetaCommand(b"mg", key_bytes(key), flags=flags)


def build_set(
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
) -> MetaCommand:
    if not isinstance(value, bytes):
        raise TypeError("meta set requires bytes; serialize before calling")
    if mode not in _STORE_MODES:
        raise ValueError("invalid store mode {!r}".format(mode))
    if vivify_ttl is not None and mode not in ("append", "prepend"):
        raise ValueError("vivify_ttl is only valid for append/prepend")
    if ttl is not None and mode in ("append", "prepend"):
        # The server ignores T for concatenation; the miss path takes its
        # TTL from N (vivify_ttl) instead. Reject the no-op.
        raise ValueError("ttl is ignored for append/prepend; use vivify_ttl")
    if compare_cas is not None and mode == "add":
        # Add only stores when no item exists, so there is no CAS to
        # compare; the protocol leaves the combination undefined.
        raise ValueError("compare_cas cannot be combined with add mode")
    positive("client_flags", client_flags)
    positive("ttl", ttl)
    positive("compare_cas", compare_cas)
    positive("new_cas", new_cas)
    positive("vivify_ttl", vivify_ttl)
    mode_flag = _STORE_MODES[mode]
    markers = (
        (invalidate, b"I"),
        (return_cas, b"c"),
        (return_size, b"s"),
        (return_key, b"k"),
    )
    tokens = (
        (b"F", client_flags),
        (b"T", ttl),
        (b"C", compare_cas),
        (b"E", new_cas),
        (b"N", vivify_ttl),
    )
    flags = [] if mode_flag is None else [mode_flag]
    flags += _assemble_flags(markers, tokens, opaque)
    return MetaCommand(b"ms", key_bytes(key), len(value), flags, value)


def build_delete(
    key: Key,
    *,
    compare_cas: int | None = None,
    new_cas: int | None = None,
    invalidate: bool = False,
    ttl: int | None = None,
    drop_value: bool = False,
    return_key: bool = False,
    opaque: Token | None = None,
) -> MetaCommand:
    positive("compare_cas", compare_cas)
    positive("new_cas", new_cas)
    positive("ttl", ttl)
    if ttl is not None and not invalidate:
        # The server only applies T when paired with I; reject the no-op.
        raise ValueError("ttl is only applied when invalidate is set")
    markers = (
        (invalidate, b"I"),
        (drop_value, b"x"),
        (return_key, b"k"),
    )
    tokens = (
        (b"C", compare_cas),
        (b"E", new_cas),
        (b"T", ttl),
    )
    flags = _assemble_flags(markers, tokens, opaque)
    return MetaCommand(b"md", key_bytes(key), flags=flags)


def build_arithmetic(
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
) -> MetaCommand:
    positive("delta", delta)
    positive("initial", initial)
    positive("initial_ttl", initial_ttl)
    positive("ttl", ttl)
    positive("compare_cas", compare_cas)
    positive("new_cas", new_cas)
    if initial is not None and initial_ttl is None:
        # J is silently ignored without N; reject the no-op.
        raise ValueError("initial requires initial_ttl to vivify on miss")
    markers = (
        (decrement, b"MD"),
        (return_value, b"v"),
        (return_ttl, b"t"),
        (return_cas, b"c"),
        (return_key, b"k"),
    )
    tokens = (
        (b"D", delta),
        (b"N", initial_ttl),
        (b"J", initial),
        (b"T", ttl),
        (b"C", compare_cas),
        (b"E", new_cas),
    )
    flags = _assemble_flags(markers, tokens, opaque)
    return MetaCommand(b"ma", key_bytes(key), flags=flags)


def build_debug(key: Key) -> MetaCommand:
    raw = key_bytes(key)
    _, needs_base64 = encode_key(raw)
    if needs_base64:
        # protocol.txt documents the ``b`` flag for ``me``, but real servers
        # (verified on memcached 1.6.45) look up the base64 token literally,
        # so a binary-key debug would silently report a miss for a live item.
        raise ValueError("meta debug does not support keys that require base64")
    return MetaCommand(b"me", raw)
