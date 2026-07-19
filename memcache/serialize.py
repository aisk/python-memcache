import bz2
import gzip
import json
import lzma
import pickle
import sys
import zlib
from typing import Any, Protocol
from collections.abc import Callable

from .errors import SerializeError

FLAG_BYTES = 0
FLAG_PICKLE = 1 << 0
FLAG_INT = 1 << 1
FLAG_COMPRESSED = 1 << 3
FLAG_STR = 1 << 4
FLAG_JSON = 1 << 5


class Serializer(Protocol):
    """Paired value codec; ``load`` must understand every flag ``dump`` emits."""

    def dump(self, key: str | bytes, value: Any) -> tuple[bytes, int]: ...

    def load(self, key: str | bytes, value: bytes, flags: int) -> Any: ...


class BaseSerializer:
    """Shared primitive fast path: bytes, int, and str map to stable flags.

    ``int`` is stored as decimal ASCII so values stay usable by the server's
    arithmetic commands, and the flag values match across all serializers so
    primitive data written under one remains readable under another. ``bool``
    is routed to the object hooks: letting it fall through to the int branch
    would silently round-trip ``True`` as ``1``.
    """

    def dump(self, key: str | bytes, value: Any) -> tuple[bytes, int]:
        if isinstance(value, bytes):
            return value, FLAG_BYTES
        if isinstance(value, bool):
            return self.dump_object(key, value)
        if isinstance(value, int):
            return b"%d" % value, FLAG_INT
        if isinstance(value, str):
            return value.encode(), FLAG_STR
        return self.dump_object(key, value)

    def load(self, key: str | bytes, value: bytes, flags: int) -> Any:
        if flags == FLAG_BYTES:
            return value
        if flags == FLAG_INT:
            return int(value)
        if flags == FLAG_STR:
            return value.decode()
        return self.load_object(key, value, flags)

    def dump_object(self, key: str | bytes, value: Any) -> tuple[bytes, int]:
        raise NotImplementedError

    def load_object(self, key: str | bytes, value: bytes, flags: int) -> Any:
        raise NotImplementedError


class StrictSerializer(BaseSerializer):
    """Stores bytes, int, and str only; never pickles and never unpickles."""

    def dump_object(self, key: str | bytes, value: Any) -> tuple[bytes, int]:
        raise TypeError(
            "cannot serialize %s value for key %r: StrictSerializer stores "
            "only bytes, int, and str; pass serializer=PickleSerializer() "
            "or JsonSerializer() to store other types" % (type(value).__name__, key)
        )

    def load_object(self, key: str | bytes, value: bytes, flags: int) -> Any:
        if flags == FLAG_PICKLE:
            raise SerializeError(
                "key %r holds a pickled value; reading it requires "
                "serializer=PickleSerializer()" % (key,)
            )
        if flags == FLAG_JSON:
            raise SerializeError(
                "key %r holds a JSON value; reading it requires "
                "serializer=JsonSerializer()" % (key,)
            )
        raise SerializeError(f"Unrecognized flags: {flags}")


class PickleSerializer(BaseSerializer):
    """Falls back to pickle for arbitrary objects.

    ``pickle.loads`` executes code embedded in the payload; use this only
    when every writer to the cache is trusted.
    """

    def dump_object(self, key: str | bytes, value: Any) -> tuple[bytes, int]:
        return pickle.dumps(value), FLAG_PICKLE

    def load_object(self, key: str | bytes, value: bytes, flags: int) -> Any:
        if flags == FLAG_PICKLE:
            return pickle.loads(value)
        raise SerializeError(f"Unrecognized flags: {flags}")


class JsonSerializer(BaseSerializer):
    """Falls back to JSON for arbitrary objects.

    Cross-language but lossy: tuples come back as lists, dict keys are
    coerced to str, and bytes inside containers are rejected.
    """

    def dump_object(self, key: str | bytes, value: Any) -> tuple[bytes, int]:
        return json.dumps(value, separators=(",", ":")).encode(), FLAG_JSON

    def load_object(self, key: str | bytes, value: bytes, flags: int) -> Any:
        if flags == FLAG_JSON:
            return json.loads(value)
        raise SerializeError(f"Unrecognized flags: {flags}")


if sys.version_info >= (3, 14):  # stdlib zstd arrived in Python 3.14
    from compression import zstd
else:
    from backports import zstd

# Formats with an unambiguous leading magic, tried in order before the
# weaker zlib header heuristic below.
_SNIFF_TABLE: list[tuple[bytes, Callable[[bytes], bytes]]] = [
    (b"\x28\xb5\x2f\xfd", zstd.decompress),
    (b"\x1f\x8b", gzip.decompress),
    (b"BZh", bz2.decompress),
    (b"\xfd7zXZ\x00", lzma.decompress),
]


def _decompress(key: str | bytes, data: bytes) -> bytes:
    for magic, decompress in _SNIFF_TABLE:
        if data.startswith(magic):
            try:
                return decompress(data)
            except Exception as exc:
                raise SerializeError(
                    "key %r holds corrupt compressed data" % (key,)
                ) from exc
    # zlib has no magic; its 2-byte header satisfies these constraints, and
    # a false positive still fails the adler32 check inside decompress.
    if len(data) >= 2 and data[0] & 0x0F == 8 and ((data[0] << 8) | data[1]) % 31 == 0:
        try:
            return zlib.decompress(data)
        except zlib.error as exc:
            raise SerializeError(
                "key %r holds corrupt compressed data" % (key,)
            ) from exc
    raise SerializeError(
        "key %r is flagged compressed but has no recognizable "
        "compression header" % (key,)
    )


class CompressedSerializer:
    """Wraps another serializer, compressing large payloads with zstd.

    zstd dominates the cache-hot-path tradeoff: better ratio than zlib at
    a fraction of the CPU, with decompression speed where a read-heavy
    cache spends its time (stdlib on 3.14+, ``backports.zstd`` before).
    Reads sniff the payload header instead of trusting a codec label, so
    zstd, zlib, gzip, bz2, and lzma/xz payloads are all readable
    regardless of which client or library version wrote them.

    ``FLAG_COMPRESSED`` is set only when compression actually shrank the
    payload; incompressible values are stored as-is. ``FLAG_INT`` values
    are never compressed because the server-side arithmetic commands parse
    the stored bytes directly.
    """

    def __init__(
        self,
        serializer: Serializer,
        *,
        min_size: int = 1024,
        level: int | None = None,
    ) -> None:
        if min_size < 1:
            raise ValueError("min_size must be a positive integer")
        self._serializer = serializer
        self._min_size = min_size
        self._level = level

    def dump(self, key: str | bytes, value: Any) -> tuple[bytes, int]:
        raw, flags = self._serializer.dump(key, value)
        if flags == FLAG_INT or len(raw) < self._min_size:
            return raw, flags
        packed = zstd.compress(raw, self._level)
        if len(packed) >= len(raw):
            return raw, flags
        return packed, flags | FLAG_COMPRESSED

    def load(self, key: str | bytes, value: bytes, flags: int) -> Any:
        if flags & FLAG_COMPRESSED:
            value = _decompress(key, value)
            flags &= ~FLAG_COMPRESSED
        return self._serializer.load(key, value, flags)


DumpFunc = Callable[[str | bytes, Any], tuple[bytes, int]]
LoadFunc = Callable[[str | bytes, bytes, int], Any]


class FuncSerializer:
    """Adapts a legacy ``(dump_func, load_func)`` pair to ``Serializer``."""

    def __init__(self, dump_func: DumpFunc, load_func: LoadFunc) -> None:
        self._dump_func = dump_func
        self._load_func = load_func

    def dump(self, key: str | bytes, value: Any) -> tuple[bytes, int]:
        return self._dump_func(key, value)

    def load(self, key: str | bytes, value: bytes, flags: int) -> Any:
        return self._load_func(key, value, flags)


_default = PickleSerializer()


def dump(key: str | bytes, value: Any) -> tuple[bytes, int]:
    return _default.dump(key, value)


def load(key: str | bytes, value: bytes, flags: int) -> Any:
    return _default.load(key, value, flags)
