import json
import pickle
from typing import Any, Protocol
from collections.abc import Callable

from .errors import SerializeError


FLAG_BYTES = 0
FLAG_PICKLE = 1 << 0
FLAG_INT = 1 << 1
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
