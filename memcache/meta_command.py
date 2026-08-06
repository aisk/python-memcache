import base64
from dataclasses import dataclass

from .errors import MemcacheError

# memcached limits the key token *on the wire* to 250 bytes and the legacy
# text key may not contain whitespace or control characters. Keys that violate
# the latter are sent base64-encoded with the meta ``b`` flag; since the server
# checks the length before decoding, the base64 form (not the raw key) is what
# must fit in 250 bytes, which caps a binary key at ~186 raw bytes.
MAX_KEY_LENGTH = 250


def _is_legacy_safe(key: bytes) -> bool:
    """Whether ``key`` can be sent verbatim (no whitespace/control chars)."""
    for byte in key:
        if byte <= 0x20 or byte == 0x7F:  # control chars, space and DEL
            return False
    return True


def encode_key(key: bytes) -> tuple[bytes, bool]:
    """Return ``(wire_key, needs_base64_flag)`` for a raw key.

    Raises :class:`MemcacheError` if the wire key exceeds memcached's limit.
    """
    if _is_legacy_safe(key):
        wire_key, needs_base64 = key, False
    else:
        wire_key, needs_base64 = base64.b64encode(key), True
    if len(wire_key) > MAX_KEY_LENGTH:
        raise MemcacheError(
            f"key too long: {len(wire_key)} bytes on the wire "
            f"exceeds maximum {MAX_KEY_LENGTH}"
        )
    return wire_key, needs_base64


@dataclass(init=False)
class MetaCommand:
    cm: bytes
    key: bytes
    datalen: int | None
    flags: list[bytes]
    value: bytes | None

    def __init__(
        self,
        cm: bytes,
        key: bytes | str,
        datalen: int | None = None,
        flags: list[bytes] | None = None,
        value: bytes | None = None,
    ) -> None:
        if isinstance(key, str):
            key = key.encode()
        self.cm = cm
        self.key = key
        self.datalen = datalen
        self.flags = flags or []
        self.value = value

    def dump_header(self) -> bytes:
        wire_key, needs_base64 = encode_key(self.key)
        flags = self.flags
        if needs_base64 and b"b" not in flags:
            flags = flags + [b"b"]
        if self.datalen is None:
            header = b" ".join([self.cm, wire_key] + flags + [b"\r\n"])
        else:
            datalen = str(self.datalen).encode("utf-8")
            header = b" ".join([self.cm, wire_key, datalen] + flags + [b"\r\n"])
        return header

    def dump(self) -> bytes:
        payload = self.dump_header()
        if self.value is not None:
            payload += self.value + b"\r\n"
        return payload


@dataclass
class MetaResult:
    rc: bytes
    datalen: int | None
    flags: list[bytes]
    value: bytes | None

    @property
    def is_barrier(self) -> bool:
        """Whether this is the ``MN`` reply that ends a quiet pipeline."""
        return self.rc == b"MN"

    @staticmethod
    def load_header(line: bytes) -> "MetaResult":
        parts = line.split()

        rc = parts[0]
        if rc in (b"CLIENT_ERROR", b"SERVER_ERROR"):
            # Old ascii protocol error.
            line = line.removeprefix(rc + b" ")
            raise MemcacheError(line.rstrip().decode("utf-8"))

        flags = []
        datalen = None
        if len(parts) > 1:
            # Only VA carries a datalen token; ME responses echo the key,
            # which may itself start with a digit.
            if rc == b"VA" and chr(parts[1][0]).isdigit():
                datalen = int(parts[1])
                flags = parts[2:]
            else:
                flags = parts[1:]
        return MetaResult(rc=rc, datalen=datalen, flags=flags, value=None)


# A response header is a short line of tokens; memcached never emits one
# anywhere near this long, so exceeding it means the stream is corrupt.
MAX_LINE_LENGTH = 1024


class ResponseReader:
    """Sans-IO incremental parser for meta protocol responses.

    Transports feed raw bytes as they arrive off the wire; all framing
    state (header lines, ``VA`` value payloads, ``MN`` pipeline barriers)
    lives here so the parsing logic exists exactly once for the sync and
    async connections. ``next_response``/``next_line`` return ``None``
    when the buffered bytes do not yet hold a complete item.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._pending: MetaResult | None = None

    def feed(self, data: bytes) -> None:
        self._buffer += data

    def next_line(self) -> bytes | None:
        """Take one CRLF-terminated line, without the terminator."""
        end = self._buffer.find(b"\r\n")
        if end < 0:
            if len(self._buffer) > MAX_LINE_LENGTH:
                raise MemcacheError("response line exceeds maximum length")
            return None
        line = bytes(self._buffer[:end])
        del self._buffer[: end + 2]
        return line

    def next_response(self) -> MetaResult | None:
        """Take the next complete response, value payload included."""
        while True:
            pending = self._pending
            if pending is not None:
                assert pending.datalen is not None
                # The payload is followed by a CRLF terminator.
                if len(self._buffer) < pending.datalen + 2:
                    return None
                pending.value = bytes(self._buffer[: pending.datalen])
                del self._buffer[: pending.datalen + 2]
                self._pending = None
                return pending
            line = self.next_line()
            if line is None:
                return None
            result = MetaResult.load_header(line)
            if result.rc != b"VA":
                return result
            if result.datalen is None:
                raise MemcacheError("invalid response: missing datalen")
            self._pending = result
