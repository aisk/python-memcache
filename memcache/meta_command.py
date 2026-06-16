import base64
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

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


def encode_key(key: bytes) -> Tuple[bytes, bool]:
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
    datalen: Optional[int]
    flags: List[bytes]
    value: Optional[bytes]

    def __init__(
        self,
        cm: bytes,
        key: Union[bytes, str],
        datalen: Optional[int] = None,
        flags: Optional[List[bytes]] = None,
        value: Optional[bytes] = None,
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


@dataclass
class MetaResult:
    rc: bytes
    datalen: Optional[int]
    flags: List[bytes]
    value: Optional[bytes]

    @staticmethod
    def load_header(line: bytes) -> "MetaResult":
        parts = line.split()

        rc = parts[0]
        if rc == b"CLIENT_ERROR":
            # Old ascii protocol error.
            raise MemcacheError(
                line.lstrip(b"CLIENT_ERROR ")
                .rstrip()
                .decode("utf-8")
            )

        flags = []
        datalen = None
        if len(parts) > 1:
            if chr(parts[1][0]).isdigit():
                datalen = int(parts[1])
                flags = parts[2:]
            else:
                flags = parts[1:]
        return MetaResult(rc=rc, datalen=datalen, flags=flags, value=None)
