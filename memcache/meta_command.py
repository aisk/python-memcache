from dataclasses import dataclass
from typing import List, Optional, Union

from .errors import MemcacheError


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
        if self.datalen is None:
            header = b" ".join([self.cm, self.key] + self.flags + [b"\r\n"])
        else:
            datalen = str(self.datalen).encode("utf-8")
            header = b" ".join([self.cm, self.key, datalen] + self.flags + [b"\r\n"])
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
            if str(parts[1][0]).isdigit():
                datalen = int(parts[1])
                flags = parts[2:]
            else:
                flags = parts[1:]
        return MetaResult(rc=rc, datalen=datalen, flags=flags, value=None)
