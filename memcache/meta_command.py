from dataclasses import dataclass
from typing import List, Optional, Union


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
        datalen: int = None,
        flags: List[bytes] = None,
        value: bytes = None,
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
            header = b" ".join(
                [self.cm, self.key, datalen] + self.flags + [b"\r\n"]
            )
        return header


@dataclass
class MetaResult:
    rc: bytes
    datalen: Optional[int]
    flags: List[bytes]
    value: Optional[bytes]
