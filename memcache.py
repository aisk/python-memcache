import socket
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class MetaCommand:
    cm: bytes
    key: bytes
    flags: List[bytes]
    value: Optional[bytes]


@dataclass
class MetaResult:
    rc: bytes
    flags: List[bytes]
    value: Optional[bytes]


class Connection:
    def __init__(self, ip: str, port: int):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((ip, port))
        self.stream = self.socket.makefile(mode="rwb")

    def close(self) -> None:
        self.stream.close()
        self.socket.close()

    def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        header = b" ".join([command.cm, command.key] + command.flags + [b"\r\n"])
        self.stream.write(header)
        if command.value:
            self.stream.write(command.value + b"\r\n")
        self.stream.flush()
        return self._receive_meta_result()

    def _receive_meta_result(self) -> MetaResult:
        header = self.stream.readline()
        parts = header.split()
        rc = parts[0]
        flags = parts[1:]
        value = None
        if rc == b"VA":
            size = int(parts[1])
            flags = parts[2:]
            value = self.stream.read(size)
            self.stream.read(2)  # read the "\r\n"
        return MetaResult(rc=rc, flags=flags, value=value)


class Memcache:
    def __init__(self, address: str):
        self.address = address
        self.connection = Connection("localhost", 12345)

    def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return self.connection.execute_meta_command(command)
