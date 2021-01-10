import socket
from dataclasses import dataclass
from typing import List


@dataclass
class MetaCommand:
    cm: bytes
    key: bytes
    flags: List[bytes]
    value: bytes


@dataclass
class MetaResult:
    rc: bytes


class Connection:
    def __init__(self, ip: str, port: int):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((ip, port))

    def close(self) -> None:
        self.socket.close()

    def send_meta_command(self, command: MetaCommand) -> MetaResult:
        header = b" ".join([command.cm, command.key] + command.flags + [b"\r\n"])
        self.socket.sendall(header)
        if command.value:
            self.socket.sendall(command.value + b"\r\n")
        response = self.socket.recv(1024)
        rc = response[:2]
        return MetaResult(rc=rc)


class Memcache:
    def __init__(self, address: str):
        self.address = address
        self.connection = Connection("localhost", 12345)
