import socket
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union


NEWLINE = b"\r\n"


class MemcacheError(Exception):
    ...


@dataclass(init=False)
class MetaCommand:
    cm: bytes
    key: bytes
    flags: List[bytes]
    value: Optional[bytes]

    def __init__(
        self,
        cm: bytes,
        key: Union[bytes, str],
        flags: List[bytes],
        value: Optional[bytes],
    ) -> None:
        if isinstance(key, str):
            key = key.encode()
        self.cm = cm
        self.key = key
        self.flags = flags
        self.value = value


@dataclass
class MetaResult:
    rc: bytes
    flags: List[bytes]
    value: Optional[bytes]


class Connection:
    def __init__(self, addr: Union[Tuple[str, int]]):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(addr)
        self.stream = self.socket.makefile(mode="rwb")

    def close(self) -> None:
        self.stream.close()
        self.socket.close()

    def flush_all(self) -> None:
        self.stream.write(b"flush_all\r\n")
        self.stream.flush()
        response = self.stream.readline()
        if response != b"OK\r\n":
            raise MemcacheError(response.removesuffix(NEWLINE))

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

    def set(
        self, key: Union[bytes, str], value: bytes, expire: Optional[int] = None
    ) -> None:
        flags = [b"S%d" % len(value)]
        if expire:
            flags.append(b"T%d" % expire)

        command = MetaCommand(cm=b"ms", key=key, flags=flags, value=value)
        self.execute_meta_command(command)

    def get(self, key: Union[bytes, str]) -> Optional[bytes]:
        command = MetaCommand(cm=b"mg", key=key, flags=[b"v"], value=None)
        return self.execute_meta_command(command).value

    def delete(self, key: Union[bytes, str]) -> None:
        command = MetaCommand(cm=b"md", key=key, flags=[], value=None)
        self.execute_meta_command(command).value


Addr = Tuple[str, int]


class Memcache:
    def __init__(self, addr: Union[Addr, List[Addr]]):
        if isinstance(addr, list):
            self.connections = [Connection(x) for x in addr]
        else:
            self.connections = [Connection(addr)]

    def _get_connection(self, key) -> Connection:
        return self.connections[hash(key) % len(self.connections)]

    def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return self._get_connection(command.key).execute_meta_command(command)

    def flush_all(self) -> None:
        for connection in self.connections:
            connection.flush_all()

    def set(
        self, key: Union[bytes, str], value: bytes, *, expire: Optional[int] = None
    ) -> None:
        return self._get_connection(key).set(key, value, expire=expire)

    def get(self, key: Union[bytes, str]) -> Optional[bytes]:
        return self._get_connection(key).get(key)

    def delete(self, key: Union[bytes, str]) -> None:
        return self._get_connection(key).delete(key)
