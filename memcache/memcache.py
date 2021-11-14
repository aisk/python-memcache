import socket
from typing import Any, List, Optional, Tuple, Union

import hashring

from .errors import MemcacheError
from .meta_command import MetaCommand, MetaResult
from .serialize import dump, load, DumpFunc, LoadFunc


NEWLINE = b"\r\n"


class Connection:
    def __init__(
        self,
        addr: Tuple[str, int],
        *,
        load_func: LoadFunc = load,
        dump_func: DumpFunc = dump
    ):
        self._addr = addr
        self._load = load_func
        self._dump = dump_func
        self._connect()

    def _connect(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(self._addr)
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
        try:
            return self._execute_meta_command(command)
        except (IndexError, ConnectionResetError, BrokenPipeError):
            # This happens when connection is closed by memcached.
            self._connect()
            return self._execute_meta_command(command)

    def _execute_meta_command(self, command: MetaCommand) -> MetaResult:
        self.stream.write(command.dump_header())
        if command.value:
            self.stream.write(command.value + b"\r\n")
        self.stream.flush()
        return self._receive_meta_result()

    def _receive_meta_result(self) -> MetaResult:
        result = MetaResult.load_header(self.stream.readline())

        if result.rc == b"VA":
            if result.datalen is None:
                raise MemcacheError("invalid response: missing datalen")
            result.value = self.stream.read(result.datalen)
            self.stream.read(2)  # read the "\r\n"

        return result

    def set(
        self, key: Union[bytes, str], value: Any, expire: Optional[int] = None
    ) -> None:
        value, client_flags = self._dump(key, value)

        flags = [b"F%d" % client_flags]
        if expire:
            flags.append(b"T%d" % expire)

        command = MetaCommand(
            cm=b"ms", key=key, datalen=len(value), flags=flags, value=value
        )
        self.execute_meta_command(command)

    def get(self, key: Union[bytes, str]) -> Optional[Any]:
        command = MetaCommand(cm=b"mg", key=key, flags=[b"v", b"f"])
        result = self.execute_meta_command(command)

        if result.value is None:
            return None

        client_flags = int(result.flags[0][1:])

        return self._load(key, result.value, client_flags)

    def delete(self, key: Union[bytes, str]) -> None:
        command = MetaCommand(cm=b"md", key=key, flags=[], value=None)
        self.execute_meta_command(command)


Addr = Tuple[str, int]


class Memcache:
    def __init__(
        self,
        addr: Union[Addr, List[Addr]] = None,
        *,
        load_func: LoadFunc = load,
        dump_func: DumpFunc = dump
    ):
        addr = addr or ("localhost", 11211)
        if isinstance(addr, list):
            self._connections = hashring.HashRing(
                [Connection(x, load_func=load_func, dump_func=dump_func) for x in addr]
            )
        else:
            self._connections = hashring.HashRing(
                [Connection(addr, load_func=load_func, dump_func=dump_func)]
            )

    def _get_connection(self, key) -> Connection:
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        return self._connections.get_node(key)

    def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return self._get_connection(command.key).execute_meta_command(command)

    def flush_all(self) -> None:
        for connection in self._connections.nodes:
            connection.flush_all()

    def set(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> None:
        return self._get_connection(key).set(key, value, expire=expire)

    def get(self, key: Union[bytes, str]) -> Optional[Any]:
        return self._get_connection(key).get(key)

    def delete(self, key: Union[bytes, str]) -> None:
        return self._get_connection(key).delete(key)
