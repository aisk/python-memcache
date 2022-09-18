import socket
import threading
import queue
from contextlib import contextmanager
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union

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
        dump_func: DumpFunc = dump,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self._addr = addr
        self._load = load_func
        self._dump = dump_func
        self._username = username
        self._password = password
        self._connect()

    def _connect(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(self._addr)
        self.stream = self.socket.makefile(mode="rwb")
        self._auth()

    def _auth(self) -> None:
        if self._username is None or self._password is None:
            return
        auth_data = b"%s %s" % (
            self._username.encode("utf-8"),
            self._password.encode("utf-8"),
        )
        self.stream.write(b"set auth x 0 %d\r\n" % len(auth_data))
        self.stream.write(auth_data)
        self.stream.write(b"\r\n")
        self.stream.flush()
        response = self.stream.readline()
        if response != b"STORED\r\n":
            raise MemcacheError(response.rstrip(NEWLINE))

    def close(self) -> None:
        self.stream.close()
        self.socket.close()

    def flush_all(self) -> None:
        self.stream.write(b"flush_all\r\n")
        self.stream.flush()
        response = self.stream.readline()
        if response != b"OK\r\n":
            raise MemcacheError(response.rstrip(NEWLINE))

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


class Pool:
    def __init__(
        self,
        create_connection: Callable[..., Connection],
        max_size: Optional[int],
        timeout: Optional[int],
    ) -> None:
        self._create_connection = create_connection
        self._max_size = max_size
        self._timeout = timeout
        self._size = 0
        self._lock = threading.Lock()
        self._connections: queue.Queue[Connection] = queue.Queue()

    @contextmanager
    def get(self) -> Iterator[Connection]:
        try:
            connection = self._connections.get_nowait()
            yield connection
            self._connections.put(connection)
        except queue.Empty:
            if self._max_size and self._size >= self._max_size:
                connection = self._connections.get(timeout=self._timeout)
                yield connection
                self._connections.put(connection)
            else:
                with self._lock:
                    self._size += 1
                connection = self._create_connection()
                yield connection
                self._connections.put(connection)


class Memcache:
    def __init__(
        self,
        addr: Union[Addr, List[Addr], None] = None,
        *,
        pool_size: Optional[int] = 23,
        pool_timeout: Optional[int] = 1,
        load_func: LoadFunc = load,
        dump_func: DumpFunc = dump,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        addr = addr or ("localhost", 11211)
        if isinstance(addr, list):
            addrs: List[Addr] = addr
            nodes: List[Pool] = []
            for addr in addrs:
                create_connection = lambda: Connection(
                    addr,
                    load_func=load_func,
                    dump_func=dump_func,
                    username=username,
                    password=password,
                )
                nodes.append(
                    Pool(create_connection, max_size=pool_size, timeout=pool_timeout)
                )
            self._connections = hashring.HashRing(nodes)
        elif isinstance(addr, tuple):
            a: Addr = addr
            create_connection = lambda: Connection(
                a,
                load_func=load_func,
                dump_func=dump_func,
                username=username,
                password=password,
            )
            self._connections = hashring.HashRing(
                [Pool(create_connection, max_size=pool_size, timeout=pool_timeout)]
            )
        else:
            raise TypeError("invalid type for addr")

    @contextmanager
    def _get_connection(self, key: Union[str, bytes]) -> Iterator[Connection]:
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        pool = self._connections.get_node(key)
        with pool.get() as connection:
            yield connection

    def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        with self._get_connection(command.key) as connection:
            return connection.execute_meta_command(command)

    def flush_all(self) -> None:
        for pool in self._connections.nodes:
            with pool.get() as connection:
                connection.flush_all()

    def set(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> None:
        with self._get_connection(key) as connection:
            return connection.set(key, value, expire=expire)

    def get(self, key: Union[bytes, str]) -> Optional[Any]:
        with self._get_connection(key) as connection:
            return connection.get(key)

    def delete(self, key: Union[bytes, str]) -> None:
        with self._get_connection(key) as connection:
            return connection.delete(key)
