import queue
import socket
import threading
from contextlib import contextmanager
from typing import Callable, Iterator, Optional, Tuple

from .errors import MemcacheError
from .meta_command import MetaCommand, MetaResult


NEWLINE = b"\r\n"

Addr = Tuple[str, int]


class Connection:
    def __init__(
        self,
        addr: Addr,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self._addr = addr
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

    def flush_all(self, delay: int = 0) -> None:
        if delay > 0:
            self.stream.write(b"flush_all %d\r\n" % delay)
        else:
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
