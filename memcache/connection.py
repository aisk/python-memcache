import queue
import socket
import threading
from contextlib import contextmanager
from typing import Callable, Iterator, List, Optional, Tuple

from .errors import MemcacheError, PipelineError
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
        timeout: Optional[float] = None,
    ):
        self._addr = addr
        self._username = username
        self._password = password
        self._connect(timeout)

    def _connect(self, timeout: Optional[float]) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(timeout)
        try:
            self.socket.connect(self._addr)
            self.stream = self.socket.makefile(mode="rb")
            self._auth()
        except BaseException:
            self.socket.close()
            raise

    def _set_timeout(self, timeout: Optional[float]) -> None:
        self.socket.settimeout(timeout)

    def _auth(self) -> None:
        if self._username is None or self._password is None:
            return
        auth_data = b"%s %s" % (
            self._username.encode("utf-8"),
            self._password.encode("utf-8"),
        )
        self.socket.sendall(
            b"set auth x 0 %d\r\n" % len(auth_data) + auth_data + b"\r\n"
        )
        response = self.stream.readline()
        if response != b"STORED\r\n":
            raise MemcacheError(response.rstrip(NEWLINE))

    def close(self) -> None:
        try:
            self.stream.close()
        finally:
            self.socket.close()

    def flush_all(self, delay: int = 0, timeout: Optional[float] = None) -> None:
        self._set_timeout(timeout)
        if delay > 0:
            self.socket.sendall(b"flush_all %d\r\n" % delay)
        else:
            self.socket.sendall(b"flush_all\r\n")
        response = self.stream.readline()
        if response != b"OK\r\n":
            raise MemcacheError(response.rstrip(NEWLINE))

    def execute_meta_command(
        self, command: MetaCommand, timeout: Optional[float] = None
    ) -> MetaResult:
        # Never reconnect and replay here. Once a write has started, a lost
        # response makes the outcome ambiguous (especially for ms/ma).
        self._set_timeout(timeout)
        return self._execute_meta_command(command)

    def _execute_meta_command(self, command: MetaCommand) -> MetaResult:
        self.socket.sendall(command.dump())
        return self._receive_meta_result()

    def _receive_meta_result(self) -> MetaResult:
        line = self.stream.readline()
        if not line:
            raise MemcacheError("connection closed while reading response")
        result = MetaResult.load_header(line)

        if result.rc == b"VA":
            if result.datalen is None:
                raise MemcacheError("invalid response: missing datalen")
            result.value = self.stream.read(result.datalen)
            self.stream.read(2)  # read the "\r\n"

        return result

    def execute_pipeline(
        self, commands: List[MetaCommand], timeout: Optional[float] = None
    ) -> List[MetaResult]:
        """Write a quiet pipeline and read through its ``mn`` barrier."""
        self._set_timeout(timeout)
        written = 0
        responses: List[MetaResult] = []
        try:
            for command in commands:
                written += 1
                self.socket.sendall(command.dump())
            self.socket.sendall(b"mn\r\n")
            while True:
                line = self.stream.readline()
                if not line:
                    raise MemcacheError("connection closed while reading pipeline")
                if line == b"MN\r\n":
                    return responses
                result = MetaResult.load_header(line)
                if result.rc == b"VA":
                    if result.datalen is None:
                        raise MemcacheError("invalid response: missing datalen")
                    result.value = self.stream.read(result.datalen)
                    self.stream.read(2)
                responses.append(result)
        except BaseException as exc:
            raise PipelineError(written, responses, exc)


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
        except queue.Empty:
            if self._max_size and self._size >= self._max_size:
                connection = self._connections.get(timeout=self._timeout)
            else:
                with self._lock:
                    self._size += 1
                try:
                    connection = self._create_connection()
                except BaseException:
                    with self._lock:
                        self._size -= 1
                    raise
        try:
            yield connection
        except BaseException:
            try:
                connection.close()
            finally:
                with self._lock:
                    self._size -= 1
            raise
        else:
            self._connections.put(connection)

    def close(self) -> None:
        while True:
            try:
                connection = self._connections.get_nowait()
            except queue.Empty:
                break
            connection.close()
        self._size = 0
