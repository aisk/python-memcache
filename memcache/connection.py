import socket
from typing import TypeAlias

from .errors import MemcacheError, PipelineError
from .meta_command import MetaCommand, MetaResult, ResponseReader

NEWLINE = b"\r\n"
RECV_SIZE = 65536

Addr: TypeAlias = tuple[str, int]


class Connection:
    def __init__(
        self,
        addr: Addr,
        *,
        username: str | None = None,
        password: str | None = None,
        timeout: float | None = None,
    ):
        self._addr = addr
        self._username = username
        self._password = password
        self._connect(timeout)

    def _connect(self, timeout: float | None) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(timeout)
        self._reader = ResponseReader()
        try:
            self.socket.connect(self._addr)
            self._auth()
        except BaseException:
            self.socket.close()
            raise

    def _set_timeout(self, timeout: float | None) -> None:
        self.socket.settimeout(timeout)

    def _fill(self) -> None:
        data = self.socket.recv(RECV_SIZE)
        if not data:
            raise MemcacheError("connection closed while reading response")
        self._reader.feed(data)

    def _next_line(self) -> bytes:
        while True:
            line = self._reader.next_line()
            if line is not None:
                return line
            self._fill()

    def _next_response(self) -> MetaResult:
        while True:
            result = self._reader.next_response()
            if result is not None:
                return result
            self._fill()

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
        response = self._next_line()
        if response != b"STORED":
            raise MemcacheError(response)

    def close(self) -> None:
        self.socket.close()

    def flush_all(self, delay: int = 0, timeout: float | None = None) -> None:
        self._set_timeout(timeout)
        if delay > 0:
            self.socket.sendall(b"flush_all %d\r\n" % delay)
        else:
            self.socket.sendall(b"flush_all\r\n")
        response = self._next_line()
        if response != b"OK":
            raise MemcacheError(response)

    def execute_meta_command(
        self, command: MetaCommand, timeout: float | None = None
    ) -> MetaResult:
        # Never reconnect and replay here. Once a write has started, a lost
        # response makes the outcome ambiguous (especially for ms/ma).
        self._set_timeout(timeout)
        self.socket.sendall(command.dump())
        return self._next_response()

    def send_pipeline(
        self, commands: list[MetaCommand], timeout: float | None = None
    ) -> None:
        """Write a quiet pipeline and its ``mn`` barrier without reading."""
        self._set_timeout(timeout)
        written = 0
        try:
            for command in commands:
                written += 1
                self.socket.sendall(command.dump())
            self.socket.sendall(b"mn\r\n")
        except BaseException as exc:
            raise PipelineError(written, [], exc)

    def receive_pipeline(
        self, written: int, timeout: float | None = None
    ) -> list[MetaResult]:
        """Read a sent pipeline's responses through its ``mn`` barrier.

        ``written`` is the number of commands already on the wire; it only
        attributes a failure (``PipelineError.written``), no response count
        is enforced here because quiet commands suppress their responses.
        """
        self._set_timeout(timeout)
        responses: list[MetaResult] = []
        try:
            while True:
                result = self._next_response()
                if result.is_barrier:
                    return responses
                responses.append(result)
        except BaseException as exc:
            raise PipelineError(written, responses, exc)

    def execute_pipeline(
        self, commands: list[MetaCommand], timeout: float | None = None
    ) -> list[MetaResult]:
        """Write a quiet pipeline and read through its ``mn`` barrier."""
        self.send_pipeline(commands, timeout)
        return self.receive_pipeline(len(commands), timeout)
