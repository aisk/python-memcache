import socket
from typing import TypeAlias

from .errors import MemcacheError, PipelineError
from .meta_command import MetaCommand, MetaResult


NEWLINE = b"\r\n"

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
        try:
            self.socket.connect(self._addr)
            self.stream = self.socket.makefile(mode="rb")
            self._auth()
        except BaseException:
            self.socket.close()
            raise

    def _set_timeout(self, timeout: float | None) -> None:
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

    def flush_all(self, delay: int = 0, timeout: float | None = None) -> None:
        self._set_timeout(timeout)
        if delay > 0:
            self.socket.sendall(b"flush_all %d\r\n" % delay)
        else:
            self.socket.sendall(b"flush_all\r\n")
        response = self.stream.readline()
        if response != b"OK\r\n":
            raise MemcacheError(response.rstrip(NEWLINE))

    def execute_meta_command(
        self, command: MetaCommand, timeout: float | None = None
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

    def execute_pipeline(
        self, commands: list[MetaCommand], timeout: float | None = None
    ) -> list[MetaResult]:
        """Write a quiet pipeline and read through its ``mn`` barrier."""
        self.send_pipeline(commands, timeout)
        return self.receive_pipeline(len(commands), timeout)
