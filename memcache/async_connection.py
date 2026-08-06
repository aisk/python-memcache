import anyio

from .connection import RECV_SIZE, Addr
from .errors import MemcacheError, PipelineError as PipelineError
from .meta_command import MetaCommand, MetaResult, ResponseReader


class AsyncConnection:
    def __init__(
        self,
        addr: Addr,
        *,
        username: str | None = None,
        password: str | None = None,
    ):
        self._addr = addr
        self._username = username
        self._password = password
        self._connected = False

    async def _connect(self) -> None:
        self.stream = await anyio.connect_tcp(self._addr[0], self._addr[1])
        self._reader = ResponseReader()
        await self._auth()
        self._connected = True

    async def close(self) -> None:
        self._connected = False
        stream = getattr(self, "stream", None)
        if stream is not None:
            await stream.aclose()

    async def _fill(self) -> None:
        try:
            data = await self.stream.receive(RECV_SIZE)
        except anyio.EndOfStream:
            raise MemcacheError("connection closed while reading response") from None
        self._reader.feed(data)

    async def _next_line(self) -> bytes:
        while True:
            line = self._reader.next_line()
            if line is not None:
                return line
            await self._fill()

    async def _next_response(self) -> MetaResult:
        while True:
            result = self._reader.next_response()
            if result is not None:
                return result
            await self._fill()

    async def _auth(self) -> None:
        if self._username is None or self._password is None:
            return
        auth_data = b"%s %s" % (
            self._username.encode("utf-8"),
            self._password.encode("utf-8"),
        )
        await self.stream.send(
            b"set auth x 0 %d\r\n" % len(auth_data) + auth_data + b"\r\n"
        )
        response = await self._next_line()
        if response != b"STORED":
            raise MemcacheError(response)

    async def flush_all(self, delay: int = 0) -> None:
        if not self._connected:
            await self._connect()

        if delay > 0:
            await self.stream.send(b"flush_all %d\r\n" % delay)
        else:
            await self.stream.send(b"flush_all\r\n")
        response = await self._next_line()
        if response != b"OK":
            raise MemcacheError(response)

    async def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        try:
            return await self._execute_meta_command(command)
        except BaseException:
            self._connected = False
            raise

    async def _execute_meta_command(self, command: MetaCommand) -> MetaResult:
        if not self._connected:
            await self._connect()

        await self.stream.send(command.dump())
        return await self._next_response()

    async def execute_pipeline(self, commands: list[MetaCommand]) -> list[MetaResult]:
        """Write a quiet pipeline and read through its ``mn`` barrier.

        ``PipelineError.written`` is conservative: an operation is counted as
        written as soon as its first send starts, since a failed send may have
        transferred an arbitrary prefix to the kernel.
        """
        written = 0
        responses: list[MetaResult] = []
        try:
            if not self._connected:
                await self._connect()
            for command in commands:
                written += 1
                await self.stream.send(command.dump())
            await self.stream.send(b"mn\r\n")
            while True:
                result = await self._next_response()
                if result.is_barrier:
                    return responses
                responses.append(result)
        except BaseException as exc:
            self._connected = False
            raise PipelineError(written, responses, exc)
