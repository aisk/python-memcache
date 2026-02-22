import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable, Optional, Tuple

import anyio
from anyio.streams.buffered import BufferedByteReceiveStream

from .errors import MemcacheError
from .meta_command import MetaCommand, MetaResult


class AsyncConnection:
    def __init__(
        self,
        addr: Tuple[str, int],
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self._addr = addr
        self._username = username
        self._password = password
        self._connected = False

    async def _connect(self) -> None:
        self.writer = await anyio.connect_tcp(self._addr[0], self._addr[1])
        self.reader = BufferedByteReceiveStream(self.writer)
        await self._auth()
        self._connected = True

    async def _auth(self) -> None:
        if self._username is None or self._password is None:
            return
        auth_data = b"%s %s" % (
            self._username.encode("utf-8"),
            self._password.encode("utf-8"),
        )
        await self.writer.send(b"set auth x 0 %d\r\n" % len(auth_data))
        await self.writer.send(auth_data)
        await self.writer.send(b"\r\n")
        response = await self.reader.receive_until(b"\r\n", max_bytes=1024)
        if response != b"STORED":
            raise MemcacheError(response)

    async def flush_all(self, delay: int = 0) -> None:
        if not self._connected:
            await self._connect()

        if delay > 0:
            await self.writer.send(b"flush_all %d\r\n" % delay)
        else:
            await self.writer.send(b"flush_all\r\n")
        response = await self.reader.receive_until(b"\r\n", max_bytes=1024)
        if response != b"OK":
            raise MemcacheError(response)

    async def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        try:
            return await self._execute_meta_command(command)
        except (IndexError, ConnectionResetError, BrokenPipeError):
            self._connected = False
            return await self._execute_meta_command(command)

    async def _execute_meta_command(self, command: MetaCommand) -> MetaResult:
        if not self._connected:
            await self._connect()

        await self.writer.send(command.dump_header())
        if command.value:
            await self.writer.send(command.value + b"\r\n")
        return await self._receive_meta_result()

    async def _receive_meta_result(self) -> MetaResult:
        header_line = await self.reader.receive_until(b"\r\n", max_bytes=1024)
        result = MetaResult.load_header(header_line)

        if result.rc == b"VA":
            if result.datalen is None:
                raise MemcacheError("invalid response: missing datalen")
            result.value = await self.reader.receive_exactly(result.datalen)
            await self.reader.receive_exactly(2)  # read the "\r\n"

        return result


class AsyncPool:
    def __init__(
        self,
        create_connection: Callable[..., AsyncConnection],
        max_size: Optional[int],
        timeout: Optional[int],
    ) -> None:
        self._create_connection = create_connection
        self._max_size = max_size
        self._timeout = timeout
        self._size = 0
        self._lock = asyncio.Lock()
        self._connections: asyncio.Queue[AsyncConnection] = asyncio.Queue()

    @asynccontextmanager
    async def get(self) -> AsyncIterator[AsyncConnection]:
        try:
            connection = self._connections.get_nowait()
            yield connection
            await self._connections.put(connection)
        except asyncio.QueueEmpty:
            if self._max_size and self._size >= self._max_size:
                connection = await asyncio.wait_for(
                    self._connections.get(), timeout=self._timeout
                )
                yield connection
                await self._connections.put(connection)
            else:
                async with self._lock:
                    self._size += 1
                connection = self._create_connection()
                yield connection
                await self._connections.put(connection)
