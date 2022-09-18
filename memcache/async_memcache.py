import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, List, Tuple, Union, Optional

import hashring

from .errors import MemcacheError
from .memcache import Addr
from .meta_command import MetaCommand, MetaResult
from .serialize import dump, load, DumpFunc, LoadFunc


class AsyncConnection:
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
        self._connected = False

    async def _connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(
            self._addr[0], self._addr[1]
        )
        await self._auth()
        self._connected = True

    async def _auth(self) -> None:
        if self._username is None or self._password is None:
            return
        auth_data = b"%s %s" % (
            self._username.encode("utf-8"),
            self._password.encode("utf-8"),
        )
        self.writer.write(b"set auth x 0 %d\r\n" % len(auth_data))
        self.writer.write(auth_data)
        self.writer.write(b"\r\n")
        await self.writer.drain()
        response = await self.reader.readline()
        if response != b"STORED\r\n":
            raise MemcacheError(response.rstrip(b"\r\n"))

    async def flush_all(self) -> None:
        if not self._connected:
            await self._connect()

        self.writer.write(b"flush_all\r\n")
        await self.writer.drain()
        response = await self.reader.readline()
        if response != b"OK\r\n":
            raise MemcacheError(response.rstrip(b"\r\n"))

    async def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        try:
            return await self._execute_meta_command(command)
        except (IndexError, ConnectionResetError, BrokenPipeError):
            self._connected = False
            return await self._execute_meta_command(command)

    async def _execute_meta_command(self, command: MetaCommand) -> MetaResult:
        if not self._connected:
            await self._connect()

        self.writer.write(command.dump_header())
        if command.value:
            self.writer.write(command.value + b"\r\n")
        await self.writer.drain()
        return await self._receive_meta_result()

    async def _receive_meta_result(self) -> MetaResult:
        result = MetaResult.load_header(await self.reader.readline())

        if result.rc == b"VA":
            if result.datalen is None:
                raise MemcacheError("invalid response: missing datalen")
            result.value = await self.reader.read(result.datalen)
            await self.reader.read(2)  # read the "\r\n"

        return result

    async def set(
        self, key: Union[bytes, str], value: Any, expire: Optional[int] = None
    ) -> None:
        value, client_flags = self._dump(key, value)

        flags = [b"F%d" % client_flags]
        if expire:
            flags.append(b"T%d" % expire)

        command = MetaCommand(
            cm=b"ms", key=key, datalen=len(value), flags=flags, value=value
        )
        await self.execute_meta_command(command)

    async def get(self, key: Union[bytes, str]) -> Optional[Any]:
        command = MetaCommand(cm=b"mg", key=key, flags=[b"v", b"f"])
        result = await self.execute_meta_command(command)

        if result.value is None:
            return None

        client_flags = int(result.flags[0][1:])

        return self._load(key, result.value, client_flags)

    async def delete(self, key: Union[bytes, str]) -> None:
        command = MetaCommand(cm=b"md", key=key, flags=[], value=None)
        await self.execute_meta_command(command)


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


class AsyncMemcache:
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
            nodes: List[AsyncPool] = []
            for addr in addrs:
                create_connection = lambda: AsyncConnection(
                    addr,
                    load_func=load_func,
                    dump_func=dump_func,
                    username=username,
                    password=password,
                )
                nodes.append(
                    AsyncPool(
                        create_connection, max_size=pool_size, timeout=pool_timeout
                    )
                )
            self._connections = hashring.HashRing(nodes)
        elif isinstance(addr, tuple):
            a: Addr = addr
            create_connection = lambda: AsyncConnection(
                a,
                load_func=load_func,
                dump_func=dump_func,
                username=username,
                password=password,
            )
            self._connections = hashring.HashRing(
                [AsyncPool(create_connection, max_size=pool_size, timeout=pool_timeout)]
            )
        else:
            raise TypeError("invalid type for addr")

    @asynccontextmanager
    async def _get_connection(
        self, key: Union[str, bytes]
    ) -> AsyncIterator[AsyncConnection]:
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        pool = self._connections.get_node(key)
        async with pool.get() as connection:
            yield connection

    async def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        async with self._get_connection(command.key) as connection:
            return await connection.execute_meta_command(command)

    async def flush_all(self) -> None:
        for pool in self._connections.nodes:
            async with pool.get() as connection:
                await connection.flush_all()

    async def set(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> None:
        async with self._get_connection(key) as connection:
            return await connection.set(key, value, expire=expire)

    async def get(self, key: Union[bytes, str]) -> Optional[Any]:
        async with self._get_connection(key) as connection:
            return await connection.get(key)

    async def delete(self, key: Union[bytes, str]) -> None:
        async with self._get_connection(key) as connection:
            return await connection.delete(key)
