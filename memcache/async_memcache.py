import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, List, Tuple, Union, Optional

import anyio
import hashring
from anyio.streams.buffered import BufferedByteReceiveStream

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

    async def flush_all(self) -> None:
        if not self._connected:
            await self._connect()

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

    async def cas(
        self,
        key: Union[bytes, str],
        value: Any,
        cas_token: int,
        *,
        expire: Optional[int] = None,
    ) -> None:
        """
        Store a value using compare-and-swap operation.

        :param key: The key to store
        :param value: The value to store
        :param cas_token: The CAS token from a previous gets operation
        :param expire: Optional expiration time in seconds
        :raises MemcacheError: If the CAS token doesn't match or other error occurs
        """
        value, client_flags = self._dump(key, value)

        flags = [b"F%d" % client_flags, b"C%d" % cas_token]
        if expire:
            flags.append(b"T%d" % expire)

        command = MetaCommand(
            cm=b"ms", key=key, datalen=len(value), flags=flags, value=value
        )
        result = await self.execute_meta_command(command)

        if result.rc != b"HD":
            raise MemcacheError("CAS operation failed: token mismatch or other error")

    async def get(self, key: Union[bytes, str]) -> Optional[Any]:
        command = MetaCommand(cm=b"mg", key=key, flags=[b"v", b"f"])
        result = await self.execute_meta_command(command)

        if result.value is None:
            return None

        client_flags = int(result.flags[0][1:])

        return self._load(key, result.value, client_flags)

    async def gets(self, key: Union[bytes, str]) -> Optional[Tuple[Any, int]]:
        """
        Get a value and its CAS token from memcached.

        :param key: The key to retrieve
        :return: A tuple of (value, cas_token) or None if key doesn't exist
        """
        command = MetaCommand(cm=b"mg", key=key, flags=[b"v", b"f", b"c"])
        result = await self.execute_meta_command(command)

        if result.value is None:
            return None

        client_flags = int(result.flags[0][1:])
        value = self._load(key, result.value, client_flags)

        # Find CAS token in flags
        cas_token = None
        for flag in result.flags[1:]:  # Skip the first flag (client_flags)
            if flag.startswith(b"c"):
                cas_token = int(flag[1:])
                break

        if cas_token is None:
            raise MemcacheError("CAS token not found in response")

        return value, cas_token

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
    """
    Async Memcache client.

    :param addr: memcached server addresses to be connected.

      The address can be a two elements tuple, as ``(ip, port)`` format.

      The address can be None, thus the default server ``("localhost", 11211)`` should
      be used.

      The address can be a list of tuple, like ``[("192.168.1.10", 11211),
      ("192.168.1.11", 11211)]``. In this situation, the keys will be hashed to one
      of those servers by consistent hash algorithm.
    :param pool_size: The connection pool size. This size will be used as the max
      number to keep the connections for future uses.
    :param pool_timeout: If the there is no available connection in the pool, and the
      ``pool_size`` is reached, wait the specified time to get an available connection,
      or a `asyncio.TimeoutError` is raised.
    :param load_func: Function to load the bytes content from memcached to python
      values.
    :param dump_func: Function to dump the python values to bytes content to store in
      memcached.
    :param username: Memcached ASCII protocol authentication username.
    :param password: Memcached ASCII protocol authentication password.
    """

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

    async def gets(self, key: Union[bytes, str]) -> Optional[Tuple[Any, int]]:
        """
        Get a value and its CAS token from memcached.

        :param key: The key to retrieve
        :return: A tuple of (value, cas_token) or None if key doesn't exist
        """
        async with self._get_connection(key) as connection:
            return await connection.gets(key)

    async def cas(
        self,
        key: Union[bytes, str],
        value: Any,
        cas_token: int,
        *,
        expire: Optional[int] = None,
    ) -> None:
        """
        Store a value using compare-and-swap operation.

        :param key: The key to store
        :param value: The value to store
        :param cas_token: The CAS token from a previous gets operation
        :param expire: Optional expiration time in seconds
        :raises MemcacheError: If the CAS token doesn't match or other error occurs
        """
        async with self._get_connection(key) as connection:
            await connection.cas(key, value, cas_token, expire=expire)

    async def delete(self, key: Union[bytes, str]) -> None:
        async with self._get_connection(key) as connection:
            return await connection.delete(key)
