import asyncio
from typing import Any, List, Tuple, Union, Optional

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
        dump_func: DumpFunc = dump
    ):
        self._addr = addr
        self._load = load_func
        self._dump = dump_func
        self._connected = False

    async def _connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(
            self._addr[0], self._addr[1]
        )
        self._connected = True

    async def flush_all(self) -> None:
        if not self._connected:
            await self._connect()

        self.writer.write(b"flush_all\r\n")
        await self.writer.drain()
        response = await self.reader.readline()
        if response != b"OK\r\n":
            raise MemcacheError(response.removesuffix(b"\r\n"))

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


class AsyncMemcache:
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
                [
                    AsyncConnection(x, load_func=load_func, dump_func=dump_func)
                    for x in addr
                ]
            )
        else:
            self._connections = hashring.HashRing(
                [AsyncConnection(addr, load_func=load_func, dump_func=dump_func)]
            )

    def _get_connection(self, key) -> AsyncConnection:
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        return self._connections.get_node(key)

    async def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return await self._get_connection(command.key).execute_meta_command(command)

    async def flush_all(self) -> None:
        for connection in self._connections.nodes:
            await connection.flush_all()

    async def set(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> None:
        return await self._get_connection(key).set(key, value, expire=expire)

    async def get(self, key: Union[bytes, str]) -> Optional[Any]:
        return await self._get_connection(key).get(key)

    async def delete(self, key: Union[bytes, str]) -> None:
        return await self._get_connection(key).delete(key)
